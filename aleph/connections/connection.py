from ..common.exceptions import *
from ..common.datetime_functions import *
from ..common.dict_functions import *
from ..common.wait_one_step import WaitOneStep
from ..common.local_storage import LocalStorage
from ..common.data_filter import DataFilter
import threading
import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class Connection:

    def __init__(self, client_id=""):

        self.client_id = client_id                    # Assign the connection a client id, required if persistent = True
        self.local_storage = LocalStorage()           # Local storage instance

        # Optional parameters: reading
        self.default_time_step = 10                   # Default time step for loops
        self.persistent = False                       # Remembers last record, equivalent to mqtt clean session = False
        self.clean_on_read = True                     # Check since, until, fields, filter, limit, offset and order
        self.report_by_exception = False              # When reading data, only returns changing values
        self.report_by_exception_resend_after = None  # Seconds after which data reported by exception is resent
        self.force_close_on_read_error = True         # Call close() when reading fails
        self.multithread = True                       # If true, uses threading, else uses asyncio

        # Optional parameters: writing
        self.models = {}                              # Dict {key: DataModel} (for data validation)
        self.store_and_forward = False                # Resends failed writes
        self.clean_on_write = True                    # Clean data when writing (adds time)
        self.force_close_on_write_error = True        # Call close() when writing fails

        # Internal
        self.__unsubscribe_flags__ = {}               # Used to implement unsubscribe

        self.__async_loop__ = asyncio.new_event_loop()
        threading.Thread(target=self.__async_loop__.run_forever).start()

    # ===================================================================================
    # Main functions (override me)
    # ===================================================================================
    def open(self):
        """
        Opens the connection. If it fails, it must raise an Exception
        """
        return

    def close(self):
        """
        Closes the connection.
        """
        return

    def read(self, key, **kwargs):
        """
        Must return a list (data, a list of records) or a dict (single record)
        The **kwargs are optional read arguments
        Use clean_read_args to clean the **kwargs
        """
        return []

    def write(self, key, data):
        """
        Returns None.
        """
        return

    def is_connected(self):
        """
        Returns a boolean (True if open, False if not open)
        """
        return True

    # ===================================================================================
    # Callbacks
    # ===================================================================================
    def on_new_data(self, key, data):
        """
        Callback function for when a new message arrives. Data can be a dict or a list of
        dict. This function is used by the read_async, subscribe async and subscribe
        methods.
        """
        return

    def on_read_error(self, error):
        """
        Callback function for when safe_read fails
        """
        return

    def on_write_error(self, error):
        """
        Callback function for when safe_write fails
        """
        return

    def on_connect(self):
        """
        Callback function for when the connection is open
        """
        return

    def on_disconnect(self):
        """
        Callback function for when the connection is closed
        """
        return

    # ===================================================================================
    # Read and write safe functions
    # ===================================================================================

    def safe_read(self, key, **kwargs):
        try:
            # Check if connection is open
            if not self.is_connected(): self.open()
            # Clean arguments and read data
            args = self.__clean_read_args__(key, **kwargs)
            # Call read function
            data = self.read(key, **args)
            if data is None: raise Exceptions.InvalidKey("Reading function returned None")
            # Clean data
            data = self.__clean_read_data__(key, data, **args)
            return data

        except Exception as e:
            if self.force_close_on_read_error: self.close()
            self.on_read_error(Error(e, client_id=self.client_id, key=key, kw_args=kwargs))
            return None

    def safe_write(self, key, data):
        """
        Executes the write function safely.
        Returns a bool: True for success, False for failure
        """
        # Clean data
        try:
            data = self.__clean_write_data__(key, data)
            if len(data) == 0: return
        except Exception as e:
            self.on_write_error(Error(e, client_id=self.client_id, key=key, data=data))
            return False

        # Write data
        try:
            if not self.is_connected(): self.open()
            self.write(key, data)
            if self.store_and_forward: self.__store_and_forward_flush_buffer__()
            return True
        except Exception as e:
            if self.force_close_on_write_error: self.close()
            if self.store_and_forward: self.__store_and_forward_add_to_buffer__(key, data)
            self.on_write_error(Error(e, client_id=self.client_id, key=key, data=data))
            return False

    # ===================================================================================
    # Opening
    # ===================================================================================

    def open_async(self, time_step=None):
        """
        Executes the open function without blocking the main thread
        Calls is_connected on a loop and tries to reconnect if disconnected
        """
        if time_step is None: time_step = self.default_time_step
        logger.info("Adding open_async coroutine")
        asyncio.run_coroutine_threadsafe(self.__open_async_aux__(time_step), self.__async_loop__)

    async def __open_async_aux__(self, time_step):
        w = WaitOneStep(time_step)

        s_prev = False
        while True:
            # Check status and try to reopen
            s = self.is_connected()
            if not s:
                try:
                    self.open()
                    s = True
                except:
                    s = False

            # Callbacks
            if s and not s_prev: self.__on_connect__()
            if not s and s_prev: self.__on_disconnect__()
            s_prev = s

            await w.async_wait()

    def __on_connect__(self):
        # Flush sx|tore and forward buffer
        if self.store_and_forward:
            self.__store_and_forward_flush_buffer__()

        # If not persistent wipe out last times
        if not self.persistent:
            self.local_storage.set(LocalStorage.LAST_TIME_READ, {})

        self.on_connect()

    def __on_disconnect__(self):
        self.on_disconnect()

    # ===================================================================================
    # More reading functions
    # ===================================================================================

    def read_async(self, key, **kwargs):
        """
        Executes the safe_read function without blocking the main thread.
        """
        logger.info("Adding read_async coroutine for key %s", key)
        asyncio.run_coroutine_threadsafe(self.__read_async_aux__(key, **kwargs), self.__async_loop__)

    async def __read_async_aux__(self, key, **kwargs):
        data = self.safe_read(key, **kwargs)
        if data is None or len(data) == 0: return
        self.on_new_data(key, data)

    def subscribe(self, key, time_step=None):
        """
        Executes the safe_read function in a loop. This method blocks the main thread.
        """
        if time_step is None: time_step = self.default_time_step
        self.__unsubscribe_flags__[key] = True
        w = WaitOneStep(time_step)

        while True:
            try:
                w.wait()
                if not self.__unsubscribe_flags__[key]: break
                data = self.safe_read(key)
                if data is None or len(data) == 0: continue
                self.on_new_data(key, data)
            except KeyboardInterrupt:
                self.__unsubscribe_flags__[key] = False
                break

    async def __subscribe_async_aux__(self, key, time_step):
        self.__unsubscribe_flags__[key] = True
        w = WaitOneStep(time_step)

        while True:
            await w.async_wait()
            if not self.__unsubscribe_flags__[key]: break
            data = self.safe_read(key)
            if data is None or len(data) == 0: continue
            self.on_new_data(key, data)

    def subscribe_async(self, key, time_step=None):
        """
        Executes the subscribe function without blocking the main thread.
        """
        if time_step is None: time_step = self.default_time_step

        if self.multithread:
            logger.info("Creating subscribe_async thread for key %s", key)
            threading.Thread(target=self.subscribe, args=(key, time_step,), daemon=True).start()
        else:
            logger.info("Adding subscribe_async coroutine for key %s", key)
            asyncio.run_coroutine_threadsafe(self.__subscribe_async_aux__(key, time_step), self.__async_loop__)

    def unsubscribe(self, key):
        self.__unsubscribe_flags__[key] = False

    # ===================================================================================
    # Clean and check data for read and write
    # ===================================================================================
    def __clean_read_args__(self, key, **kwargs):
        """
        Gets the kwargs and returns valid arguments
        If some argument is missing it returns the default value.

        fields: fields that should be returned.
                Default: "*" (all)
                Accepts: "*", string (single field), string (multiple fields separated by commas), list of string
                Cleaned: "*" or list of string

        since: timestamp.
               Default: timestamp of the last record read or now()
               Accepts: int (seconds from today), float (unix), string (parseable format), empty string (today at 00:00),
                        datetime.datetime, None (since the beginning of time)
               Cleaned: datetime.datetime (with tzinfo=UTC) or None

        until: timestamp.
               Default: None
               Accepts: int (seconds from today), float (unix), string (parseable format), empty string (tomorrow at 00:00),
                        datetime.datetime, None (this moment)
               Cleaned: datetime.datetime (with tzinfo=UTC) or None

        limit: limit result count.
               Default: 0 (no limit)
               Accepts: int
               Cleaned: int

        offset: skip records.
                Default: 0
                Accepts: int
                Cleaned: int

        timezone: return timestamps in the given timezone.
                  Default: 'UTC'
                  Accepts: string
                  Cleaned string

        order: order by some field
               Default: None
               Accepts: string (field). Place a "-" in front of the field for reversed order (like "-t")
               Cleaned: string or None

        filter: return only data matching a filter.
                Default: None
                Accepts: dict or json
                Cleaned: dict or None

        ###
        Other optional parameters
        ###

        """
        # If cleaned flag is set, do nothing
        if "cleaned" in kwargs and kwargs["cleaned"]: return kwargs

        # Get and set last read time
        last_t = self.local_storage.get(LocalStorage.LAST_TIME_READ, {})
        last_t = parse_datetime(last_t[key]) if key in last_t else now()

        # Preset args
        args = {
            "fields": "*",
            "since": last_t,
            "until": now(),
            "limit": 0,
            "offset": 0,
            "timezone": "UTC",
            "order": None,
            "filter": None,
            "cleaned": False
        }

        # Fields
        if "fields" in kwargs:
            fields = kwargs["fields"]
            if fields == "*" or isinstance(fields, list): args["fields"] = fields
            elif "," in fields: args["fields"] = fields.replace(" ", "").split(",")
            elif isinstance(fields, str): args["fields"] = [fields]

        if "since" in kwargs: args["since"] = parse_datetime(kwargs["since"])
        if "until" in kwargs: args["until"] = parse_datetime(kwargs["until"], True)
        if "limit" in kwargs: args["limit"] = int(kwargs["limit"])
        if "offset" in kwargs: args["offset"] = int(kwargs["offset"])
        if "timezone" in kwargs: args["timezone"] = kwargs["timezone"]
        if "order" in kwargs: args["order"] = kwargs["order"]
        if "filter" in kwargs: args["filter"] = DataFilter.load(kwargs["filter"])

        if "response_code" in kwargs: args["response_code"] = kwargs["response_code"]

        args["cleaned"] = True
        return args

    def __clean_read_data__(self, key, data, **kwargs):
        """
        After reading data, make sure that:
        - Data is list of records
        - Check filter, fields, limit, offset and order
        - Check that records have a timestamp and the timestamp is between since and until
        - Check that records have a timestamp and the timestamp is in the right timezone
        """

        # Data is list of records
        if not isinstance(data, list): data = [data]
        cleaned_data = []

        # Report by exception
        if self.report_by_exception and len(data) > 0 and "id_" in data[0]:
            data = self.__report_by_exception_ids__(key, data)

        # Get last time read
        last_times = {}
        if self.clean_on_read:
            last_times = self.local_storage.get(LocalStorage.LAST_TIME_READ, {})
            if key not in last_times:
                last_times[key] = kwargs["since"].timestamp() if kwargs["since"] is not None else None

        # For each record:
        for record in data:

            if self.report_by_exception and "id_" not in record:
                record = self.__report_by_exception__(key, record)

            if self.clean_on_read:
                # Check filter
                if kwargs["filter"] is not None and not kwargs["filter"].apply_to_record(record):
                    continue

                # Check fields
                if kwargs["fields"] != "*":
                    record = {f: record[f] for f in record if f in kwargs["fields"] or f == "t" or f == "id_"}

                # Check timestamp
                if "t" in record:
                    # Record has a timestamp
                    record["t"] = parse_datetime(record["t"])
                    # Timestamp in valid range
                    if kwargs["since"] is not None and kwargs["since"] >= record["t"]: continue
                    if kwargs["until"] is not None and kwargs["until"] < record["t"]: continue
                    # Update last time read
                    if last_times[key] is None or last_times[key] < record["t"].timestamp():
                        last_times[key] = record["t"].timestamp()
                    # Timestamp in timezone
                    record["t"] = datetime_to_string(record["t"], kwargs["timezone"])

                if "t_" in record:
                    record["t_"] = parse_datetime_to_string(record["t_"], kwargs["timezone"])

            # Check timestamp is in the correct timezone
            elif kwargs["timezone"] != "UTC":
                if "t" in record: record["t"] = parse_datetime_to_string(record["t"], kwargs["timezone"])
                if "t_" in record: record["t_"] = parse_datetime_to_string(record["t_"], kwargs["timezone"])

            cleaned_data.append(record)
            continue

        # Update last time read
        if self.clean_on_read:
            self.local_storage.set(LocalStorage.LAST_TIME_READ, last_times)

        if self.clean_on_read:
            # Order
            if kwargs["order"] is not None:
                if kwargs["order"][0] == "-":
                    cleaned_data.sort(key=lambda x: x[kwargs["order"][1:]], reverse=True)
                else:
                    cleaned_data.sort(key=lambda x: x[kwargs["order"]])

            # Limit and offset
            if len(cleaned_data) > kwargs["offset"] > 0: cleaned_data = cleaned_data[0:-kwargs["offset"]]
            if len(cleaned_data) > kwargs["limit"] > 0: cleaned_data = cleaned_data[-kwargs["limit"]:]

        return cleaned_data

    def __clean_write_data__(self, key, data):
        """
        Makes sure that:
        - Data is a list of record
        - Records are flattened
        - Every record in data has a timestamp (string, utc)
        - Model is valid
        - data
        """
        if not isinstance(data, list): data = [data]

        cleaned_data = []
        for record in data:

            if self.clean_on_write:
                # Time
                if "t" not in record: record["t"] = now(string=True)
                else: record["t"] = parse_datetime_to_string(record["t"])

                # Flatten record
                record = flatten_dict(record)

            # Check model
            if key in self.models:
                record = self.models[key].validate(record)
                if record is None: continue

            # Check that record is not empty
            if not self.__check_record_is_not_empty__(record): continue

            cleaned_data.append(record)
            continue

        return cleaned_data

    def __check_record_is_not_empty__(self, record):
        if len([x for x in record if x not in ["t", "t_", "ignore_", "id_"]]) > 0: return True
        return False

    # ===================================================================================
    # Store and forward
    # ===================================================================================
    def __store_and_forward_add_to_buffer__(self, key, data):
        # Get buffer from local storage
        buffer = self.local_storage.get(LocalStorage.SNF_BUFFER, {})
        
        # Add data to buffer
        if key not in buffer: buffer[key] = []
        buffer[key] += data
        
        # Save buffer to local storage
        self.local_storage.set(LocalStorage.SNF_BUFFER, buffer)

    def __store_and_forward_flush_buffer__(self):
        # Get buffer from local storage
        buffer = self.local_storage.get(LocalStorage.SNF_BUFFER)
        if buffer is None or len(buffer) == 0: return

        # For each record in the buffer, try to write
        new_buffer = {}
        for key in buffer:
            data = buffer[key]
            try: self.write(key, data)
            except: new_buffer[key] = data

        # Save buffer to local storage
        self.local_storage.set(LocalStorage.SNF_BUFFER, new_buffer)

    # ===================================================================================
    # Report by exception
    # ===================================================================================

    # Report by exception if data doesn't have id's
    def __report_by_exception__(self, key, record):
        """
        Takes a record (dict) and returns a new record with only the fields whose values are
        different from that of the past values. The record must be flattened first.
        """

        # Get last record sent from local storage
        past_values = self.local_storage.get(LocalStorage.PAST_VALUES, {})
        t = time.time()
        if key not in past_values:
            past_values[key] = {f: [record[f], t] for f in record}
            self.local_storage.set(LocalStorage.PAST_VALUES, past_values)
            return record

        # For each field in record, check if changed
        new_record = {}
        for v in record:
            if v == "t": continue

            # Check if should resend regardless whether the value changed or not
            do_resend = False
            if self.report_by_exception_resend_after is not None:
                if t - past_values[key][v][1] > self.report_by_exception_resend_after: do_resend = True

            # If new field, value changed, or should resend, store new value
            if v not in past_values[key] or past_values[key][v][0] != record[v] or do_resend:
                new_record[v] = record[v]
                past_values[key][v] = [record[v], t]

        # Store new record to local storage and return
        self.local_storage.set(LocalStorage.PAST_VALUES, past_values)
        return new_record

    # Report by exception if data has id's
    def __report_by_exception_ids__(self, key, data):
        """
        This method, when called multiple times, returns only the records in data that 
        are different from the previous call. This works by storing the data in the
        local storage and using it in the next call to check for new or modified records. 
        
        It only works if every record has an 'id_' field, which is the id of each record.
        Also, records must be flat.
        """
        
        # Organize records by id
        data_as_dict = {r["id_"]: r for r in data if "id_" in r}
        if len(data_as_dict) == 0: return data

        # Get past values
        past_values = self.local_storage.get(LocalStorage.PAST_VALUES, {})
        if key not in past_values:
            past_values[key] = data_as_dict
            self.local_storage.set(LocalStorage.PAST_VALUES, past_values)
            return data

        aux_past = past_values[key]

        # Create a list to keep the records that are different from past values
        data_that_changed = []

        # For each record in the new data
        for record_id in data_as_dict:
            changed = False
            
            # If record is new:
            if record_id not in aux_past: changed = True
            # If the # of fields in the past record is different from the new record:
            elif len(data_as_dict[record_id]) != len(aux_past[record_id]): changed = True
            # If some value has changed
            elif False in [data_as_dict[record_id][v] == aux_past[record_id][v] for v in data_as_dict[record_id] if v != "t"]: changed = True
            
            # If changed, add to data_that_changed:
            if changed: data_that_changed.append(data_as_dict[record_id].copy())

        # For records that are not in the new data, mark as deleted
        # if len(aux_past) != len(data_as_dict):
        #     for record_id in aux_past:
        #         if record_id not in data_as_dict:
        #             data_that_changed.append({"id_": record_id, "deleted_": True})

        past_values[key] = data_as_dict
        self.local_storage.set(LocalStorage.PAST_VALUES, past_values)
        return data_that_changed
