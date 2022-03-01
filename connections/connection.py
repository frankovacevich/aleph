from ..common.exceptions import *
from ..common.datetime_functions import *
from ..common.wait_one_step import WaitOneStep
from ..common.dict_functions import *
from ..common.local_storage import LocalStorage
from ..common.data_filter import DataFilter
import threading
import time


class Connection:

    def __init__(self, client_id=""):

        # Optional parameters
        self.client_id = client_id                # Assign the connection a client id (required if persistent = True)
        self.local_storage = LocalStorage()       # Local storage instance

        # Optional parameters: reading
        self.persistent = False                   # Remembers last record (equivalent to mqtt clean session = False)
        self.compare_to_previous_on_read = False  # Use on read function to compare values after reading (see docs)
        self.check_filters_on_read = True         # Check if fields, filter, limit, offset and order are correct
        self.check_timestamp_on_read = True       # Check if record time is between since and until

        # Optional parameters: writing
        self.models = {}                          # Dict {key: Model} (for data validation)
        self.report_by_exception = False          # When reading data, only returns changing values
        self.store_and_forward = False            # Resends failed writes
        self.include_time_on_write = True         # Automatically adds the current timestamp when writing

        # Internal
        self.__connected__ = False
        self.__unsubscribe_flags__ = {}

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
        The **kwargs are optional read arguments (can be read as a dict)
        Use clean_read_args to clean the **kwargs
        """
        return []

    def write(self, key, data):
        """
        Returns None.
        """
        return

    def connected(self):
        """
        Returns a boolean (True if connected, False if not connected)
        """
        return self.__connected__

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

    def on_open_error(self, error):
        """
        Callback function for when opening fails
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
        Callback function called after open(). If overridden, use super().on_connect()
        """
        self.__connected__ = True
        if self.store_and_forward: self.__store_and_forward_flush_buffer__()

        # If not persistent wipe out last times
        if not self.persistent: self.local_storage.set(LocalStorage.LAST_TIME_READ, {})
        return

    def on_disconnect(self):
        """
        Callback function called after close(). If overridden, use super().on_disconnect()
        """
        self.__connected__ = False
        return

    # ===================================================================================
    # Safe functions
    # ===================================================================================

    def safe_open(self):
        try:
            if not self.connected():
                self.open()
                self.on_connect()
        except:
            self.on_open_error(Error(Exceptions.OpenError(), client_id=self.client_id))

    def safe_read(self, key, **kwargs):
        """
        Executes the read function safely, checking for models and report by exception.
        If new data is available, the on_new_data method is executed.
        If there is an error, the on_read_error method is executed
        """
        self.safe_open()

        try:
            # Open connection
            if not self.connected(): self.open()

            # Clean arguments and read data
            args = self.__clean_read_args__(key, **kwargs)

            # Call read function
            data = self.read(key, **args)

            # Store last time read
            if args["until"] is None:
                last_times = self.local_storage.get(LocalStorage.LAST_TIME_READ, {})
                last_times[key] = time.time()
                self.local_storage.set(LocalStorage.LAST_TIME_READ, last_times)

            # Compare to past values
            if self.compare_to_previous_on_read: data = self.__compare_to_previous__(data)
            # Clean data
            data = self.__clean_read_data__(key, data, **args)
            return data

        except:
            self.on_read_error(Error(Exceptions.ReadError(), client_id=self.client_id, key=key, kw_args=kwargs))
            return []

    def safe_write(self, key, data):
        """
        Executes the write function safely.
        Returns a bool: True for success, False for failure
        """
        # Clean data
        try:
            data = self.__clean_write_data__(key, data)
        except:
            self.on_write_error(Error(Exceptions.WriteError(), client_id=self.client_id, key=key, data=data))
            return False

        # Write data
        try:
            if not self.connected(): self.open()
            self.write(key, data)
            if self.store_and_forward: self.__store_and_forward_flush_buffer__()
            return True
        except:
            if self.store_and_forward: self.__store_and_forward_add_to_buffer__(key, data)
            self.on_write_error(Error(Exceptions.WriteError(), client_id=self.client_id, key=key, data=data))
            return False

    # ===================================================================================
    # Async functions
    # ===================================================================================
    def subscribe(self, key, time_step=1):
        """
        Executes the safe_read function in a loop. This method blocks the main thread.
        Implementation may change for each connection
        """
        self.__unsubscribe_flags__[key] = True
        w = WaitOneStep(time_step)
        t = w.step(0)
        while True:
            # Break loop if unsubscribed
            if not self.__unsubscribe_flags__[key]: break
            # Sleep
            t = w.step(t)
            # Read data
            data = self.safe_read(key)
            if len(data) == 0: continue
            # Callback
            self.on_new_data(key, data)

    def __read_async_aux__(self, key, **kwargs):
        data = self.safe_read(key, **kwargs)
        self.on_new_data(key, data)

    def open_async(self, key, **kwargs):
        """
        Executes the open function without blocking the main thread
        """
        open_thread = threading.Thread(target=self.safe_open, name="Open async")
        open_thread.start()

    def read_async(self, key, **kwargs):
        """
        Executes the safe_read function without blocking the main thread.
        """
        read_thread = threading.Thread(target=self.__read_async_aux__, name="Read async", args=(key,), kwargs=kwargs)
        read_thread.start()

    def subscribe_async(self, key, time_step=1):
        """
        Executes the subscribe function without blocking the main thread.
        """
        if key in self.__unsubscribe_flags__: return
        subscribe_thread = threading.Thread(target=self.subscribe, name="Subscribe async", args=(key, time_step,))
        subscribe_thread.start()

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
        last_times = self.local_storage.get(LocalStorage.LAST_TIME_READ, {})
        last_t = parse_date(last_times.pop(key, time.time()))
        last_times[key] = time.time()
        # TODO: check time.time() precision

        # Preset args
        args = {
            "fields": "*",
            "since": last_t,
            "until": None,
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
            elif "," in fields: args["fields"] = fields.repewlace(" ", "").split(",")
            elif isinstance(fields, str): args["fields"] = [fields]

        if "since" in kwargs: args["since"] = parse_date(kwargs["since"])
        if "until" in kwargs: args["until"] = parse_date(kwargs["until"])
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

        # If no checking needed, return.
        if not self.check_filters_on_read and not self.check_timestamp_on_read and kwargs["timezone"] == "UTC":
            return data

        # For each record:
        for record in data:

            # Check filter and fields
            if self.check_filters_on_read:
                if kwargs["filter"] is not None and not kwargs["filter"].apply_to_record(record): continue
                if kwargs["fields"] != "*": record = {f: record[f] for f in record if f in kwargs["fields"] or f == "t" or f == "id_"}

            # Check timestamp
            if self.check_timestamp_on_read:
                # Record has a timestamp
                if "t" not in record: record["t"] = now()
                else: record["t"] = parse_date(record["t"])
                # Timestamp in valid range
                if kwargs["since"] is not None and kwargs["since"] > record["t"]:
                    print("ACA", kwargs["since"], record["t"])
                    continue
                if kwargs["until"] is not None and kwargs["until"] < record["t"]:
                    print("ALLA")
                    continue
                # Timestamp in timezone
                record["t"] = date_to_string(record["t"], kwargs["timezone"])

            # Check timestamp is in the correct timezone
            elif kwargs["timezone"] != "UTC":
                if "t" not in record: record["t"] = now()
                record["t"] = parse_date_to_string(record["t"], kwargs["timezone"])

            cleaned_data.append(record)
            continue

        if self.check_filters_on_read:
            # Order
            if kwargs["order"] is not None:
                if kwargs["order"][0] == "-": cleaned_data.sort(key=lambda x: x[kwargs["order"][1:]], reverse=True)
                else: cleaned_data.sort(key=lambda x: x[kwargs["order"]])

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

            # Time
            if "t" not in record and self.include_time_on_write: record["t"] = now(string=True)
            else: record["t"] = parse_date_to_string(record["t"])
            # Flatten record
            record = flatten_dict(record)
            # Check model
            if key in self.models: record = self.models[key].parse(record)
            # Check report by exception
            if self.report_by_exception: record = self.__check_report_by_exception__(key, record)
            # Check that record is not empty
            if not self.__check_record_is_not_empty__(record): continue

            cleaned_data.append(record)
            continue

        return cleaned_data

    def __check_record_is_not_empty__(self, record):
        if len([x for x in record if x not in ["t", "t_", "ignore_", "id_"]]) > 0: return True
        return False

    def __check_report_by_exception__(self, key, record):
        """
        Takes a record (dict) and returns a new record with only the fields whose values are
        different from that of the last_record_sent. The record must be flattened first.
        """

        # Get last record sent from local storage
        last_records = self.local_storage.get(LocalStorage.LAST_RECORD_SENT, {})
        if key not in last_records: last_records[key] = {}
            
        # For each field in record, check if changed
        new_record = {}
        for v in record:
            if v in ["t", "t_", "deleted_", "ignore_", "id_"]: continue
            if v not in last_records[key] or last_records[key][v] != record[v]: new_record[v] = record[v]

        # Store new record to local storage and return
        last_records[key] = record
        self.local_storage.set(LocalStorage.LAST_RECORD_SENT, last_records)
        return new_record

    # ===================================================================================
    # Store and forward
    # ===================================================================================
    def __store_and_forward_add_to_buffer__(self, key, data):
        # Get buffer from local storage
        buffer = self.local_storage.get(LocalStorage.SNF_BUFFER)
        if buffer is None: buffer = {}
        
        # Add data to buffer
        if key not in buffer: buffer[key] = []
        buffer.append(data)
        
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
        self.local_storage.set(LocalStorage.SNF_BUFFER, buffer)

    # ===================================================================================
    # Compare to past values
    # ===================================================================================
    def __compare_to_previous__(self, data):
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
        past_values = self.local_storage.get(LocalStorage.PAST_VALUES)
        if past_values is None:
            self.local_storage.set(LocalStorage.PAST_VALUES, data_as_dict)
            return []

        # Create a list to keep the records that are different from past values
        data_that_changed = []

        # For each record in the new data
        for record_id in data_as_dict:
            changed = False
            
            # If record is new:
            if record_id not in past_values: changed = True
            # If the # of fields in the past record is different from the new record:
            elif len(data_as_dict[record_id]) != len(past_values[record_id]): changed = True
            # If some value has changed
            elif False in [data_as_dict[record_id][v] == past_values[record_id][v] for v in data_as_dict[record_id] if v != "t"]: changed = True
            
            # If changed, add to data_that_changed:
            if changed: data_that_changed.append(data_as_dict[record_id].copy())

        # For records that are not in the new data, mark as deleted
        if len(past_values) != len(data_as_dict):
            for record_id in past_values:
                if record_id not in data_as_dict:
                    data_that_changed.append({"id_": record_id, "deleted_": True})

        self.local_storage.set(LocalStorage.PAST_VALUES, data_as_dict)
        return data_that_changed

    # ===================================================================================
    # Other
    # ===================================================================================
    def __store_last_time_read__(self, key, t):
        last_times = self.local_storage.get(LocalStorage.LAST_TIME_READ, {})
        last_times[key] = t
        self.local_storage.set(LocalStorage.LAST_TIME_READ, last_times)
