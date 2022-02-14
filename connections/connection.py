from ..common.exceptions import *
from ..common.datetime_functions import *
from ..common.wait_one_step import WaitOneStep
from ..common.dict_functions import *
from ..common.local_storage import LocalStorage
from ..common.data_filter import DataFilter

import threading
import os
import json


class Connection:

    def __init__(self, client_id=""):

        # Optional parameters
        self.client_id = client_id               # Assign the connection a client id
        self.report_by_exception = False         # When reading data, only returns changing values
        self.models = {}                         # Dict {key: Model} (for data validation)
        self.persistent = True                   # Remembers last record read (equivalent to mqtt clean session = False)
        self.store_and_forward = False           # Resends failed writes
        self.local_storage = LocalStorage()      # Local storage
        self.accept_stale_messages = False       # Include stale messages when subscribed (see docs)

        # Internal
        self.__connected__ = False
        self.__unsubscribe_flags__ = {}
        self.compare_to_previous = False         # Use on read function to compare values after reading (see docs)
        self.skip_read_cleaning = False          # Use on read function to skip clean_read_data() (see docs)

    # ===================================================================================
    # Main functions (override me)
    # ===================================================================================
    def open(self):
        """
        Opens the connection. If it fails, it must raise an Exception
        """
        self.on_connect()

    def close(self):
        """
        Closes the connection.
        """
        self.on_disconnect()

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

    def on_read_error(self, key, error_message):
        """
        Callback function for when safe_read fails
        """
        return

    def on_write_error(self, key, error_message):
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
        if not self.persistent: self.local_storage.set(self.client_id + LocalStorage.Pre.LAST_TIME_READ, {})
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

    def safe_read(self, key, **kwargs):
        """
        Executes the read function safely, checking for models and report by exception.
        If new data is available, the on_new_data method is executed.
        If there is an error, the on_read_error method is executed
        """
        try:
            # Oopen connection
            if not self.connected(): self.open()
            # Clean arguments and read data
            args = self.__clean_read_args__(key, **kwargs)
            # Call read function
            data = self.read(key, **args)
            # Compare to past values
            if self.compare_to_previous: data = self.__compare_to_previous__(data)
            # Clean and return
            data = self.__clean_read_data__(key, data, **args)
            return data

        except Exception as e:
            self.on_read_error(key, get_error_and_traceback_message(e))
            return []

    def safe_write(self, key, data):
        """
        Executes the write function safely.
        Returns a bool: True for success, False for failure
        """
        # Clean data
        try:
            data = self.__clean_write_data__(key, data)
        except Exception as e:
            self.on_write_error(key, get_error_and_traceback_message(e))
            return False

        # Write data
        try:
            if not self.connected(): self.open()
            self.write(key, data)
            self.__store_and_forward_flush_buffer__()
            return True
        except Exception as e:
            if self.store_and_forward: self.__store_and_forward_add_to_buffer__(key, data)
            self.on_write_error(key, get_error_and_traceback_message(e))
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
            if not self.__unsubscribe_flags__[key]: break
            t = w.step(t)
            data = self.safe_read(key)
            self.on_new_data(key, data)

    def __read_async_aux__(self, key, **kwargs):
        data = self.safe_read(key, **kwargs)
        self.on_new_data(key, data)

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
               Default: self.last_record_sent["t"]
               Accepts: int (days from today or unix), string (parseable datetime format), empty string (today at 00:00),
                        datetime.datetime, None (since the beginning of time)
               Cleaned: datetime.datetime (with tzinfo=UTC) or None

        until: timestamp.
               Default: None
               Accepts: int (days from today or unix), string (parseable datetime format), empty string (tomorrow),
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
               Default: "t"
               Accepts: string (field). Place a "-" in front of the field for reversed order (like "-t")
               Cleaned: string

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

        # Get last read time
        last_t = self.local_storage.get(self.client_id + LocalStorage.Pre.LAST_TIME_READ)
        if last_t is not None and key in last_t: last_t = last_t[key]
        else: last_t = now()

        # Preset args
        args = {
            "fields": "*",
            "since": last_t,
            "until": None,
            "limit": 0,
            "offset": 0,
            "timezone": "UTC",
            "order": "t",
            "filter": None,
            "cleaned": False
        }

        # Fields
        if "fields" in kwargs:
            fields = kwargs["fields"]
            if fields == "*": args["fields"] = fields
            elif "," in fields: args["fields"] = fields.replace(" ", "").split(",")
            elif isinstance(fields, str): args["fields"] = [fields]

        if "since" in kwargs: args["since"] = parse_date(kwargs["since"])
        if "until" in kwargs: args["until"] = parse_date(kwargs["until"])
        if "limit" in kwargs: args["limit"] = int(kwargs["limit"])
        if "offset" in kwargs: args["offset"] = int(kwargs["offset"])
        if "timezone" in kwargs: args["timezone"] = kwargs["timezone"]
        if "order" in kwargs: args["order"] = str(kwargs["order"])
        if "filter" in kwargs: args["filter"] = DataFilter.load(kwargs["filter"])

        args["cleaned"] = True
        return args

    def __clean_read_data__(self, key, data, **kwargs):
        """
        After reading data, make sure that:
        - Data is list of records
        - Every record in data has a timestamp (string)
        - Every record in data has a timestamp (string)
        - Records are not stale (timestamp is between kwargs since and until)
        - Check timezone, filter, order, offset and limit from kwargs
        - Check that records are not empty
        """

        # Data is list of records
        if not isinstance(data, list): data = [data]
        cleaned_data = []

        # Skip cleaning (use for better performance if cleaning is done on read())
        if self.skip_read_cleaning:
            print("HEHEHE", kwargs["timezone"])
            if kwargs["timezone"] == "UTC": return data
            for record in data: record["t"] = parse_date_to_string(record["t"], kwargs["timezone"])
            return data

        for record in data:

            # Every record in data has a timestamp
            if "t" not in record: record["t"] = now()
            else: record["t"] = parse_date(record["t"])
            # Check if not stale
            if not self.accept_stale_messages and not kwargs["since"] <= record["t"] <= kwargs["until"]: continue
            # Filter
            if kwargs["filter"] is not None: record = kwargs["filter"].apply_to(record)
            # Fields
            if kwargs["fields"] != "*": record = {f: record[f] for f in record if f in kwargs["fields"]}
            # Check if record is not empty
            if not self.__check_record_is_not_empty__(record): continue
            # Change time from datetime to string
            record["t"] = date_to_string(record["t"], kwargs["timezone"])

            cleaned_data.append(record)
            continue

        # Order
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
            if "t" not in record: record["t"] = now(string=True)
            else: record["t"] = parse_date_to_string(record["t"])
            # Flatten record
            record = flatten_dict(record)
            # Check model
            if key in self.models: self.models[key].validate(record)
            # Check report by exception
            if self.report_by_exception: record = self.__check_report_by_exception__(key, record)
            # Check that record is not empty
            if not self.__check_record_is_not_empty__(record): continue

            cleaned_data.append(record)
            continue

        return cleaned_data

    def __check_record_is_not_empty__(self, record):
        if len([x for x in record if x not in ["t", "t_", "deleted_", "ignore_", "id_"]]) > 0: return True
        return False

    def __check_report_by_exception__(self, key, record):
        """
        Takes a record (dict) and returns a new record with only the fields whose values are
        different from that of the last_record_sent. The record must be flattened first.
        """

        # Get last record sent from local storage
        last_record = self.local_storage.get(self.client_id + LocalStorage.Pre.LAST_RECORD_SENT)
        if last_record is None: last_record = {}
        if key not in last_record: last_record[key] = {}
            
        # For each field in record, check if changed
        new_record = {}
        for v in record:
            if v in ["t", "t_", "deleted_", "ignore_", "id_"]: continue
            if v not in last_record[key] or last_record[key][v] != record[v]: new_record[v] = record[v]

        # Store new record to local storage and return
        self.local_storage.set(self.client_id + LocalStorage.Pre.LAST_RECORD_SENT, new_record)
        return new_record

    # ===================================================================================
    # Store and forward
    # ===================================================================================
    def __store_and_forward_add_to_buffer__(self, key, data):
        # Get buffer from local storage
        buffer = self.local_storage.get(self.client_id + LocalStorage.Pre.SNF_BUFFER)
        
        # Add data to buffer
        if key not in buffer: buffer[key] = []
        buffer.append(data)
        
        # Save buffer to local storage
        self.local_storage.set(self.client_id + LocalStorage.Pre.SNF_BUFFER, buffer)

    def __store_and_forward_flush_buffer__(self):
        # Get buffer from local storage
        buffer = self.local_storage.get(self.client_id + LocalStorage.Pre.SNF_BUFFER)
        if len(buffer) == 0: return

        # For each record in the buffer, try to write
        new_buffer = {}
        for key in buffer:
            data = buffer[key]
            try: self.write(key, data)
            except: new_buffer[key] = data

        # Save buffer to local storage
        self.local_storage.set(self.client_id + LocalStorage.Pre.SNF_BUFFER, buffer)

    # ===================================================================================
    # Compare to past values
    # ===================================================================================
    def __compare_to_previous__(self, data):
        """
        This method, when called multiple times, returns only the records in data that 
        are different from the previous call. This works by storing the data in the
        local storage and using it in the next call to check for new or modified records. 
        
        It only works if every record has an 'id_' field, which is the id of each record. 
        """
        
        # Organize records by id
        data_as_dict = {r["id_"]: r for r in data if "id_" in r}
        if len(data_as_dict) == 0: return data

        # Get past values
        past_values = self.local_storage.get(self.client_id + LocalStorage.Pre.PAST_VALUES)

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
            if changed: data_that_changed.append(data_as_dict[record_id])

        # For records that are not in the new data, mark as deleted
        if len(past_values) != len(data_as_dict):
            for record_id in past_values:
                if record_id not in data_as_dict:
                    data_that_changed.append({"id_": record_id, "t": now(string=True), "deleted_": True})

        self.local_storage.set(self.client_id + LocalStorage.Pre.PAST_VALUES, data_as_dict)
        return data_that_changed
