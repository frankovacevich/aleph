"""

"""

import datetime
import math
from influxdb import InfluxDBClient
from ...connection import Connection
from ....common.database_field_parse import *
from ....common.data_filter import DataFilter


class InfluxDBTimeSeries(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)

        # Settings
        self.server = "localhost"
        self.port = 8086
        self.username = ""
        self.password = ""
        self.database = "main"

        # Internal
        self.client = None
        self.data_buffer = []
        self.buffer_size = 10000

    # ===================================================================================
    # Open
    # ===================================================================================
    def open(self):
        self.client = InfluxDBClient(self.server, self.port, self.username, self.password, self.database)
        super().open()

    def close(self):
        self.client.close()
        super().close()

    # ===================================================================================
    # Read
    # ===================================================================================
    def read(self, key, **kwargs):
        # Parse args and key
        args = self.__clean_read_args__(key, **kwargs)
        key = db_parse_key(key)

        # Time filter
        since = args["since"].strftime('%Y-%m-%dT%H:%M:%SZ')
        until = args["until"].strftime('%Y-%m-%dT%H:%M:%SZ')
        if since is not None and until is not None: time_filter = "time >= '" + since + "' AND time <= '" + until + "'"
        elif since is not None: time_filter = "time >= '" + since + "'"
        elif until is not None: time_filter = "time <= '" + until + "'"
        else: time_filter = ""

        # Filter
        where_clause = " WHERE " + time_filter
        data_filter = ""
        if args["filter"] is not None: data_filter = args["filter"].to_sql_where_clause()
        if data_filter != "": where_clause = " AND (" + data_filter + ")"

        # Fields
        fields = args["fields"]
        if fields != "*": fields = ",".join(fields)

        # Limit and offset
        limit_and_offset = ""
        if args["limit"] != 0: limit_and_offset = " LIMIT " + str(args["limit"])
        if args["offset"] != 0: limit_and_offset = " OFFSET " + str(args["offset"])

        # Order: influx only supports sorting by time
        if args["order"] == "t": sorting_clause = " ORDER BY time ASC"
        elif args["order"] == "-t": sorting_clause = " ORDER BY time DESC"
        else: sorting_clause = ""

        # Run query
        query = "SELECT " + fields + ", time as t FROM " + key + where_clause + sorting_clause + limit_and_offset
        q = list(self.client.query(query))
        if len(q) == 0: return []

        result = q[0]
        return result

    # ===================================================================================
    # Write
    # ===================================================================================
    def write(self, key, data):
        for d in data:
            self.__add_to_buffer__(key, d)
        if len(self.data_buffer) >= self.buffer_size:
            self.client.write_points(self.data_buffer, database=self.database)
            self.data_buffer.clear()

    def __add_to_buffer__(self, key, record):
        record_ = {}
        for field in record:
            v = db_check_value(record[field])
            if v is None: continue
            record_[field] = v

        influx_record = {"measurement": db_parse_key(key), "tags": {}, "fields": record_, "time": record_["t"]}
        self.data_buffer.append(influx_record)
