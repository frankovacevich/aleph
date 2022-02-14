"""

"""

import datetime
import math
from influxdb import InfluxDBClient
from ...connection import Connection
from ....common.database_field_parse import *
from ....common.data_filter import DataFilter


class Conn(Connection):

    def __init__(self):
        super().__init__()

        # Settings
        self.server = "localhost"
        self.port = 8086
        self.username = ""
        self.password = ""
        self.database = "default"

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
        kwargs["skip_read_cleaning"] = True
        key = db_parse_key(key)

        # Time filter
        since = args["since"].strftime('%Y-%m-%d %H:%M:%S')
        until = args["until"].strftime('%Y-%m-%d %H:%M:%S')
        time_filter = 'time >= "' + since + '" AND time <= "' + until + '"'

        # Filter
        data_filter = DataFilter.load(args["filter"]).to_sql_where_clause()
        where_clause = " WHERE (" + time_filter + ") AND (" + data_filter + ")"

        # Fields
        fields = args["fields"]
        if fields != "*": fields = ",".join(fields)

        # Limit and offset
        limit_and_offset = ""
        if args["limit"] != 0: limit_and_offset = " LIMIT " + str(args["limit"])
        if args["offset"] != 0: limit_and_offset = " OFFSET " + str(args["offset"])

        # Order
        if args["order"][0] == "-":
            sorting_field = args["order"][1:]
            sorting_clause = " ORDER BY " + sorting_field + " DESCENDING"
        else:
            sorting_field = args["order"]
            sorting_clause = " ORDER BY " + sorting_field + " ASCENDING"

        query = "SELECT " + fields + ", time as t FROM " + key + where_clause + sorting_clause + limit_and_offset

        print(query)
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
            v = self.__check_value__(record[field])
            if v is None: continue
            record_[field] = v

        influx_record = {"measurement": db_parse_key(key), "tags": {}, "fields": record_, "time": record_["t"]}
        self.data_buffer.append(influx_record)

    # ===================================================================================
    # Aux
    # ===================================================================================

    @staticmethod
    def __check_value__(v):
        if isinstance(v, float):
            if math.isinf(v) or math.isnan(v): return None
        if isinstance(v, int):
            return float(v)
        return v