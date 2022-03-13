from influxdb import InfluxDBClient
from ...connection import Connection
from ....common.database_field_parse import *


class InfluxDBTimeSeriesConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)

        # Settings
        self.server = "localhost"
        self.port = 8086
        self.username = ""
        self.password = ""
        self.database = "main"

        # Internal
        self.check_filters_on_read = False
        self.check_timestamp_on_read = False

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
        since = args.pop("since", None)
        until = args.pop("until", None)
        if since is None and until is None: time_filter = ""
        elif since is not None: time_filter = "time >= '" + since.strftime('%Y-%m-%d %H:%M:%S') + "'"
        elif until is not None: time_filter = "time <= '" + until.strftime('%Y-%m-%d %H:%M:%S') + "'"
        else: time_filter = "time >= '" + since.strftime('%Y-%m-%d %H:%M:%S') + "' AND time <= '" + until.strftime('%Y-%m-%d %H:%M:%S') + "'"

        # Filter
        where_clause = " WHERE " + time_filter
        data_filter = ""
        args_filter = args.pop("filter", None)
        if args_filter is not None: data_filter = args_filter.to_sql_where_clause()
        if data_filter != "": where_clause = " AND (" + data_filter + ")"

        # Fields
        fields = args.pop("fields", "*")
        if fields != "*": fields = ",".join(fields)

        # Limit and offset
        limit_and_offset = ""
        limit = args.pop("limit", 0)
        offset = args.pop("offset", 0)
        if limit != 0: limit_and_offset = " LIMIT " + str(limit)
        if offset != 0: limit_and_offset = " OFFSET " + str(offset)

        # Order: influx only supports sorting by time
        order = args.pop("order", "t")
        if order == "t": sorting_clause = " ORDER BY time ASC"
        elif order == "-t": sorting_clause = " ORDER BY time DESC"
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
