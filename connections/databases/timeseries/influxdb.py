"""

"""

from ...connection import Connection
from influxdb import InfluxDBClient
import math


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
        since = kwargs["since"].strftime('%Y-%m-%d %H:%M:%S')
        until = kwargs["until"].strftime('%Y-%m-%d %H:%M:%S')

        # get field ids
        fields = kwargs["fields"]
        if kwargs["fields"] == "*":
            fields_str = "*"
            null_filter = ""
        else:
            if isinstance(fields, str): field = [fields]
            field_ids = [x for x in field]
            fields_str = ",".join(field_ids)
            null_filter = " AND (" + "".join([x + ' IS NOT NULL OR ' for x in field_ids])[0:-4] + ")"

        #if where is None: where = ""
        #query = "SELECT " + fields_str + " FROM \"" + key + "\" WHERE time >= '" + since_t + "' AND time <= '" + until_t + "'" + " AND (" + where + ")" + " ORDER BY time DESC LIMIT " + str(
        #    count)
        print(query)
        q = list(self.client.query(query))
        if len(q) == 0: return []

        result = q[0]
        for r in result:
            r["t"] = datetime.datetime.strptime(r["time"], "%Y-%m-%dT%H:%M:%SZ")
            r.pop("time")

        return result

    # ===================================================================================
    # Write
    # ===================================================================================
    def write(self, key, data):
        for d in data:
            self.__write_one__(key, d)
        if len(self.data_buffer) >= self.buffer_size:
            self.client.write_points(self.data_buffer, database=self.database)
            self.data_buffer.clear()

    def __write_one__(self, key, record):
        record_ = {}
        for field in record:
            v = self.__check_value__(record[field])
            if v is None: continue
            record_[field] = v

        influx_record = {"measurement": self.__parse_key__(key), "tags": {}, "fields": record_, "time": record_["t"]}
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