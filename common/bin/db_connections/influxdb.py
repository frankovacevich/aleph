"""

"""

from influxdb import InfluxDBClient
import datetime
import math
from dateutil.tz import tzutc


class InfluxDBConnection:

    def __init__(self, username, password, database, server="localhost", port=8086):
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.database = database

        ##
        self.client = None
        self.data_buffer = []
        self.buffer_size = 15000
        return

    # Connect to database
    def connect(self):
        self.client = InfluxDBClient(self.server, self.port, self.username, self.password, self.database)
        return

    def close(self):
        self.client.close()
        return

    # ==========================================================================
    # Operations (save, get, delete)
    # ==========================================================================

    def save_data(self, key, data):
        try:
            dat = {}
            for x in data:
                y = data[x]
                if isinstance(y, float) and (math.isinf(y) or math.isnan(y)): continue
                if isinstance(y, int): y = float(y)
                dat[x] = y

            item = {"measurement": key, "tags": {}, "fields": dat, "time": data["t"]}

            self.data_buffer.append(item)
            if len(self.data_buffer) >= self.buffer_size:
                self.client.write_points(self.data_buffer, database=self.database)
                self.data_buffer.clear()

        except:
            for item in self.data_buffer:
                try:
                    self.client.write_points([item], database=self.database)
                except:
                   pass

            self.data_buffer.clear()
            raise

        return

    def get_data(self, key, field, since, until, count):
        if key not in self.get_keys(): return []
        since_t = since.strftime('%Y-%m-%d %H:%M:%S')
        until_t = until.strftime('%Y-%m-%d %H:%M:%S')

        # get field ids
        if field == "*":
            fields_str = "*"
            null_filter = ""
        else:
            if isinstance(field, str): field = [field]
            field_ids = [x for x in field]
            fields_str = ",".join(field_ids)
            null_filter = " AND (" + "".join([x + ' IS NOT NULL OR ' for x in field_ids])[0:-4] + ")"

        query = "SELECT " + fields_str + " FROM \"" + key + "\" WHERE time >= '" + since_t + "' AND time <= '" + until_t + "'" + "" + " ORDER BY time DESC LIMIT " + str(count)
        print(query)
        q = list(self.client.query(query))
        if len(q) == 0: return []

        result = q[0]
        for r in result:
            r["t"] = datetime.datetime.strptime(r["time"], "%Y-%m-%dT%H:%M:%SZ")
            r.pop("time")

        return result

    def get_data_by_id(self, key, id_):
        # should not be implemented on influx since update queries are not optimized
        # use another db engine to store id'd data
        raise Exception("Invalid method")

    def delete_data(self, key, since, until):
        since_t = since.strftime('%Y-%m-%d %H:%M:%S')
        until_t = until.strftime('%Y-%m-%d %H:%M:%S')
        query = "DELETE FROM \"" + key + "\" WHERE time >= '" + str(since_t) + "' AND time <= '" + str(until_t) + "'"

        q = list(self.client.query(query))
        return q

    def delete_data_by_id(self, key, id_):
        # should not be implemented on influx since update queries are not optimized
        # use another db engine to store id'd data
        raise Exception("Invalid method")

    # ==========================================================================
    # Get keys and fields
    # ==========================================================================

    def get_keys(self):
        query = "SHOW MEASUREMENTS ON " + self.database
        q = self.client.query(query)
        if len(q) == 0: return []
        return [x["name"] for x in list(q)[0]]

    def get_fields(self, key):
        query = 'SHOW FIELD KEYS ON ' + self.database + ' FROM "' + key + '"'
        q = self.client.query(query)
        if len(q) == 0: return []
        return [x["fieldKey"] for x in list(q)[0]]

    def remove_key(self, key):
        self.client.drop_measurement(key)
        return

    def remove_field(self, key, field):
        raise Exception("Invalid method")

    def rename_key(self, key, new_key):
        query = f'SELECT * INTO "{new_key}" FROM "{key}"'
        q = self.client.query(query)
        query = f'DROP MEASUREMENT "{key}"'
        q = self.client.query(query)
        return

    def rename_field(self, key, field, new_field):
        raise Exception("Invalid method")


    # ==========================================================================
    # Metadata
    # ==========================================================================

    def get_metadata(self, key):
        raise Exception("Invalid method")

    def set_metadata(self, key, field, alias, description):
        raise Exception("Invalid method")

    # ==========================================================================
    # Other
    # ==========================================================================

    def count_all_records(self):
        keys = self.get_all_keys()

        all = {}
        total = 0
        for key in keys:
            query = 'SELECT COUNT(t) FROM "' + key + '"'
            q = self.client.query(query)
            count = list(q)[0][0]["count"]
            total += count
            all[key] = count

        all["__total__"] = total
        return all
