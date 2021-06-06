"""
- connect()
- save_to_database(key, data)
- get_from_database(key, since)
- get_all_keys()
- get_fields(key)

"""

from influxdb import InfluxDBClient
import datetime
import math


class InfluxDBConnection:

    def __init__(self, username, password, database, server="localhost", port=8086):
        self.server = "localhost"
        self.port = 8086
        self.username = username
        self.password = password
        self.database = database

        ##
        self.client = None
        self.data_buffer = []
        self.buffer_size = 5000
        return

    # Connect to database
    def connect(self):
        self.client = InfluxDBClient(self.server, self.port, self.username, self.password, self.database)
        return

    def close(self):
        self.client.close()
        return

    # Save to database: receives a dict and stores it's contents on the database
    # key = namespace path
    # data = a 1-depth dict {field:value}
    def save_to_database(self, key, data):
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

    def get_from_database(self, key, field, since, until, count):
        if key not in self.get_all_keys(): return []
        if field != "*": field = f'"{field}"'
        query = f'SELECT {field} FROM "{key}" WHERE time >= now() - {str(since)}d AND time <= now() - {str(until)}d ORDER BY time DESC LIMIT {str(count)}'

        q = list(self.client.query(query))

        if len(q) == 0: return []

        result = q[0]
        for r in result:
            r["t"] = datetime.datetime.strptime(r["time"], "%Y-%m-%dT%H:%M:%SZ")
            r.pop("time")

        return result

    def get_from_database_by_id(self, key, id_):
        # should not be implemented on influx since update queries are not optimized
        # use another db engine to store id'd data
        return None

    def delete_records(self, key, since, until):
        query = f'DELETE FROM "{key}" WHERE time >= now() - {str(since)}d AND time <= now() - {str(until)}d'
        q = list(self.client.query(query))
        return q

    def delete_record_by_id(self, key, id_):
        # should not be implemented on influx since update queries are not optimized
        # use another db engine to store id'd data
        return None

    def get_all_keys(self):
        query = f"SHOW MEASUREMENTS ON {self.database}"
        q = self.client.query(query)
        if len(q) == 0: return []

        return [x["name"] for x in list(q)[0]]

    def get_fields(self, key):
        query = f'SHOW FIELD KEYS ON {self.database} FROM "{key}"'
        q = self.client.query(query)
        if len(q) == 0: return []

        return [x["fieldKey"] for x in list(q)[0]]

    def remove_key(self, key):
        self.client.drop_measurement(key)
        return

    def remove_field(self, key, field):
        # Not implemented
        raise Exception("Not implemented")
        return

    def rename_key(self, key, new_key):
        query = f'SELECT * INTO "{new_key}" FROM "{key}"'
        q = self.client.query(query)
        query = f'DROP MEASUREMENT "{key}"'
        q = self.client.query(query)
        return

    def rename_field(self, key, field, new_field):
        # Not implemented
        raise Exception("Not implemented")
        return

    def count_all_records(self):
        keys = self.get_all_keys()

        all = {}
        total = 0
        for key in keys:
            query = f'SELECT COUNT(t) FROM "{key}"'
            q = self.client.query(query)
            count = list(q)[0][0]["count"]
            total += count
            all[key] = count

        all["__total__"] = total
        return all
