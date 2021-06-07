import psycopg2
import time
import datetime
import urllib.parse
from dateutil.tz import tzutc


class PostgresConnection:

    def __init__(self, username, password, database, server="localhost", port=5432):
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.database = database

        ##
        self.client = None
        return

    def connect(self):
        self.client = psycopg2.connect(database=self.database, user=self.username,
                                       password=self.password, host=self.server, port=self.port)
        return

    def close(self):

        pass

    def __int_limits__(self, v):
        if v > 2147483647: return 2147483647
        if v < -2147483647: return -2147483647
        return v

    def __float_limits__(self, v):
        return v

    def save_to_database(self, key, data):
        dat = data.copy()

        # 1. Create table if not exists
        query = 'CREATE TABLE IF NOT EXISTS "' + key + '" (id BIGSERIAL PRIMARY KEY, t TIMESTAMP NOT NULL)'
        cur = self.client.cursor()
        cur.execute(query)
        self.client.commit()
        cur.close()

        # 2. Make time a timestamp and id_ a string
        dat["t"] = dat["t"].replace("T", " ").replace("Z", "")
        if "id_" in dat: dat["id_"] = str(dat["id_"])

        # 3. Check if already exists
        columns = self.get_fields(key)
        already_exists = False
        if "id_" in columns and "id_" in dat:
            query = 'SELECT COUNT(*) FROM "' + key + '" WHERE id_=\'' + dat["id_"] + '\' LIMIT 1'
            cur = self.client.cursor()
            cur.execute(query)
            r = cur.fetchall()
            cur.close()
            already_exists = r[0][0] > 0

        if already_exists:
            dat["t_"] = dat["t"]
            del dat["t"]

        # 4. Get data
        query_update_table = ''
        query_insert = ''
        query_values = ''

        for field in dat:
            v = dat[field]

            if not already_exists:
                if isinstance(v, str) or isinstance(v, bool) or isinstance(v, int) or isinstance(v, float):
                    query_insert += '"' + field + '",'

                if isinstance(v, str): query_values += '\'' + str(v) + '\','
                elif isinstance(v, bool): query_values += ('TRUE' if v else 'FALSE') + ','
                elif isinstance(v, int): query_values += str(self.__int_limits__(v)) + ','
                elif isinstance(v, float): query_values += str(self.__float_limits__(v)) + ','

            else:
                # if isinstance(v, str) or isinstance(v, bool) or isinstance(v, int) or isinstance(v, float):
                #     query_insert += """ + field + "","

                if isinstance(v, str): query_values += '"' + field + '"=\'' + str(v) + '\','
                elif isinstance(v, bool): query_values += '"' + field + '"=' + ('TRUE' if v else 'FALSE') + ','
                elif isinstance(v, int): query_values += '"' + field + '"=' + str(self.__int_limits__(v)) + ','
                elif isinstance(v, float): query_values += '"' + field + '"=' + str(self.__float_limits__(v)) + ','

            if field in columns: continue
            if isinstance(v, str): query_update_table += 'ALTER TABLE "' + key + '" ADD "' + field + '" VARCHAR(255);'
            elif isinstance(v, bool): query_update_table += 'ALTER TABLE "' + key + '" ADD "' + field + '" BOOL;'
            elif isinstance(v, int): query_update_table += 'ALTER TABLE "' + key + '" ADD "' + field + '" INT;'
            elif isinstance(v, float): query_update_table += 'ALTER TABLE "' + key + '" ADD "' + field + '" FLOAT(32);'

        # 5. Update columns
        if query_update_table != '':
            for q in query_update_table.split(";"):
                if q == "": continue
                cur = self.client.cursor()
                cur.execute(q)
            self.client.commit()
            cur.close()

        # 6. Insert/Update data
        if already_exists:
            query_insert = 'UPDATE "' + key + '" SET ' + query_values[0:-1] + ' WHERE id_=\'' + data["id_"] + '\''
        else:
            query_insert = 'INSERT INTO "' + key + '" (' + query_insert[0:-1] + ') VALUES (' + query_values[0:-1] + ')'
        cur = self.client.cursor()
        cur.execute(query_insert)
        self.client.commit()
        cur.close()

        return

    def __format_data_for_return__(self, key, data, field="*"):

        if field != "*":
            if "id_" in data:
                return [{field: d[0], "t": d[1], "id_": d[2]} for d in data]
            else:
                return [{field: d[0], "t": d[1]} for d in data]

        result = []
        allfields = self.get_fields(key)
        for row in data:
            n_result = {}
            for i in range(0, len(row)):
                f = allfields[i]
                if f == "id": continue
                n_result[f] = row[i]
                continue
            result.append(n_result)

        return result

    def get_from_database(self, key, field, since, until, count, ffilter):
        if key not in self.get_all_keys(): return []

        since_t = (datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=since)).strftime('%Y-%m-%d %H:%M:%S')
        until_t = (datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=until)).strftime('%Y-%m-%d %H:%M:%S')

        if field == "*":
            query = 'SELECT * FROM "' + key + '" WHERE t >= \'' + str(since_t) + '\' AND t <= \'' + str(until_t) + '\' ORDER BY t DESC LIMIT ' + str(count)
        else:
            aux = ",t "
            if "id_" in self.get_fields(): aux = ",t , id_ "
            query = 'SELECT ' + field + aux + ' FROM "' + key + '" WHERE t >= \'' + str(since_t) + '\' AND t <= \'' + str(until_t) + '\' AND ' + field + ' IS NOT NULL ORDER BY t DESC LIMIT ' + str(count)

        cur = self.client.cursor()
        cur.execute(query)
        r = cur.fetchall()
        cur.close()
        return self.__format_data_for_return__(key, r, field)


    def get_from_database_by_id(self, key, id_):
        query = "SELECT * FROM \"" + key + "\" WHERE id_ = '" + id_ + "' LIMIT 1"
        cur = self.client.cursor()
        cur.execute(query)
        r = cur.fetchall()
        cur.close()
        return self.__format_data_for_return__(key, r)

    def delete_records(self, key, since, until):
        since_t = (datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=since)).strftime('%Y-%m-%d %H:%M:%S')
        until_t = (datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=until)).strftime('%Y-%m-%d %H:%M:%S')
        query = "DELETE FROM \"" + key + "\" WHERE t >= '" + str(since_t) + "' AND t < '" + str(until_t) + "'"
        cur = self.client.cursor()
        cur.execute(query)
        q = cur.rowcount
        self.client.commit()
        cur.close()
        return q

    def delete_record_by_id(self, key, id_):
        query = "DELETE FROM \"" + key + "\" WHERE id_ = \'" + str(id_) + "\'"
        cur = self.client.cursor()
        cur.execute(query)
        q = cur.rowcount
        self.client.commit()
        cur.close()
        return q

    def get_all_keys(self):
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        cur = self.client.cursor()
        cur.execute(query)
        r = cur.fetchall()
        cur.close()
        return [x[0] for x in r]

    def get_fields(self, key):
        query = 'SELECT column_name FROM information_schema.columns WHERE table_name = \'' + key + '\''
        cur = self.client.cursor()
        cur.execute(query)
        r = cur.fetchall()
        cur.close()
        return [x[0] for x in r if x != "id"]

    def remove_key(self, key):
        query = "DROP TABLE IF EXISTS \"" + key + "\";"
        cur = self.client.cursor()
        cur.execute(query)
        self.client.commit()
        cur.close()
        return

    def remove_field(self, key, field):
        query = "ALTER TABLE \"" + key + "\" DROP COLUMN \"" + field + "\";"
        cur = self.client.cursor()
        cur.execute(query)
        cur.close()
        self.client.commit()
        return

    def rename_key(self, key, new_key):
        query = "ALTER TABLE \"" + key + "\" RENAME TO \"" + new_key + "\";"
        cur = self.client.cursor()
        cur.execute(query)
        self.client.commit()
        return

    def rename_field(self, key, field, new_field):
        query = 'ALTER TABLE "' + key + '" RENAME COLUMN \"' + field + '" TO "' + new_field + '";'
        cur = self.client.cursor()
        cur.execute(query)
        cur.close()

        self.client.commit()
        self.close()
        return

    def count_all_records(self):
        keys = self.get_all_keys()

        all = {}
        total = 0
        for key in keys:
            query = 'SELECT COUNT(t) FROM "' + key + '";'
            cur = self.client.cursor()
            cur.execute(query)
            r = cur.fetchall()
            cur.close()
            count = r[0][0]

            total += count
            all[key] = count

        all["__total__"] = total
        return all
