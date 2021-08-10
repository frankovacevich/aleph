import math
import datetime


class SQLConnection:

    def __init__(self, dbs="sqlite"):
        if dbs not in ["sqlite", "mariadb", "mysql", "postgres"]:
            raise Exception("Invalid database system")
        self.dbs = dbs
        self.client = None
        return

    def connect(self):
        cur = self.client.cursor()

        if self.dbs == "sqlite":
            sql = 'CREATE TABLE IF NOT EXISTS metadata (id INTEGER PRIMARY KEY AUTOINCREMENT, key_name VARCHAR(255), field VARCHAR(255), field_id VARCHAR(255), alias VARCHAR(255), description VARCHAR(1000) DEFAULT "", created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'
        elif self.dbs == "mariadb":
            sql = 'CREATE TABLE IF NOT EXISTS metadata (id INT NOT NULL AUTO_INCREMENT, key_name VARCHAR(255), field VARCHAR(255), field_id VARCHAR(255), alias VARCHAR(255), description VARCHAR(1000) DEFAULT "", created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`))'
        elif self.dbs == "postgres":
            sql = 'CREATE TABLE IF NOT EXISTS metadata (id BIGSERIAL PRIMARY KEY, key_name VARCHAR(255), field VARCHAR(255), field_id VARCHAR(255), alias VARCHAR(255), description VARCHAR(1000) DEFAULT \'\', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'

        cur.execute(sql)
        sql = 'CREATE INDEX IF NOT EXISTS idx_key ON metadata (key_name)'
        cur.execute(sql)
        self.client.commit()
        cur.close()

    # ==========================================================================
    # Operations (save, get, delete)
    # ==========================================================================

    def save_data(self, key, data):
        if key == "metadata": raise Exception("Invalid key")

        cur = self.client.cursor()
        key_org = key
        key = self.__format_key__(key)
        dat = data.copy()

        # Make time a timestamp and id_ a string
        dat["t"] = dat["t"].replace("T", " ").replace("Z", "")
        if "id_" in dat: dat["id_"] = str(dat["id_"])

        # Create table if not exists
        if self.dbs == "sqlite":
            sql = 'CREATE TABLE IF NOT EXISTS ' + key + ' (id INTEGER PRIMARY KEY AUTOINCREMENT, t DATETIME NOT NULL)'
        elif self.dbs == "mariadb":
            sql = 'CREATE TABLE IF NOT EXISTS ' + key + ' (id INT NOT NULL AUTO_INCREMENT, t DATETIME NOT NULL, PRIMARY KEY (`id`))'
        elif self.dbs == "postgres":
            sql = 'CREATE TABLE IF NOT EXISTS ' + key + ' (id BIGSERIAL PRIMARY KEY, t TIMESTAMP NOT NULL)'
            print(sql)
        cur.execute(sql)

        # Get name: field map
        fmap = self.__field_map__(key_org, cur)

        # Check if id_ already exists
        already_exists = False
        if "id_" in data and "id_" in fmap:
            sql = "SELECT COUNT(id_) FROM " + key + " WHERE id_='" + data["id_"] + "' LIMIT 1"
            cur.execute(sql)
            r = cur.fetchall()
            already_exists = r[0][0] > 0

        if already_exists:
            dat["t_"] = dat["t"]
            del dat["t"]

        # Insert data
        query_update_table = ''
        query_insert = ''
        query_values = ''

        for f in dat:
            v = self.__check_value__(dat[f])
            if v is None: continue

            if f in fmap:
                field_id = fmap[f]
            else:
                # Update metadata table
                field_id = f.replace(".", "_")
                field_name = f
                while field_id in fmap.values(): field_id += "_"
                if field_id[0].isnumeric(): field_id = "i" + field_id
                sql = "INSERT INTO metadata(key_name, field_id, field, alias) VALUES ('" + key_org + "', '" + field_id +  "', '" + field_name +  "', '" + field_name + "')"
                cur.execute(sql)

            if not already_exists:

                if isinstance(v, str) or isinstance(v, bool) or isinstance(v, int) or isinstance(v, float):
                    query_insert += "" + field_id + ","

                if isinstance(v, str): query_values += "'" + str(v) + "',"
                elif isinstance(v, bool): query_values += self.__sql_bool__(v) + ','
                elif isinstance(v, int): query_values += str(v) + ','
                elif isinstance(v, float): query_values += str(v) + ','

            else:

                if isinstance(v, str): query_values += "" + field_id + "='" + str(v) + "',"
                if isinstance(v, bool): query_values += "" + field_id + "=" + self.__sql_bool__(v) + ","
                elif isinstance(v, int): query_values += "" + field_id + "=" + str(v) + ","
                elif isinstance(v, float): query_values += "" + field_id + "=" + str(v) + ","

            if f in fmap or f == "t": continue
            if isinstance(v, str): query_update_table += 'ALTER TABLE ' + key + ' ADD ' + field_id + ' VARCHAR(255);'
            elif isinstance(v, bool): query_update_table += 'ALTER TABLE ' + key + ' ADD ' + field_id + ' BOOL;'
            elif isinstance(v, int): query_update_table += 'ALTER TABLE ' + key + ' ADD ' + field_id + ' INT;'
            elif isinstance(v, float): query_update_table += 'ALTER TABLE ' + key + ' ADD ' + field_id + ' FLOAT(32);'

        # Update columns
        if query_update_table != '':
            for q in query_update_table.split(";"):
                if q == "": continue
                try:
                    if self.dbs == "postgres": q = q.replace(" ADD ", " ADD COLUMN IF NOT EXISTS ")
                    cur.execute(q)
                except:
                    pass

        #
        if already_exists:
            query_insert = "UPDATE " + key + " SET " + query_values[0:-1] + " WHERE id_='" + data["id_"] + "'"
            cur.execute(query_insert)
        else:
            query_insert = 'INSERT INTO ' + key + ' (' + query_insert[0:-1] + ') VALUES (' + query_values[0:-1] + ')'
            cur.execute(query_insert)
            if "id_" in dat:
                sql = 'CREATE INDEX IF NOT EXISTS idx_' + key + ' ON ' + key + ' (id_)'
                cur.execute(sql)

        self.client.commit()
        cur.close()

    def get_data(self, key, field, since, until, count):
        cur = self.client.cursor()
        key_org = key
        key = self.__format_key__(key)

        # field map
        fmap = self.__field_map__(key_org, cur)
        fmap_inv = {fmap[x]: x for x in fmap}
        if len(fmap) == 0: return []

        # get field ids
        if field == "*":
            field_ids = list(fmap.values())
        else:
            if isinstance(field, str): field = [field]
            field_ids = [fmap[x] for x in field]

        if "t" not in field_ids: field_ids.append("t")
        if "id_" in fmap and "id_" not in field_ids: field_ids.append("id_")

        fields_str = ",".join(field_ids)
        null_filter = "".join([x + ' IS NOT NULL OR ' for x in field_ids])[0:-4]

        # make time a string
        since_t = since.strftime('%Y-%m-%d %H:%M:%S')
        until_t = until.strftime('%Y-%m-%d %H:%M:%S')

        # run query
        query = "SELECT " + fields_str + " FROM " + key + " WHERE t >= '" + str(since_t) + "' AND t <= '" + str(until_t) + "' AND (" + null_filter + ") ORDER BY t DESC LIMIT " + str(count)
        cur.execute(query)
        r = cur.fetchall()
        cur.close()

        return self.__format_data_for_return__(r, [fmap_inv[x] for x in field_ids])

    def get_data_by_id(self, key, id_):
        cur = self.client.cursor()
        fmap = self.__field_map__(key, cur)
        key = self.__format_key__(key)
        fmap_inv = {fmap[x]: x for x in fmap}
        if len(fmap) == 0: return {}

        fields = ",".join(fmap.keys())
        query = "SELECT " + fields + " FROM " + key + " WHERE id_='" + id_ + "' LIMIT 1"
        cur.execute(query)
        r = cur.fetchall()
        cur.close()
        return self.__format_data_for_return__(r, [fmap_inv[x] for x in fields.split(",")])

    def delete_data(self, key, since, until):
        cur = self.client.cursor()
        key = self.__format_key__(key)
        since_t = since.strftime('%Y-%m-%d %H:%M:%S')
        until_t = until.strftime('%Y-%m-%d %H:%M:%S')
        query = "DELETE FROM " + key + " WHERE t >= '" + str(since_t) + "' AND t < '" + str(until_t) + "'"
        #print(">>", query)
        cur.execute(query)

        # Get number of deleted rows
        if self.dbs == "sqlite":
            cur.execute("SELECT changes()")
            q = cur.fetchall()[0][0]
        elif self.dbs == "mariadb":
            cur.execute("SELECT ROW_COUNT()")
            q = cur.fetchall()[0][0]
        elif self.dbs == "postgres":
            q = cur.rowcount

        self.client.commit()
        cur.close()
        return q

    def delete_data_by_id(self, key, id_):
        cur = self.client.cursor()
        key = self.__format_key__(key)
        query = "DELETE FROM " + key + " WHERE id_='" + str(id_) + "'"
        cur.execute(query)

        # Get number of deleted rows
        if self.dbs == "sqlite":
            cur.execute("SELECT changes()")
            q = cur.fetchall()[0][0]
        elif self.dbs == "mariadb":
            cur.execute("SELECT ROW_COUNT()")
            q = cur.fetchall()[0][0]
        elif self.dbs == "postgres":
            q = cur.rowcount

        self.client.commit()
        cur.close()
        return q

    # ==========================================================================
    # Get keys and fields
    # ==========================================================================

    def get_keys(self):
        cur = self.client.cursor()
        query = "SELECT DISTINCT(key_name) FROM metadata"
        cur.execute(query)
        r = cur.fetchall()
        cur.close()
        return [x[0] for x in r]

    def get_fields(self, key):
        cur = self.client.cursor()
        query = "SELECT DISTINCT(field), key_name FROM metadata WHERE key_name='" + key + "'"
        cur.execute(query)
        r = cur.fetchall()
        cur.close()
        return [x[0] for x in r]

    def remove_key(self, key):
        cur = self.client.cursor()
        fmap = self.__field_map__(key, cur)
        if len(fmap) == 0: raise Exception("Unkown key")

        query = 'DROP TABLE IF EXISTS ' + self.__format_key__(key)
        print(query)
        cur.execute(query)
        query = "DELETE FROM metadata WHERE key_name='" + key + "'"
        print(query)
        cur.execute(query)
        self.client.commit()
        cur.close()
        return True

    def remove_field(self, key, field):
        cur = self.client.cursor()
        fmap = self.__field_map__(key, cur)
        if len(fmap) == 0: raise Exception("Unkown key")
        if field not in fmap: raise Exception("Unkown field")

        query = "DELETE FROM metadata WHERE key_name='" + key + "' AND field='" + field + "'"
        cur.execute(query)
        query = "UPDATE " +  self.__format_key__(key) + " SET " + fmap[field] + "=NULL"
        cur.execute(query)
        self.client.commit()
        cur.close()
        return

    def rename_key(self, key, new_key):
        cur = self.client.cursor()
        query = "ALTER TABLE " + self.__format_key__(key) + " RENAME TO " + self.__format_key__(new_key)
        cur.execute(query)
        query = "UPDATE metadata SET key_name='" + new_key + "' WHERE key_name='" + key + "'"
        cur.execute(query)
        self.client.commit()
        cur.close()
        return

    def rename_field(self, key, field, new_field):
        cur = self.client.cursor()

        # check if field not exists
        query = "SELECT count(*) FROM metadata WHERE field='" + new_field + "' AND key_name='" + key + "' LIMIT 1"
        cur.execute(query)
        r = cur.fetchall()
        if r[0][0] > 0: raise Exception("Field '" + new_field + "' already exists")

        query = "UPDATE metadata SET field='" + new_field + "' WHERE field='" + field + "' AND key_name='" + key + "'"
        cur.execute(query)
        self.client.commit()
        cur.close()
        return

    # ==========================================================================
    # Metadata
    # ==========================================================================

    def get_metadata(self, key):
        cur = self.client.cursor()
        query = "SELECT field, alias, description, key_name FROM metadata WHERE key_name='" + key + "'"
        cur.execute(query)
        r = cur.fetchall()
        cur.close()
        return [{"field": x[0], "alias": x[1], "description": x[2]} for x in r]

    def set_metadata(self, key, field, alias, description):
        cur = self.client.cursor()
        query = "UPDATE metadata SET alias='" + alias + "', description='" + description + "' WHERE field='" + field + "' AND key_name='" + key + "'"
        cur.execute(query)
        self.client.commit()
        cur.close()
        return

    # ==========================================================================
    # Other
    # ==========================================================================

    def run_sql_query(self, query, return_fields=[]):
        cur = self.client.cursor()
        cur.execute(query)
        r = cur.fetchall()

        if len(return_fields) > 0:
            return self.__format_data_for_return__(r, fields)
            cur.close()
        return r

    def count_all_records(self):
        # Get keys
        cur = self.client.cursor()
        query = 'SELECT DISTINCT(key_name) FROM metadata'
        cur.execute(query)
        r = cur.fetchall()
        keys = [x[0] for x in r]

        all = {}
        total = 0
        for key in keys:
            query = 'SELECT COUNT(id) FROM ' + key
            cur.execute(query)
            r = cur.fetchall()
            count = r[0][0]
            total += count
            all[key] = count

        all["__total__"] = total
        cur.close()
        return all

    # ==========================================================================
    # Aux
    # ==========================================================================

    def __check_value__(self, v):
        if isinstance(v, int):
            if v > 2147483647: return 2147483647
            if v < -2147483647: return -2147483647
        if isinstance(v, float):
            if math.isinf(v) or math.isnan(v): return None
        return v

    def __format_key__(self, key):
        return key.replace("/", ".").replace(".", "_dot_")

    def __format_data_for_return__(self, data, fields):
        m = []
        for row in data:
            r = {}
            for i in range(0, len(fields)):
                if fields[i] == "t" and isinstance(row[i], str):
                    r[fields[i]] = datetime.datetime.strptime(row[i], "%Y-%m-%d %H:%M:%S")
                elif fields[i] == "t_" and isinstance(row[i], str):
                    r[fields[i]] = datetime.datetime.strptime(row[i], "%Y-%m-%d %H:%M:%S")
                elif row[i] is None:
                    continue
                else:
                    r[fields[i]] = row[i]

            m.append(r)
        return m

    def __field_map__(self, key, cur):
        sql = "SELECT field, field_id FROM metadata WHERE key_name='" + key + "'"
        cur.execute(sql)
        r = cur.fetchall()
        return {x[0]: x[1] for x in r}

    def __sql_bool__(self, v):
        if self.dbs == "postgres": return "TRUE" if v else "FALSE"
        return "1" if v else "0"
