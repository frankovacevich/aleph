import math
import datetime


class SQLConnection:

    def __init__(self, dbs="sqlite"):
        if dbs not in ["sqlite", "mariadb", "mysql", "postgres"]:
            raise Exception("Invalid database system")
        self.dbs = dbs
        self.client = None

        self.buffer_size = 5000
        self.buffer_count = 0
        self.buffer = {}
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

    def save_data(self, key, data):
        if key == "metadata": raise Exception("Invalid key")

        key_org = key
        key = self.__format_key__(key)
        dat = data.copy()

        # Make time a timestamp and id_ a string
        dat["t"] = dat["t"].replace("T", " ").replace("Z", "")

        # Create table if not exists
        if self.dbs == "sqlite":
            sql = 'CREATE TABLE IF NOT EXISTS ' + key + ' (id INTEGER PRIMARY KEY AUTOINCREMENT, t DATETIME NOT NULL, field VARCHAR(255) NOT NULL, str_value VARCHAR(255) DEFAULT NULL, bool_value VARCHAR(255) DEFAULT NULL, int_value VARCHAR(255) DEFAULT NULL, float_value VARCHAR(255) DEFAULT NULL)'
        elif self.dbs == "mariadb":
            sql = 'CREATE TABLE IF NOT EXISTS ' + key + ' (id INT NOT NULL AUTO_INCREMENT, t DATETIME NOT NULL, field VARCHAR(255) NOT NULL, str_value VARCHAR(255) DEFAULT NULL, bool_value VARCHAR(255) DEFAULT NULL, int_value VARCHAR(255) DEFAULT NULL, float_value VARCHAR(255) DEFAULT NULL, PRIMARY KEY (`id`))'
        #cur.execute(sql)
        #if sql not in self.buffer[0]: self.buffer[0].append(sql)
        if key not in self.buffer: self.buffer[key] = ["", "", "", "", ""]
        self.buffer[key][0] = sql

        # Insert value
        for f in dat:
            if f == "t": continue
            v = self.__check_value__(dat[f])
            if v is None: continue

            #if isinstance(v, str): sql = 'INSERT INTO ' + key + ' (t, field, str_value) VALUES ("' + dat["t"] + '","' + f + '","' + v + '")'
            #elif isinstance(v, bool): sql = 'INSERT INTO ' + key + ' (t, field, bool_value) VALUES ("' + dat["t"] + '","' + f + '",' + self.__sql_bool__(v) + ')'
            #elif isinstance(v, int): sql = 'INSERT INTO ' + key + ' (t, field, int_value) VALUES ("' + dat["t"] + '","' + f + '",' + str(v) + ')'
            #elif isinstance(v, float): sql = 'INSERT INTO ' + key + ' (t, field, float_value) VALUES ("' + dat["t"] + '","' + f + '",' + str(v) + ')'

            if isinstance(v, str):
                sql = '("' + dat["t"] + '","' + f + '","' + v + '"),'
                self.buffer[key][1] += sql
            elif isinstance(v, bool):
                sql = '("' + dat["t"] + '","' + f + '",' + self.__sql_bool__(v) + '),'
                self.buffer[key][2] += sql
            elif isinstance(v, int):
                sql = '("' + dat["t"] + '","' + f + '",' + str(v) + '),'
                self.buffer[key][3] += sql
            elif isinstance(v, float):
                sql = '("' + dat["t"] + '","' + f + '",' + str(v) + '),'
                self.buffer[key][4] += sql

            self.buffer_count += 1
            if self.buffer_count >= self.buffer_size :
                self.flush_buffer()
            #cur.execute(sql)

        return

    def flush_buffer(self):
        cur = self.client.cursor()

        for key in self.buffer:
            if self.buffer[key][0] != "": cur.execute(self.buffer[key][0])
            if self.buffer[key][1] != "": cur.execute("INSERT INTO " + key + " (t, field, str_value) VALUES " + self.buffer[key][1][0:-1])
            if self.buffer[key][2] != "": cur.execute("INSERT INTO " + key + " (t, field, bool_value) VALUES " + self.buffer[key][2][0:-1])
            if self.buffer[key][3] != "": cur.execute("INSERT INTO " + key + " (t, field, int_value) VALUES " + self.buffer[key][3][0:-1])
            if self.buffer[key][4] != "": cur.execute("INSERT INTO " + key + " (t, field, float_value) VALUES " + self.buffer[key][4][0:-1])

            self.buffer[key][0] = ""
            self.buffer[key][1] = ""
            self.buffer[key][2] = ""
            self.buffer[key][3] = ""
            self.buffer[key][4] = ""

        self.buffer_count = 0
        self.client.commit()
        cur.close()
        print("FLUSHED")

    def get_data(self, key, field, since, until, count):
        key = self.__format_key__(key)
        cur = self.client.cursor()

        field_filter = ""
        if field != "*":
            if isinstance(field, str): field = [field]
            for f in field:
                field_filter += 'field == "' + f + '" OR '
            field_filter = '(' + field_filter[0:-4] + ') AND '

        # make time a string
        since_t = since.strftime('%Y-%m-%d %H:%M:%S')
        until_t = until.strftime('%Y-%m-%d %H:%M:%S')

        query = 'SELECT * FROM ' + key + ' WHERE ' + field_filter + 't >= "' + since_t + '" AND t < "' + until_t + '"'
        cur.execute(query)
        r = cur.fetchall()
        cur.close()

        return self.__format_data_for_return__(r, count)


    def get_data_by_id(self, key, id_):
        raise("Invalid method")

    def delete_data(self, key, since, until):
        pass

    def delete_data_by_id(self, key, id_):
        raise("Invalid method")

    # ==========================================================================
    # Get keys and fields
    # ==========================================================================

    def get_keys(self):
        pass

    def get_fields(self, key):
        pass

    def remove_key(self, key):
        pass

    def remove_field(self, key, field):
        pass

    def rename_key(self, key, new_key):
        pass

    def rename_field(self, key, field, new_field):
        pass

    # ==========================================================================
    # Metadata
    # ==========================================================================

    def get_metadata(self, key):
        pass

    def set_metadata(self, key, field, alias, description):
        pass

    # ==========================================================================
    # Other
    # ==========================================================================

    def run_sql_query(self, query, return_fields=[]):
        pass

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

    def __format_data_for_return__(self, data, count):
        dat = {}
        for item in data:
            t = item[1]
            f = item[2]
            if item[3] is not None: v = item[3]
            elif item[4] is not None: v = item[4]
            elif item[5] is not None: v = item[5]
            elif item[6] is not None: v = item[6]

            if t not in dat: dat[t] = {"t": t}
            dat[t][f] = v

            if len(dat) >= count: break

        r = [dat[t] for t in dat]
        r.reverse()
        return r


    def __sql_bool__(self, v):
        if self.dbs == "postgres": return "TRUE" if v else "FALSE"
        return "1" if v else "0"
