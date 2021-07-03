import math
import datetime


class SQLConnectionHelper:

    def __init__(self):
        self.client = None
        return

    def __check_value__(self, v):
        if isinstance(v, int):
            if v > 2147483647: return 2147483647
            if v < -2147483647: return -2147483647
            return v
        if isinstance(v, float):
            if math.isinf(y) or math.isnan(y):
                return None
            return v
        return v

    def __format_key__(self, key):
        return key.replace("/", ".").replace(".", "_")

    def __format_data_for_return__(self, data, fields):
        m = []
        for row in data:
            r = {}
            for i in range(0, len(fields)):
                if fields[i] == "t" or fields[i] == "t_":
                    r[fields[i]] = datetime.datetime.strptime(row[i], "%Y-%m-%d %H:%M:%S")
                else:
                    r[fields[i]] = row[i]

            m.append(r)
        return m

    def __get_name_field_map__(self, key, cur):
        sql = 'SELECT field_name, field_ FROM sqlfields WHERE key_name="' + key + '"'
        cur.execute(sql)
        r = cur.fetchall()
        return {x[0]: x[1] for x in r}

    def connect(self):
        cur = self.client.cursor()
        sql = 'CREATE TABLE IF NOT EXISTS "sqlfields" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "key_" VARCHAR(255), "field_" VARCHAR(255), "field_name" VARCHAR(255), "key_name" VARCHAR(255), "alias" VARCHAR(255), "description" VARCHAR(1000) DEFAULT "", created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'
        cur.execute(sql)
        sql = 'CREATE INDEX IF NOT EXISTS idx_key ON sqlfields (key_name)'
        cur.execute(sql)
        self.client.commit()
        cur.close()

    def save_to_database(self, key, data):
        cur = self.client.cursor()
        key = self.__format_key__(key)
        dat = data.copy()

        # Create table if not exists
        sql = 'CREATE TABLE IF NOT EXISTS `' + key + '` (id INTEGER PRIMARY KEY AUTOINCREMENT, t DATETIME NOT NULL)'
        cur.execute(sql)

        # Get name: field map
        fmap = self.__get_name_field_map__(key, cur)

        # Check if id_ already exists
        already_exists = False
        if "id_" in data and "id_" in fmap:
            sql = 'SELECT COUNT(id_) FROM `' + key + '` WHERE id_="' + data["id_"] + '" LIMIT 1'
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

            field = f
            if f in fmap: field = fmap[f]

            # Update sqlfields table
            sql = 'INSERT INTO sqlfields(key_, field_, key_name, field_name, alias) SELECT "' + key + '", "' + field +  '", "' + key + '", "' + field +  '", "' + field +  '" WHERE NOT EXISTS (SELECT * FROM sqlfields WHERE key_="' + key + '" AND field_name="' + field + '")'
            cur.execute(sql)

            if not already_exists:

                if isinstance(v, str) or isinstance(v, bool) or isinstance(v, int) or isinstance(v, float):
                    query_insert += "`" + field + "`,"

                if isinstance(v, str): query_values += '"' + str(v) + '",'
                elif isinstance(v, bool): query_values += ("1" if v else "0") + ','
                elif isinstance(v, int): query_values += str(v) + ','
                elif isinstance(v, float): query_values += str(v) + ','

            else:

                if isinstance(v, str): query_values += '`' + field + '`="' + str(v) + '",'
                elif isinstance(v, bool): query_values += "`" + field + "`=" + ("1" if v else "0") + ','
                elif isinstance(v, int): query_values += '`' + field + '`=' + str(v) + ','
                elif isinstance(v, float): query_values += '`' + field + '`=' + str(v) + ','

            if f in fmap or f == "t": continue
            if isinstance(v, str): query_update_table += 'ALTER TABLE `' + key + '` ADD `' + field + '` VARCHAR(255);'
            elif isinstance(v, bool): query_update_table += 'ALTER TABLE `' + key + '` ADD `' + field + '` BOOL;'
            elif isinstance(v, int): query_update_table += 'ALTER TABLE `' + key + '` ADD `' + field + '` INT;'
            elif isinstance(v, float): query_update_table += 'ALTER TABLE `' + key + '` ADD `' + field + '` FLOAT(32);'

        # Update columns
        if query_update_table != '':
            for q in query_update_table.split(";"):
                if q == "": continue
                cur.execute(q)

        #
        if already_exists:
            query_insert = 'UPDATE `' + key + '` SET ' + query_values[0:-1] + ' WHERE id_="' + data["id_"] + '"'
            cur.execute(query_insert)
        else:
            query_insert = 'INSERT INTO `' + key + '` (' + query_insert[0:-1] + ') VALUES (' + query_values[0:-1] + ')'
            cur.execute(query_insert)
            if "id_" in dat:
                sql = 'CREATE INDEX IF NOT EXISTS `idx_' + key + '` ON `' + key + '` (id_)'
                cur.execute(sql)

        #
        self.client.commit()
        cur.close()

    def get_from_database(self, key, field, since, until, count):
        cur = self.client.cursor()
        key = self.__format_key__(key)
        fmap = self.__get_name_field_map__(key, cur)
        fmap_inv = {fmap[x]: x for x in fmap}

        if len(fmap) == 0: raise Exception("Key " + key + " does not exist")
        if field != "*" and field not in fmap: raise Exception("Field " + field + " not found")

        since_t = since.strftime('%Y-%m-%d %H:%M:%S')
        until_t = until.strftime('%Y-%m-%d %H:%M:%S')

        if field == "*":
            fields = ",".join(fmap.keys())
            query = "SELECT " + fields + " FROM `" + key + "` WHERE t >= '" + str(since_t) + "' AND t <= '" + str(until_t) + "' ORDER BY t DESC LIMIT " + str(count)
            cur.execute(query)
            r = cur.fetchall()

        else:
            aux = ",t "
            if "id_" in fmap: aux = ",t,id_"
            fields = field + aux
            query = "SELECT " + fields + " FROM `" + key + "` WHERE t >= '" + str(since_t) + "' AND t <= '" + str(until_t) + "' AND " + field + " IS NOT NULL ORDER BY t DESC LIMIT " + str(count)
            cur.execute(query)
            r = cur.fetchall()

        cur.close()
        return self.__format_data_for_return__(r, [fmap_inv[x] for x in fields.split(",")])

    def get_from_database_by_id(self, key, id_):
        cur = self.client.cursor()
        key = self.__format_key__(key)
        fmap = self.__get_name_field_map__(key, cur)
        fmap_inv = {fmap[x]: x for x in fmap}

        fields = ",".join(fmap.keys())
        sql = "SELECT " + fields + " WHERE id_=" + id_ + "LIMIT 1"
        cur.execute(query)
        r = cur.fetchall()

        cur.close()
        return self.__format_data_for_return__(r, [fmap_inv[x] for x in fields.split(",")])

    def delete_records(self, key, since, until):
        cur = self.client.cursor()
        key = self.__format_key__(key)
        since_t = (datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=since)).strftime('%Y-%m-%d %H:%M:%S')
        until_t = (datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=until)).strftime('%Y-%m-%d %H:%M:%S')
        query = "DELETE FROM `" + key + "` WHERE t >= '" + str(since_t) + "' AND t < '" + str(until_t) + "'"
        cur.execute(query)
        self.client.commit()
        return cur

    def delete_record_by_id(self, key, id_):
        cur = self.client.cursor()
        key = self.__format_key__(key)
        query = 'DELETE FROM `' + key + '` WHERE id_ = "' + str(id_) + '"'
        cur.execute(query)
        self.client.commit()
        return cur

    def get_all_keys(self):
        cur = self.client.cursor()
        query = 'SELECT DISTINCT(key_name) FROM sqlfields'
        cur.execute(query)
        r = cur.fetchall()
        return [x[0] for x in r]

    def get_fields(self):
        pass

    def remove_key(self):
        pass

    def remove_field(self):
        pass

    def rename_key(self):
        pass

    def rename_field(self):
        pass

    def count_all_records(self):
        pass

    def get_metadata(self, key):
        pass

    def set_metadata(self, key, field, alias, description):
        pass
