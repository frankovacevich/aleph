"""
- connect()
- save_to_database(key, data)
- get_from_database(key, since)
- get_all_keys()
- get_fields(key)

"""
import sqlite3
import datetime
from dateutil.tz import tzutc


class SqliteConnection:

    def __init__(self, file="my.db"):
        self.database = file
        self.log = None

        ##
        self.client = None

        return

    def connect(self):
        self.client = sqlite3.connect(self.database)

    def close(self):
        self.client.close()

    def __int_limits__(self, v):
        if v > 2147483647: return 2147483647
        if v < -2147483647: return -2147483647
        return v

    def __float_limits__(self, v):
        return v

    def save_to_database(self, key, data):
        columns = self.get_fields(key)

        self.connect()
        dat = data.copy()

        # 1. Create table if not exists
        query = 'CREATE TABLE IF NOT EXISTS `' + key + '` (id INTEGER PRIMARY KEY AUTOINCREMENT, t DATETIME NOT NULL)'
        cur = self.client.cursor()
        cur.execute(query)
        self.client.commit()
        cur.close()

        # 2. Make time a timestamp and id_ a string
        dat["t"] = dat["t"].replace("T", " ").replace("Z", "")
        if "id_" in dat: dat["id_"] = str(dat["id_"])

        # 3. Check if already exists
        already_exists = False
        if "id_" in columns and "id_" in dat:
            query = 'SELECT COUNT(*) FROM `' + key + '` WHERE id_="' + dat["id_"] + '" LIMIT 1'
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
                    query_insert += "`" + field + "`,"

                if isinstance(v, str): query_values += '"' + str(v) + '",'
                elif isinstance(v, bool): query_values += ("1" if v else "0") + ','
                elif isinstance(v, int): query_values += str(self.__int_limits__(v)) + ','
                elif isinstance(v, float): query_values += str(self.__float_limits__(v)) + ','

            else:
                # if isinstance(v, str) or isinstance(v, bool) or isinstance(v, int) or isinstance(v, float):
                #     query_insert += "`" + field + "`,"

                if isinstance(v, str): query_values += '`' + field + '`="' + str(v) + '",'
                elif isinstance(v, bool): query_values += "`" + field + "`=" + ("1" if v else "0") + ','
                elif isinstance(v, int): query_values += '`' + field + '`=' + str(self.__int_limits__(v)) + ','
                elif isinstance(v, float): query_values += '`' + field + '`=' + str(self.__float_limits__(v)) + ','

            if field in columns or field == "t": continue
            if isinstance(v, str): query_update_table += 'ALTER TABLE `' + key + '` ADD `' + field + '` VARCHAR(255);'
            elif isinstance(v, bool): query_update_table += 'ALTER TABLE `' + key + '` ADD `' + field + '` BOOL;'
            elif isinstance(v, int): query_update_table += 'ALTER TABLE `' + key + '` ADD `' + field + '` INT;'
            elif isinstance(v, float): query_update_table += 'ALTER TABLE `' + key + '` ADD `' + field + '` FLOAT(32);'

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
            query_insert = 'UPDATE `' + key + '` SET ' + query_values[0:-1] + ' WHERE id_="' + data["id_"] + '"'
        else:
            query_insert = 'INSERT INTO `' + key + '` (' + query_insert[0:-1] + ') VALUES (' + query_values[0:-1] + ')'
        cur = self.client.cursor()
        cur.execute(query_insert)
        self.client.commit()
        cur.close()
        self.close()
        return

    def __format_data_for_return__(self, key, data, field="*"):

        if field != "*":
            if "id_" in data:
                return [{field: d[0], "t": datetime.datetime.strptime(d[1], "%Y-%m-%d %H:%M:%S"), "id_": d[2]} for d in data]
            else:
                return [{field: d[0], "t": datetime.datetime.strptime(d[1], "%Y-%m-%d %H:%M:%S")} for d in data]

        result = []
        allfields = self.get_fields(key)
        for row in data:
            n_result = {}
            for i in range(0, len(row)):
                f = allfields[i]
                r = row[i]
                if f == "t": r = datetime.datetime.strptime(row[i], "%Y-%m-%d %H:%M:%S")
                if f == "id": continue
                n_result[f] = r
                continue
            result.append(n_result)

        return result

    def get_from_database(self, key, field, since, until, count, ffilter):
        #since_t = (datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=since)).strftime('%Y-%m-%dT%H:%M:%SZ')
        #until_t = (datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=until)).strftime('%Y-%m-%dT%H:%M:%SZ')
        since_t = since.strftime('%Y-%m-%d %H:%M:%S')
        until_t = until.strftime('%Y-%m-%d %H:%M:%S')

        if field == "*":
            query = "SELECT * FROM `" + key + "` WHERE t >= '" + str(since_t) + "' AND t <= '" + str(until_t) + "' ORDER BY t DESC LIMIT " + str(count)
        else:
            aux = ",t "
            if "id_" in self.get_fields(): aux = ",t , id_ "
            query = "SELECT " + field + aux + " FROM `" + key + "` WHERE t >= '" + str(since_t) + "' AND t <= '" + str(until_t) + "' AND " + field + " IS NOT NULL ORDER BY t DESC LIMIT " + str(count)

        self.connect()
        cur = self.client.cursor()
        cur.execute(query)
        r = cur.fetchall()
        cur.close()
        self.close()
        return self.__format_data_for_return__(key, r, field)

    def get_from_database_by_id(self, key, id_):
        self.connect()
        query = "SELECT * FROM `" + key + "` WHERE id_ = '" + id_ + "' LIMIT 1"
        cur = self.client.cursor()
        cur.execute(query)
        r = cur.fetchall()
        cur.close()
        self.close()
        return self.__format_data_for_return__(key, r)

    def delete_records(self, key, since, until):
        self.connect()
        since_t = (datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=since)).strftime('%Y-%m-%d %H:%M:%S')
        until_t = (datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=until)).strftime('%Y-%m-%d %H:%M:%S')
        query = "DELETE FROM `" + key + "` WHERE t >= '" + str(since_t) + "' AND t < '" + str(until_t) + "'"
        cur = self.client.cursor()
        cur.execute(query)
        cur.execute('SELECT changes()')
        q = int(cur.fetchall()[0][0])
        self.client.commit()
        cur.close()
        self.close()
        return q

    def delete_record_by_id(self, key, id_):
        self.connect()
        query = "DELETE FROM `" + key + "` WHERE id_ = \"" + str(id_) + "\""
        cur = self.client.cursor()
        cur.execute(query)
        cur.execute('SELECT changes()')
        q = int(cur.fetchall()[0][0])
        self.client.commit()
        cur.close()
        self.close()
        return q

    def get_all_keys(self):
        self.connect()
        query = "SELECT * FROM sqlite_master where type='table';"
        cur = self.client.cursor()
        cur.execute(query)
        r = cur.fetchall()
        cur.close()
        self.close()
        return [x[1] for x in r if x[1] != 'sqlite_sequence']

    def get_fields(self, key):
        self.connect()
        query = 'PRAGMA table_info(`' + key + '`);'
        cur = self.client.cursor()
        cur.execute(query)
        r = cur.fetchall()
        cur.close()
        self.close()
        return [x[1] for x in r if x != "id"]

    def remove_key(self, key):
        self.connect()
        query = "DROP TABLE IF EXISTS `" + key + "`;"
        cur = self.client.cursor()
        cur.execute(query)
        self.client.commit()
        cur.close()
        self.close()
        return

    def remove_field(self, key, field):
        self.connect()
        cur = self.client.cursor()
        self.__drop_column__(cur, key, field)
        self.client.commit()
        cur.close()
        self.close()
        return

    def __drop_column__(self, db, table, column):
        import re
        import random

        columns = [c[1] for c in db.execute("PRAGMA table_info(%s)" % table)]
        columns = [c for c in columns if c != column]
        sql = db.execute("SELECT sql from sqlite_master where name = '%s'"
                         % table).fetchone()[0]
        sql = self.__sql__format__(sql)
        lines = sql.splitlines()
        findcol = r'\b%s\b' % column
        keeplines = [line for line in lines if not re.search(findcol, line)]
        create = '\n'.join(keeplines)
        create = re.sub(r',(\s*\))', r'\1', create)
        temp = 'tmp%d' % random.randint(1e8, 1e9)
        db.execute("ALTER TABLE %(old)s RENAME TO %(new)s" % {
            'old': table, 'new': temp})
        db.execute(create)
        db.execute("""
            INSERT INTO %(new)s ( %(columns)s )
            SELECT %(columns)s FROM %(old)s
        """ % {
            'old': temp,
            'new': table,
            'columns': ', '.join(columns)
        })
        db.execute("DROP TABLE %s" % temp)

    def __sql__format__(self, sql):
        sql = sql.replace(",", ",\n")
        sql = sql.replace("(", "(\n")
        sql = sql.replace(")", "\n)")
        return sql

    def rename_key(self, key, new_key):
        self.connect()
        query = "ALTER TABLE `" + key + "` RENAME TO `" + new_key + "`;"
        cur = self.client.cursor()
        cur.execute(query)
        self.client.commit()
        cur.close()
        self.close()
        return

    def rename_field(self, key, field, new_field):
        self.connect()
        query = "ALTER TABLE `" + key + "` RENAME COLUMN " + field + " TO " + new_field + ";"
        cur = self.client.cursor()
        cur.execute(query)
        self.client.commit()
        cur.close()
        self.close()
        return

    def count_all_records(self):
        self.connect()
        keys = self.get_all_keys()

        all = {}
        total = 0
        for key in keys:
            if "t" not in self.get_fields(key): continue
            query = 'SELECT COUNT(t) FROM `' + key + '`'
            cur = self.client.cursor()
            cur.execute(query)
            r = cur.fetchall()
            cur.close()
            count = r[0][0]

            total += count
            all[key] = count

        all["__total__"] = total
        self.close()
        return all

    def run_sql_query(self, query, return_as_dict=True):
        self.connect()
        cur = self.client.cursor()
        cur.execute(query)
        r = cur.fetchall()
        cur.close()
        self.close()

        key = ""
        query = query.lower()
        if "from" in query:
            key = query.split("from")[1].split(" ")[0].replace("'", "").replace('"', '').replace("`", "")

        if return_as_dict and key != "":
            return self.__format_data_for_return__(key, r)
        else:
            return r
