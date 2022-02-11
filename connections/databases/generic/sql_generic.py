from ...connection import Connection
import math


class SqlGenericConnection(Connection):
    """

    """

    def __init__(self):
        super().__init__()
        self.dbs = "sqlite"  # sqlite, mariadb, mysql, postgres
        self.client = None

    # ==========================================================================
    # Open
    # ==========================================================================
    def open(self):
        cur = self.client.cursor()

        if self.dbs == "sqlite":
            sql = 'CREATE TABLE IF NOT EXISTS metadata (id INTEGER PRIMARY KEY AUTOINCREMENT, key_name VARCHAR(255), field VARCHAR(255), pfield VARCHAR(255), alias VARCHAR(255), description VARCHAR(1000) DEFAULT "", created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'
        elif self.dbs == "mariadb" or self.dbs == "mysql":
            sql = 'CREATE TABLE IF NOT EXISTS metadata (id INT NOT NULL AUTO_INCREMENT, key_name VARCHAR(255), field VARCHAR(255), pfield VARCHAR(255), alias VARCHAR(255), description VARCHAR(1000) DEFAULT "", created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`))'
        elif self.dbs == "postgres":
            sql = 'CREATE TABLE IF NOT EXISTS metadata (id BIGSERIAL PRIMARY KEY, key_name VARCHAR(255), field VARCHAR(255), pfield VARCHAR(255), alias VARCHAR(255), description VARCHAR(1000) DEFAULT \'\', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'

        cur.execute(sql)
        sql = 'CREATE INDEX IF NOT EXISTS idx_key ON metadata (key_name)'
        cur.execute(sql)
        self.client.commit()
        cur.close()

        super().open()

    # ==========================================================================
    # Read
    # ==========================================================================
    def read(self, key, **kwargs):
        pkey = self.__parse_key__(key)
        args = kwargs

        # Queries
        q = ""
        w = []  # collect where clauses

        # Fields
        fields = "*"
        if "fields" in args and args["fields"] != "*":
            fields = ", ".join([self.__parse_field__(f) for f in args["fields"]])

        # Since
        if "since" in args and args["since"] is not None:
            w.append("t >= '" + args["since"].strftime('%Y-%m-%d %H:%M:%S') + "'")

        # Until
        if "until" in args and args["until"] is not None:
            w.append("t <= '" + args["until"].strftime('%Y-%m-%d %H:%M:%S') + "'")

        # Deleted
        w.append("deleted_ IS NOT TRUE")

        # Filter
        # if args["filter"] is not None:
        #     w.append(self.filter_to_where(args["filter"]))

        # Limit and offset
        q += "SELECT " + fields + " FROM " + pkey + " WHERE " + " AND ".join(w)
        q += " ORDER BY t ASC"
        if "limit" in args and args["limit"] is not None:
            q += " LIMIT " + str(args["limit"])
        if "offset" in args:
            q += " OFFSET " + str(args["offset"])

        # Run query
        cur = self.client.cursor()
        cur.execute(q)
        data = cur.fetchall()
        result = []

        cols = [x[0] for x in cur.description]
        fields = self.__fields__(key, cur)
        for c in cols:
            if c not in fields: fields[c] = c

        for row in data:
            r = dict(zip(cols, row))
            result.append({fields[c]: r[c] for c in cols})

        cur.close()
        return result

    def write(self, key, data):
        pkey = self.__parse_key__(key)

        # Queries
        cur = self.client.cursor()

        # Create table if not exists
        if self.dbs == "sqlite":
            sql = 'CREATE TABLE IF NOT EXISTS ' + pkey + ' (id INTEGER PRIMARY KEY AUTOINCREMENT, t DATETIME NOT NULL, t_ DATETIME, deleted_ BOOL DEFAULT FALSE)'
        elif self.dbs == "mariadb" or self.dbs == "mysql":
            sql = 'CREATE TABLE IF NOT EXISTS ' + pkey + ' (id INT NOT NULL AUTO_INCREMENT, t DATETIME NOT NULL, t_ DATETIME, deleted_ BOOL DEFAULT FALSE, PRIMARY KEY (`id`))'
        elif self.dbs == "postgres":
            sql = 'CREATE TABLE IF NOT EXISTS ' + pkey + ' (id BIGSERIAL PRIMARY KEY, t TIMESTAMP NOT NULL, t_ TIMESTAMP, deleted_ BOOL DEFAULT FALSE)'
        cur.execute(sql)

        # Write records one by one
        for d in data:
            self.__write_one__(cur, key, d)

        self.client.commit()
        cur.close()
        return

    def __write_one__(self, cur, key, record):
        pkey = self.__parse_key__(key)
        record["t"] = record["t"].replace("T", " ").replace("Z", "")
        record["t_"] = record["t"]

        # If id data: see if record exists
        already_exists = False
        if "id_" in record:
            sql = "SELECT COUNT(id_) FROM " + pkey + " WHERE id_='" + record["id_"] + "' LIMIT 1"
            cur.execute(sql)
            r = cur.fetchall()
            already_exists = r[0][0] > 0
            del record["t"]

        # Get fields
        existing_fields = self.__fields__(key, cur)

        # Create queries
        query_update_table = ''
        query_insert = ''
        query_values = ''

        for f in record:
            # Check value
            v = self.__check_value__(record[f])
            field = self.__parse_field__(f)

            # If record exists, create update query
            if already_exists:
                if isinstance(v, str): query_values += "" + field + "='" + str(v) + "',"
                if isinstance(v, bool): query_values += "" + field + "=" + self.__sql_bool__(self.dbs, v) + ","
                elif isinstance(v, int): query_values += "" + field + "=" + str(v) + ","
                elif isinstance(v, float): query_values += "" + field + "=" + str(v) + ","

            # If record doesn't exist, create insert query
            else:
                if isinstance(v, str) or isinstance(v, bool) or isinstance(v, int) or isinstance(v, float):
                    query_insert += "" + field + ","

                if isinstance(v, str): query_values += "'" + str(v) + "',"
                elif isinstance(v, bool): query_values += self.__sql_bool__(self.dbs, v) + ','
                elif isinstance(v, int): query_values += str(v) + ','
                elif isinstance(v, float): query_values += str(v) + ','

            # Create query to add field as new column
            if field not in existing_fields:
                sql = "INSERT INTO metadata(key_name, field, pfield, alias) VALUES ('" + key + "', '" + f + "', '" + field + "', '" + field + "')"
                cur.execute(sql)
                if field in ["t", "t_", "id", "deleted_"]: continue

                if isinstance(v, str): query_update_table += 'ALTER TABLE ' + pkey + ' ADD ' + field + ' VARCHAR(255);'
                elif isinstance(v, bool): query_update_table += 'ALTER TABLE ' + pkey + ' ADD ' + field + ' BOOL;'
                elif isinstance(v, int): query_update_table += 'ALTER TABLE ' + pkey + ' ADD ' + field + ' INT;'
                elif isinstance(v, float): query_update_table += 'ALTER TABLE ' + pkey + ' ADD ' + field + ' FLOAT(32);'

        # Run queries
        if query_update_table != '':
            queries = set(query_update_table.split(";"))
            for q in queries:
                if q == "": continue
                # if self.dbs == "postgres": q = q.replace(" ADD ", " ADD COLUMN IF NOT EXISTS ")
                cur.execute(q)

        if already_exists:
            query_insert = "UPDATE " + pkey + " SET " + query_values[0:-1] + " WHERE id_='" + record["id_"] + "'"
            cur.execute(query_insert)
        else:
            query_insert = 'INSERT INTO ' + pkey + ' (' + query_insert[0:-1] + ') VALUES (' + query_values[0:-1] + ')'
            cur.execute(query_insert)
            if "id_" in record:
                index_name = "idx_" + pkey
                if len(index_name) > 64: index_name = index_name[0:64]
                sql = 'CREATE INDEX IF NOT EXISTS ' + index_name + ' ON ' + pkey + ' (id_);'
                cur.execute(sql)

    # ===================================================================================
    # More useful functions
    # ===================================================================================
    def delete_data(self, key, **kwargs):
        pass

    def show_keys(self):
        pass

    def remove_key(self, key):
        pass

    def rename_key(self, key):
        pass

    def count_records(self, key):
        pass

    def run_query(self, query):
        pass

    def result_to_dict(self, result):
        return

    # ===================================================================================
    # Aux
    # ===================================================================================

    @staticmethod
    def __parse_key__(key):
        return key

    @staticmethod
    def __parse_field__(field):
        return field.replace(".", "_")

    @staticmethod
    def __filter_to_where__(filter):
        return ""

    @staticmethod
    def __check_value__(v):
        if isinstance(v, int):
            if v > 2147483647: return 2147483647
            if v < -2147483647: return -2147483647
        if isinstance(v, float):
            if math.isinf(v) or math.isnan(v): return None
        return v

    @staticmethod
    def __fields__(key, cur):
        sql = "SELECT pfield, field FROM metadata WHERE key_name='" + key + "'"
        cur.execute(sql)
        r = cur.fetchall()
        return {x[0]: x[1] for x in r}

    @staticmethod
    def __sql_bool__(dbs, v):
        if dbs == "postgres": return "TRUE" if v else "FALSE"
        return "1" if v else "0"