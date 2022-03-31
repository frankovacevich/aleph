import math
from ....common.database_field_parse import *


class SQLGenericDB:

    def __init__(self, dbs):
        self.dbs = dbs
        self.client = None
        self.first_op = True

        if dbs not in ["sqlite", "mariadb", "mysql", "postgres"]:
            raise Exception("Invalid SQL engine: " + str(dbs))

    def create_metadata_table(self):
        # Create metadata table
        cur = self.client.cursor()

        if self.dbs == "sqlite":
            sql = 'CREATE TABLE IF NOT EXISTS metadata (id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
                  'key_name VARCHAR(255), field VARCHAR(255), field_id VARCHAR(255), alias VARCHAR(255), ' \
                  'description VARCHAR(1000) DEFAULT "", created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'
        elif self.dbs == "mariadb" or self.dbs == "mysql":
            sql = 'CREATE TABLE IF NOT EXISTS metadata (id INT NOT NULL AUTO_INCREMENT, ' \
                  'key_name VARCHAR(255), field VARCHAR(255), field_id VARCHAR(255), alias VARCHAR(255), ' \
                  'description VARCHAR(1000) DEFAULT "", created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, ' \
                  'PRIMARY KEY (`id`))'
        elif self.dbs == "postgres":
            sql = 'CREATE TABLE IF NOT EXISTS metadata (id BIGSERIAL PRIMARY KEY, key_name VARCHAR(255), ' \
                  'field VARCHAR(255), field_id VARCHAR(255), alias VARCHAR(255), ' \
                  'description VARCHAR(1000) DEFAULT \'\', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'

        cur.execute(sql)
        sql = 'CREATE INDEX IF NOT EXISTS idx_key ON metadata (key_name)'
        cur.execute(sql)
        self.client.commit()
        cur.close()
        self.first_op = False

    def read(self, key, **kwargs):
        if self.first_op: self.create_metadata_table()

        # Parse args and key
        args = kwargs
        key = db_parse_key(key)

        # Fields
        fields = args.pop("fields", "*")
        if fields != "*": fields = ", ".join(map(db_parse_field, fields))

        # We collect all where clauses in a list
        where_clauses = []

        # Time filter
        since = args.pop("since", None)
        until = args.pop("until", None)
        if since is None and until is None: time_filter = ""
        elif since is not None and until is None: time_filter = "t >= '" + since.strftime('%Y-%m-%d %H:%M:%S') + "'"
        elif until is not None and since is None: time_filter = "t <= '" + until.strftime('%Y-%m-%d %H:%M:%S') + "'"
        else: time_filter = "t >= '" + since.strftime('%Y-%m-%d %H:%M:%S') + "' AND t <= '" + until.strftime('%Y-%m-%d %H:%M:%S') + "'"
        if time_filter != "": where_clauses.append(time_filter)

        # Filter
        args_filter = args.pop("filter", None)
        if args_filter is not None: where_clauses.append(args_filter.to_sql_where_clause(db_parse_field))
        where_clauses.append("deleted_ IS NOT TRUE")

        # Collect all where clauses
        where_clause = " WHERE " + " AND ".join(where_clauses)

        # Limit and offset
        limit_and_offset = ""
        limit = args.pop("limit", 0)
        offset = args.pop("offset", 0)
        if limit != 0: limit_and_offset += " LIMIT " + str(limit)
        if offset != 0: limit_and_offset += " OFFSET " + str(offset)

        # Order: influx only supports sorting by time
        order = args.pop("order", None)
        if order is None: sorting_clause = ""
        elif order[0] == "-": sorting_clause = " ORDER BY " + order[1:] + " DESC"
        else: sorting_clause = " ORDER BY " + order + " ASC"

        # Run query
        query = "SELECT " + fields + " FROM " + key + where_clause + sorting_clause + limit_and_offset
        cur = self.client.cursor()
        cur.execute(query)

        # Get columns and data
        columns = [db_deparse_field(column[0]) for column in cur.description]
        data = cur.fetchall()
        result = []
        for record in data:
            dict_record = dict(zip(columns, record))
            if "t" in dict_record: dict_record["t"] = dict_record["t"].replace(" ", "T") + "Z"
            if "t_" in dict_record: dict_record["t_"] = dict_record["t_"].replace(" ", "T") + "Z"
            r = {f: dict_record[f] for f in dict_record if f not in ["id", "deleted_"] and dict_record[f] is not None}
            if len(r) == 0: continue
            result.append(r)

        return result

    def write(self, key, data):
        if self.first_op: self.create_metadata_table()

        org_key = key
        key = db_parse_key(key)
        cur = self.client.cursor()

        # Create table if not exists
        if self.dbs == "sqlite":
            sql = 'CREATE TABLE IF NOT EXISTS ' + key + ' (id INTEGER PRIMARY KEY AUTOINCREMENT, t DATETIME NOT NULL, t_ DATETIME, deleted_ BOOL DEFAULT FALSE)'
        elif self.dbs == "mariadb" or self.dbs == "mysql":
            sql = 'CREATE TABLE IF NOT EXISTS ' + key + ' (id INT NOT NULL AUTO_INCREMENT, t DATETIME NOT NULL, t_ DATETIME, deleted_ BOOL DEFAULT FALSE, PRIMARY KEY (`id`))'
        elif self.dbs == "postgres":
            sql = 'CREATE TABLE IF NOT EXISTS ' + key + ' (id BIGSERIAL PRIMARY KEY, t TIMESTAMP NOT NULL, t_ TIMESTAMP, deleted_ BOOL DEFAULT FALSE)'
        cur.execute(sql)

        # Insert each record
        for record in data: self.write_one(cur, org_key, record)

        self.client.commit()
        cur.close()

    def write_one(self, cur, key, record):
        record["t"] = record["t"].replace("T", " ").replace("Z", "")
        record["t_"] = record["t"]
        org_key = key
        key = db_parse_key(key)

        # Get field map
        sql = "SELECT field, field_id FROM metadata WHERE key_name='" + org_key + "'"
        cur.execute(sql)
        r = cur.fetchall()
        fmap = {x[0]: x[1] for x in r}

        # Check if id_ already exists
        already_exists = False
        if "id_" in record and "id_" in fmap:
            record["id_"] = str(record["id_"])
            sql = "SELECT COUNT(id_) FROM " + key + " WHERE id_='" + record["id_"] + "' LIMIT 1"
            cur.execute(sql)
            r = cur.fetchall()
            already_exists = r[0][0] > 0
            if already_exists: record.pop("t", None)

        # Insert data
        query_update_table = ''
        query_insert = ''
        query_values = ''

        for f in record:
            v = db_check_value(record[f])
            if v is None: continue

            if f in fmap:
                field_id = fmap[f]
            else:
                # Update metadata table
                field_id = db_parse_field(f)
                field_name = f
                while field_id in fmap.values(): field_id += "_"
                if field_id[0].isnumeric(): field_id = "i" + field_id
                sql = "INSERT INTO metadata(key_name, field_id, field, alias) VALUES ('" + org_key + "', '" + field_id + "', '" + field_name + "', '" + field_name + "')"
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
                elif isinstance(v, bool): query_values += "" + field_id + "=" + self.__sql_bool__(v) + ","
                elif isinstance(v, int): query_values += "" + field_id + "=" + str(v) + ","
                elif isinstance(v, float): query_values += "" + field_id + "=" + str(v) + ","

            if f in fmap or f in ["t", "id", "t_", "deleted_"]: continue
            if isinstance(v, str): query_update_table += 'ALTER TABLE ' + key + ' ADD ' + field_id + ' VARCHAR(255);'
            elif isinstance(v, bool): query_update_table += 'ALTER TABLE ' + key + ' ADD ' + field_id + ' BOOL;'
            elif isinstance(v, int): query_update_table += 'ALTER TABLE ' + key + ' ADD ' + field_id + ' BIG INT;'
            elif isinstance(v, float): query_update_table += 'ALTER TABLE ' + key + ' ADD ' + field_id + ' DOUBLE PRECISION;'

        # Execute update table query
        if query_update_table != '':
            for q in query_update_table.split(";"):
                if q == "": continue
                if self.dbs == "postgres": q = q.replace(" ADD ", " ADD COLUMN IF NOT EXISTS ")
                cur.execute(q)

        # Execute insert query
        if already_exists:
            query_insert = "UPDATE " + key + " SET " + query_values[0:-1] + " WHERE id_='" + record["id_"] + "'"
            cur.execute(query_insert)
        else:
            query_insert = 'INSERT INTO ' + key + ' (' + query_insert[0:-1] + ') VALUES (' + query_values[0:-1] + ')'
            cur.execute(query_insert)
            if "id_" in record:
                index_name = "idx_" + key
                if len(index_name) > 64: index_name = index_name[0:64]
                sql = 'CREATE INDEX IF NOT EXISTS ' + index_name + ' ON ' + key + ' (id_)'
                cur.execute(sql)

    def run_query(self, query):
        # Run query
        cur = self.client.cursor()
        cur.execute(query)

        # Get columns and data
        columns = [db_deparse_field(column[0]) for column in cur.description]
        data = cur.fetchall()
        result = []
        for record in data:
            dict_record = dict(zip(columns, record))
            result.append({f: dict_record[f] for f in dict_record})

        return result

    def __sql_bool__(self, v):
        if self.dbs == "postgres": return "TRUE" if v else "FALSE"
        return "1" if v else "0"
