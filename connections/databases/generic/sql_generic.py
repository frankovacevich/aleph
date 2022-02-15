"""
This is not a connection! Only a helper
"""

from ....common.database_field_parse import *
from ....common.data_filter import DataFilter


class SQLGenericDB:

    def __init__(self, dbs, client):
        self.dbs = dbs
        self.client = client

    def open(self):
        # Create metadata table
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

    def read(self, key, **kwargs):
        # Parse args and key
        args = kwargs
        key = db_parse_key(key)

        # Time filter
        since = args["since"].strftime('%Y-%m-%dT%H:%M:%SZ')
        until = args["until"].strftime('%Y-%m-%dT%H:%M:%SZ')
        time_filter = "t >= '" + since + "' AND t <= '" + until + "'"

        # Filter
        where_clause = " WHERE " + time_filter
        data_filter = ""
        if args["filter"] is not None: data_filter = args["filter"].to_sql_where_clause()
        if data_filter != "": where_clause = " AND (" + data_filter + ")"
        where_clause += " AND deleted_ IS NOT TRUE"

        # Fields
        fields = args["fields"]
        if fields != "*": fields = ",".join(fields)

        # Limit and offset
        limit_and_offset = ""
        if args["limit"] != 0: limit_and_offset = " LIMIT " + str(args["limit"])
        if args["offset"] != 0: limit_and_offset = " OFFSET " + str(args["offset"])

        # Order: influx only supports sorting by time
        if args["order"][0] == "-": sorting_clause = " ORDER BY " + args["order"][1:] + " DESC"
        else: sorting_clause = " ORDER BY " + args["order"] + " ASC"

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
            result.append({f: dict_record[f] for f in dict_record if f not in ["id", "deleted_"]})

        return result

    def write(self, key, data):
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
        record["t_"] = record["t"]
        org_key = key
        key = db_parse_key(key)

        # Get field map
        sql = "SELECT field, field_id FROM metadata WHERE key_name='" + key + "'"
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

            if f in fmap or f in ["t", "id", "t_", "delete_"]: continue
            if isinstance(v, str): query_update_table += 'ALTER TABLE ' + key + ' ADD ' + field_id + ' VARCHAR(255);'
            elif isinstance(v, bool): query_update_table += 'ALTER TABLE ' + key + ' ADD ' + field_id + ' BOOL;'
            elif isinstance(v, int): query_update_table += 'ALTER TABLE ' + key + ' ADD ' + field_id + ' INT;'
            elif isinstance(v, float): query_update_table += 'ALTER TABLE ' + key + ' ADD ' + field_id + ' FLOAT(32);'

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

    def __sql_bool__(self, v):
        if self.dbs == "postgres": return "TRUE" if v else "FALSE"
        return "1" if v else "0"
