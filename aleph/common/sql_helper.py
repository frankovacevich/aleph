
class SqlHelper:

    def __init__(self, database_type):
        self.database_type = database_type

    # =========================================================================
    # Field types
    # =========================================================================
    def text_field(self, x=255): return f"VARCHAR({x})"
    def integer_field(self): return "BIGINT"
    def real_field(self): return "DOUBLE_PRECISION"
    def date_field(self): return "DATE"
    def boolean_field(self): return "BOOL"
    def datetime_field(self): return "DATETIME" if self.database_type != "postgres" else "TIMESTAMP"

    # =========================================================================
    # Parse / deparse fields and table names and other helpers
    # =========================================================================
    @staticmethod
    def __parse_field__(field_name):
        return field_name.replace(".", "__")

    @staticmethod
    def __deparse_field__(field_name):
        return field_name.replace("__", ".")

    @staticmethod
    def __parse_many_fields__(*fields):
        fields_ = []
        for field in fields:
            aux = field.split(" ")
            field_ = SqlHelper.__parse_field__(aux[0])
            if len(aux) > 1: field_ += " " + "".join(aux[1:])
            fields_.append(field_)
        return fields_

    @staticmethod
    def __parse_table_name__(table_name):
        return table_name.replace("/", ".").replace(".", "_")

    @staticmethod
    def __deparse_table_name__(table_name):
        return table_name.replace("_", ".")

    @staticmethod
    def __check_value__(value):
        import math
        if isinstance(value, int):
            pass
        elif isinstance(value, float):
            if math.isinf(value) or math.isnan(value): return None

        return value

    def bool_to_sql(self, value):
        if self.database_type == "postgres": return "TRUE" if value else "FALSE"
        return "1" if value else "0"

    # =========================================================================
    # Table and index creation
    # =========================================================================
    def clause_create_table(self, table_name, *fields):
        fields = SqlHelper.__parse_many_fields__(*fields)

        if self.database_type == "sqlite":
            q_fields = "id INTEGER PRIMARY KEY AUTOINCREMENT, " + ", ".join(fields)
        elif self.database_type == "mariadb" or self.database_type == "mysql":
            fields.append("PRIMARY KEY (`id`)")
            q_fields = "id INT NOT NULL AUTO_INCREMENT, " + ", ".join(fields)
        elif self.database_type == "postgres":
            q_fields = "id BIGSERIAL PRIMARY KEY, " + ", ".join(fields)

        return f"CREATE TABLE IF NOT EXISTS {SqlHelper.__parse_table_name__(table_name)} ({q_fields})"

    def clause_create_index(self, index_name, table_name, *fields):
        fields = SqlHelper.__parse_many_fields__(*fields)
        return f"CREATE INDEX IF NOT EXISTS {index_name} ON {SqlHelper.__parse_table_name__(table_name)} ({''.join(fields)})"

    def clause_alter_table_add_column(self, table_name, column_name, column_type):
        return f"ALTER TABLE {SqlHelper.__parse_table_name__(table_name)} ADD {SqlHelper.__parse_field__(column_name)} {column_type}"

    # =========================================================================
    # Table querying
    # =========================================================================
    def clause_query_table(self, table_name, fields="*", where_clauses="", order_by_clause="", limit_offset_clause=""):
        if isinstance(fields, list): fields = SqlHelper.__parse_many_fields__(*fields)
        q = f"SELECT {fields} FROM {SqlHelper.__parse_table_name__(table_name)}"
        if where_clauses != "": q += f" WHERE {' AND '.join([w for w in where_clauses if w != ''])}"
        q += f" {order_by_clause} {limit_offset_clause}"
        return q

    def clause_query_count(self, table_name, where_clauses=""):
        q = f"SELECT COUNT(id) FROM {SqlHelper.__parse_table_name__(table_name)}"
        if where_clauses != "": q += f" WHERE {' AND '.join([w for w in where_clauses if w != ''])}"
        return q

    # =========================================================================
    # Table get columns
    # =========================================================================
    def clause_get_table_columns(self, table_name):
        return

    # =========================================================================
    # Table insert / update
    # =========================================================================
    def clause_query_insert(self, table_name, fields_and_values):
        fields = []
        values = []
        for k in fields_and_values:
            fields.append(SqlHelper.__parse_field__(k))

            value = fields_and_values[k]
            if isinstance(value, str): value = f'"{value}"'
            elif isinstance(value, bool): value = self.bool_to_sql(value)
            values.append(str(value))

        return f"INSERT INTO {SqlHelper.__parse_table_name__(table_name)}({', '.join(fields)}) VALUES ({', '.join(values)})"

    def clause_query_update(self, table_name, fields_and_values, where_clauses=""):
        values_to_set = []
        for k in fields_and_values:
            field = SqlHelper.__parse_field__(k)

            value = fields_and_values[k]
            if isinstance(value, str): value = f'"{value}"'
            elif isinstance(value, bool): value = self.bool_to_sql(value)

            values_to_set.append(f"{field} = {value}")

        q = f"UPDATE {SqlHelper.__parse_table_name__(table_name)} SET {', '.join(values_to_set)}"
        if where_clauses != "": q += f" WHERE {' AND '.join([w for w in where_clauses if w != ''])}"
        return q

    # =========================================================================
    # Order by, limit and offset
    # =========================================================================
    def clause_order_by(self, *fields):
        fields = SqlHelper.__parse_many_fields__(*fields)
        fields = [f[1:] + " DESC" if f.startswith("-") else f for f in fields]
        return f"ORDER BY {', '.join(fields)}"

    def clause_limit_offset(self, limit=0, offset=0):
        limit_and_offset = ""
        if limit != 0: limit_and_offset += f"LIMIT {limit}"
        if offset != 0: limit_and_offset += f" OFFSET {offset}"
        return limit_and_offset

    # =========================================================================
    # Where clauses
    # =========================================================================
    def clause_where_time_since_until(self, time_field, since=None, until=None):
        if since is None and until is None: time_filter = ""
        elif since is not None and until is None: time_filter = f"{time_field} >= '{since.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif until is not None and since is None: time_filter = f"{time_field} < '{until.strftime('%Y-%m-%d %H:%M:%S')}'"
        else: time_filter = f"{time_field} >= '{since.strftime('%Y-%m-%d %H:%M:%S')}' AND {time_field} < '{until.strftime('%Y-%m-%d %H:%M:%S')}'"
        return time_filter

    def clause_where_field_not_null(self, *fields):
        if fields == "*": return ""
        fields = SqlHelper.__parse_many_fields__(*fields)
        return "(" + " IS NOT NULL OR ".join(fields) + " IS NOT NULL" + ")"
