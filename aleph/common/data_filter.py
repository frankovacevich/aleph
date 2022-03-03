"""
The DataFilter contains a collection of expressions that determine if
a record is valid or not. It's used to query when reading a connection,
to return only records that match the filter.

To create a filter, use:
data_filter = DataFilter.load(my_filter)

This will create a DataFilter object with a valid filter from my_filter
The my_filter parameter is a dict that contains the valid conditions
for each field. For example:
>> my_filter = {"x": 1, "y": 2}
>> data_filter = DataFilter.load(my_filter)

This will create a filter that validates records with "x" equal to 1
AND "y" equal to 2. You can use a list to specify multiple values:
>> my_filter = {"x": 1, "y": [2, 3]}
>> data_filter = DataFilter.load(my_filter)

This will create a filter that validates records with "x" equal to 1
AND "y" equal to 2 OR "y" equal to 3.

The values of each filter can also contain operators. For example:
>> my_filter = {"x": ">1"}
>> data_filter = DataFilter.load(my_filter)

This will create a filter that validates records with "x" greater than
1. Notice than the filter value is a string. The following operators
are available:

For numeric (int or float) values (here X and Y are numeric strings):
- "x": "<X": value of "x" less than X
- "x": "<=X": value of "x" less than or equal to X
- "x": ">X": value of "x" greater than X
- "x": ">=X": value of "x" greater than or equal X
- "x": "[X;Y]": value of "x" between X and Y (including X and Y)
- "x": "(X;Y)": value of "x" between X and Y (not including X and Y)
- "x": "(X;Y]": value of "x" between X and Y (including Y but not X)
- "x": "[X;Y)": value of "x" between X and Y (including X but not Y)

For strings:
- "x": "!=Some string": value of "x" not equal to "Some string"
- "x": "=@Some string": value of "x" contains "Some string"
- "x": "!@Some string": value of "x" does not contain "Some string"
- "x": "=^Some string": value of "x" starts with "Some string"
- "x": "=$Some string": value of "x" ends with "Some string"

See https://developer.matomo.org/api-reference/reporting-api-segmentation

"""

import json


class DataFilter:

    def __init__(self):
        self.filter = None         # None or dict
        self.parsed_filter = None  # None or dict

    def to_sql_where_clause(self, field_map={}):
        """
        Returns a string that serves as SQL WHERE clause
        """
        if self.filter is None: return ""

        and_clauses = []
        for field in self.parsed_filter:
            x = field
            if field in field_map: x = field_map[field]

            or_clauses = []
            for v in self.parsed_filter[field]:
                if isinstance(v, str): v = v.replace(" ", "")

                try:
                    # Numeric
                    if isinstance(v, bool): clause = x + " IS " + ("" if v else "NOT ") + "TRUE"
                    elif isinstance(v, int) or isinstance(v, float): clause = x + " = " + str(v)
                    elif v.startswith("=="): clause = x + " = " + v[2:]
                    elif v.startswith("<="): clause = x + " <= " + v[2:]
                    elif v.startswith(">="): clause = x + " >= " + v[2:]
                    elif v.startswith("<"): clause = x + " < " + v[1:]
                    elif v.startswith(">"): clause = x + " > " + v[1:]

                    # String
                    elif v.startswith("!="): clause = x + " <> '" + v[2:] + "'"
                    elif v.startswith("=@"): clause = "CONTAINS(" + x + ", '" + v[2:] + "')"
                    elif v.startswith("!@"): clause = "NOT CONTAINS(" + x + ", '" + v[2:] + "')"
                    elif v.startswith("=^"): clause = x + " LIKE '%" + v[2:] + "'"
                    elif v.startswith("=$"): clause = x + " LIKE '" + v[2:] + "%'"

                    # Between numbers
                    elif ";" in v and (v.startswith("(") or v.startswith("[")) and (v.endswith(")") or v.endswith("]")):
                        b = v[1:-1].split(";")
                        if v[0] == "[" and v[-1] == "]":  clause = "(" + x + " >= " + b[0] + " AND " + x + " <= " + b[1] + ")"
                        elif v[0] == "(" and v[-1] == ")":  clause = "(" + x + " > " + b[0] + " AND " + x + " < " + b[1] + ")"
                        elif v[0] == "(" and v[-1] == "]":  clause = "(" + x + " > " + b[0] + " AND " + x + " <= " + b[1] + ")"
                        elif v[0] == "[" and v[-1] == ")":  clause = "(" + x + " >= " + b[0] + " AND " + x + " < " + b[1] + ")"
                        else: clause = x + " = '" + v + "'"

                    # String again
                    else:
                        clause = x + " = '" + v + "'"

                    or_clauses.append(clause)

                except:
                    continue

            and_clauses.append("(" + " OR ".join(or_clauses) + ")")

        return " AND ".join(and_clauses)

    def to_mongodb_filter(self, field_map={}):
        """
        Returns a dict that serves as a mongodb filter
        """
        if self.filter is None: return {}

        and_clauses = []
        for field in self.parsed_filter:
            x = field
            if field in field_map: x = field_map[field]

            or_clauses = []
            for v in self.parsed_filter[field]:
                if isinstance(v, str): v = v.replace(" ", "")
                clause = {}
                try:
                    if not isinstance(v, str): clause[x] = v

                    # Strings
                    elif v.startswith("!="): clause[x] = {"$ne": v[2:]}
                    elif v.startswith("=@"): clause[x] = "/" + v[2:] + "/"
                    elif v.startswith("!@"): clause[x] = {"$not": "/" + v[2:] + "/"}
                    elif v.startswith("=^"): clause[x] = "/^" + v[2:] + "/"
                    elif v.startswith("=$"): clause[x] = "/" + v[2:] + "$/"

                    # Numeric
                    elif v.startswith("=="): clause[x] = float(v[2:])
                    elif v.startswith("<="): clause[x] = {"$lte": float(v[2:])}
                    elif v.startswith(">="): clause[x] = {"$gte": float(v[2:])}
                    elif v.startswith("<"): clause[x] = {"$lt": float(v[2:])}
                    elif v.startswith(">"): clause[x] = {"$gt": float(v[2:])}

                    elif ";" in v and (v.startswith("(") or v.startswith("[")) and (v.endswith(")") or v.endswith("]")):
                        b = v[1:-1].split(";")

                        if v[0] == "[" and v[-1] == "]": clause[x] = {"$gte": float(b[0]), "$lte": float(b[1])}
                        elif v[0] == "(" and v[-1] == ")": clause[x] = {"$gt": float(b[0]), "$lt": float(b[1])}
                        elif v[0] == "(" and v[-1] == "]": clause[x] = {"$gt": float(b[0]), "$lte": float(b[1])}
                        elif v[0] == "[" and v[-1] == ")": clause[x] = {"$gt": float(b[0]), "$lt": float(b[1])}
                        else: clause[x] = v

                    or_clauses.append(clause)

                except:
                    raise

            and_clauses.append({"$or": or_clauses})

        return {"$and": and_clauses}

    def apply_to_record(self, record):
        """
        Returns True or False if the record complies with the filter or not
        """
        if self.filter is None: return True

        for field in self.filter:
            if field not in record: return False
            if True not in [condition(record[field]) for condition in self.filter[field]]: return False

        return True

    @staticmethod
    def load(parsed_filter):
        df = DataFilter()
        df.parsed_filter = parsed_filter

        # TODO: Deparse filter (from JSON to some sort of function)
        if parsed_filter is None: return None
        if isinstance(parsed_filter, str): parsed_filter = json.loads(parsed_filter)

        df.filter = {}
        for field in parsed_filter:
            df.filter[field] = []

            if not isinstance(parsed_filter[field], list):
                parsed_filter[field] = [parsed_filter[field]]
            filters = parsed_filter[field]

            for v in filters:
                if isinstance(v, str): v = v.replace(" ", "")

                try:
                    if not isinstance(v, str): fun = lambda x, f=v: x == f

                    # Strings
                    elif v.startswith("!="): fun = lambda x, f=v: x != f[2:]
                    elif v.startswith("=@"): fun = lambda x, f=v: f[2:] in x
                    elif v.startswith("!@"): fun = lambda x, f=v: f[2:] not in x
                    elif v.startswith("=^"): fun = lambda x, f=v: x.startswith(f[2:])
                    elif v.startswith("=$"): fun = lambda x, f=v: x.endswith(f[2:])

                    # Numeric
                    elif v.startswith("=="): fun = lambda x, f=v: x == float(f[2:])
                    elif v.startswith("<="): fun = lambda x, f=v: x <= float(f[2:])
                    elif v.startswith(">="): fun = lambda x, f=v: x >= float(f[2:])
                    elif v.startswith("<"): fun = lambda x, f=v: x < float(f[1:])
                    elif v.startswith(">"): fun = lambda x, f=v: x > float(f[1:])

                    # Between numbers
                    elif ";" in v and (v.startswith("(") or v.startswith("[")) and (v.endswith(")") or v.endswith("]")):
                        between = tuple(v[1:-1].split(";"))

                        if v[0] == "[" and v[-1] == "]": fun = lambda x, b=between: float(b[0]) <= x <= float(b[1])
                        elif v[0] == "(" and v[-1] == ")": fun = lambda x, b=between: float(b[0]) < x < float(b[1])
                        elif v[0] == "(" and v[-1] == "]": fun = lambda x, b=between: float(b[0]) < x <= float(b[1])
                        elif v[0] == "[" and v[-1] == ")": fun = lambda x, b=between: float(b[0]) <= x < float(b[1])
                        else: fun = lambda x, f=v: x == f

                    # Other
                    else:
                        fun = lambda x, f=v: x == f
                    df.filter[field].append(fun)

                except:
                    continue

        return df
