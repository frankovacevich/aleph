"""

"""


class DataFilter:

    def __init__(self):
        self.filter = None
        self.parsed_filter = None

    def parse_filter(self):
        if self.filter is None: return None

    def to_sql_where_clause(self):
        if self.filter is None: return ""

    def to_mongodb_filter(self):
        if self.filter is None: return {}

    def apply_to(self, record):
        if self.filter is None: return record

    @staticmethod
    def load(parsed_filter):
        df = DataFilter()
        df.parsed_filter = parsed_filter

        # TODO: Deparse filter (from JSON to some sort of function)
        if parsed_filter is None: df.filter = None

        return df
