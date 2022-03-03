from ..interfaces import MySQLInterfaceConnection


class MySQLTimeSeries(MySQLInterfaceConnection):

    def read(self, key, **kwargs):
        # Parse args and key
        args = self.__clean_read_args__(key, **kwargs)
        self.skip_read_cleaning = True
        key = db_parse_key(key)

    def write(self, key, data):
        pass