from .sql_generic import SqlGenericConnection
import sqlite3


class SqliteConnection(SqlGenericConnection):

    def __init__(self):
        super().__init__()
        self.file = "default.db"
        self.client = None

    # ===================================================================================
    # Open
    # ===================================================================================
    def open(self):
        self.__client_open__()
        super().open()

    def close(self):
        self.__client_close__()
        super().close()

    def __client_open__(self):
        self.client = sqlite3.connect(self.file)

    def __client_close__(self):
        self.client.close()

    # ===================================================================================
    # Read
    # ===================================================================================
    def read(self, key, **kwargs):
        self.__client_open__()
        r = super().read(key, **kwargs)
        self.__client_close__()
        return r

    # ===================================================================================
    # Write
    # ===================================================================================
    def write(self, key, data):
        self.__client_open__()
        r = super().write(key, data)
        self.__client_close__()
        return r
