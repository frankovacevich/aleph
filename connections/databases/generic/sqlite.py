from ..interfaces import SqliteInterfaceConnection
from .sql_generic import SQLGenericDB
import sqlite3


class Sqlite(SqliteInterfaceConnection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.generic_engine = SQLGenericDB("sqlite", self.client)

    # ===================================================================================
    # Open
    # ===================================================================================
    def open(self):
        super().open()
        self.generic_engine.client = self.client
        self.generic_engine.open()

    def close(self):
        self.client.close()
        super().close()

    def read(self, key, **kwargs):
        self.client = sqlite3.connect(self.file)
        r = self.generic_engine.read(key, **kwargs)
        self.client.close()
        return r

    def write(self, key, data):
        self.client = sqlite3.connect(self.file)
        r = self.generic_engine.write(key, data)
        self.client.close()
        return r
