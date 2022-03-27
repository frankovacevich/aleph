from ...connection import Connection
from .sql_generic import SQLGenericDB
import sqlite3


class SqliteConnection(Connection):
    file = "./main.db"

    def __init__(self, client_id=""):
        super().__init__(client_id)

        # Private
        self.client = None
        self.clean_on_read = False
        self.sql_generic = SQLGenericDB("sqlite")

    def open(self):
        self.client = sqlite3.connect(self.file)
        self.sql_generic.client = self.client
        self.client.close()

    def close(self):
        return

    def is_connected(self):
        if self.client is None: return False
        return True

    def read(self, key, **kwargs):
        self.sql_generic.client = sqlite3.connect(self.file)
        data = self.sql_generic.read(key, **kwargs)
        self.sql_generic.client.close()
        return data

    def write(self, key, data):
        self.sql_generic.client = sqlite3.connect(self.file)
        self.sql_generic.write(key, data)
        self.sql_generic.client.close()

    def run_query(self, query):
        self.sql_generic.client = sqlite3.connect(self.file)
        result = self.sql_generic.run_query(query)
        self.sql_generic.client.close()
        return result
