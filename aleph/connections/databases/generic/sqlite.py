from ...connection import Connection
from .sql_generic import SQLGenericDB
import sqlite3


class SqliteConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.file = "./main.db"

        # Private
        self.clean_on_read = False
        self.sql_generic = SQLGenericDB("sqlite")

    def open(self):
        self.sql_generic.client = sqlite3.connect(self.file)
        self.sql_generic.client.close()

    def close(self):
        return

    def read(self, key, **kwargs):
        self.sql_generic.client = sqlite3.connect(self.file)
        data = self.sql_generic.read(key, **kwargs)
        self.sql_generic.client.close()
        return data

    def write(self, key, data):
        self.sql_generic.client = sqlite3.connect(self.file)
        self.sql_generic.write(key, data)
        self.sql_generic.client.close()
