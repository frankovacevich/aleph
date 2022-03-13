from ...connection import Connection
from .sql_generic import SQLGenericDB
import sqlite3


class SqliteConnection(Connection, SQLGenericDB):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.file = "./main.db"

        # Private
        self.check_filters_on_read = False
        self.check_timestamp_on_read = False

        self.client = None
        self.dbs = "sqlite"

    def open(self):
        self.client = sqlite3.connect(self.file)
        super().open()

    def close(self):
        self.client.close()
        super().close()
