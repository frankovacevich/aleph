from ...connection import Connection
from .sql_generic import SQLGenericDB
import mysql.connector


class MySQLConnection(Connection, SQLGenericDB):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.server = "localhost"
        self.username = ""
        self.password = ""
        self.port = 3306
        self.database = "main"

        # Private
        self.check_filters_on_read = False
        self.check_timestamp_on_read = False

        self.client = None
        self.dbs = "mysql"

    def open(self):
        self.client = mysql.connector.connect(host=self.server,
                                              port=self.port,
                                              database=self.database,
                                              user=self.username,
                                              password=self.password)

    def close(self):
        self.client.close()
