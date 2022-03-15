from ...connection import Connection
from .sql_generic import SQLGenericDB
import mysql.connector


class MySQLConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.server = "localhost"
        self.username = ""
        self.password = ""
        self.port = 3306
        self.database = "main"

        # Private
        self.clean_on_read = False
        self.sql_generic = SQLGenericDB("mysql")

    def open(self):
        self.sql_generic.client = mysql.connector.connect(host=self.server,
                                                          port=self.port,
                                                          database=self.database,
                                                          user=self.username,
                                                          password=self.password)

    def close(self):
        self.sql_generic.client.close()

    def read(self, key, **kwargs):
        return self.sql_generic.read(key, **kwargs)

    def write(self, key, data):
        self.sql_generic.write(key, data)

