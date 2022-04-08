from ...connection import Connection
from .sql_generic import SQLGenericDB
import psycopg2


class PostgresConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.server = "localhost"
        self.username = ""
        self.password = ""
        self.port = 5432
        self.database = "main"

        # Private
        self.clean_on_read = False
        self.sql_generic = SQLGenericDB("mysql")

    def open(self):
        self.sql_generic.client = psycopg2.connect(database=self.database,
                                                   user=self.username,
                                                   password=self.password,
                                                   host=self.server,
                                                   port=self.port
                                                   )

    def close(self):
        self.sql_generic.client.close()

    def read(self, key, **kwargs):
        return self.sql_generic.read(key, **kwargs)

    def write(self, key, data):
        self.sql_generic.write(key, data)

    def run_query(self, query):
        return self.sql_generic.run_query(query)
