from ...connection import Connection
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
        self.check_filters_on_read = False
        self.check_timestamp_on_read = False

        self.client = None
        self.dbs = "postgres"

    def open(self):
        self.client = psycopg2.connect(database=self.database,
                                       user=self.username,
                                       password=self.password,
                                       host=self.server,
                                       port=self.port
                                       )
        super().open()

    def close(self):
        self.client.close()
        super().close()
