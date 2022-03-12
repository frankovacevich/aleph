from ...connection import Connection
import psycopg2


class PostgresInterfaceConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.server = "localhost"
        self.username = ""
        self.password = ""
        self.port = 5432
        self.database = "main"

        # Private
        self.client = None

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
