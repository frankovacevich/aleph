from ...connection import Connection
import mysql.connector


class MySQLInterfaceConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.server = "localhost"
        self.username = ""
        self.password = ""
        self.port = 3306
        self.database = "main"

        # Private
        self.client = None

    def open(self):
        self.client = mysql.connector.connect(host=self.server,
                                              port=self.port,
                                              database=self.database,
                                              user=self.username,
                                              password=self.password)

    def close(self):
        self.client.close()
