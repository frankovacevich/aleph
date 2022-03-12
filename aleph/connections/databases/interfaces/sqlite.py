from ...connection import Connection


class SqliteInterfaceConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.file = "./main.db"

        # Private
        self.client = None

    def open(self):
        import sqlite3
        self.client = sqlite3.connect(self.file)
        super().open()

    def close(self):
        self.client.close()
        super().close()