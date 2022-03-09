from ..interfaces import MySQLInterfaceConnection
from ._sql_generic import SQLGenericDB


class MySQLConnection(MySQLInterfaceConnection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.generic_engine = SQLGenericDB("mysql", self.client)

    # ===================================================================================
    # Open
    # ===================================================================================
    def open(self):
        super().open()
        self.generic_engine.client = self.client
        self.generic_engine.open()

    def close(self):
        self.client.close()
        super().close()

    def read(self, key, **kwargs):
        return self.generic_engine.read(key, **kwargs)

    def write(self, key, data):
        self.generic_engine.write(key, data)
