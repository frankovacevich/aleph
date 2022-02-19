from ..interfaces import PostgresInterfaceConnection
from .sql_generic import SQLGenericDB


class Postgres(PostgresInterfaceConnection):

    def __init__(self):
        super().__init__()
        self.generic_engine = SQLGenericDB("postgres", self.client)

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
