from aleph.connections.namespace.mqtt_namespace_connection import MqttNamespaceConnection
from aleph.connections.databases.generic.sqlite import Sqlite
from aleph.connections.connection import Connection


class MqttNamespaceConnectionWithLocalBackup(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)

        self.namespace_connection = MqttNamespaceConnection(client_id)
        self.local_backup = Sqlite(client_id)

    def open(self):
        pass

    def close(self):
        pass

    def read(self, key, **kwargs):
        pass

    def write(self, key, data):
        self.namespace_connection.write()

