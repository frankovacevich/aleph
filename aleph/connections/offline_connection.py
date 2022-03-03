from .connection import Connection
from .namespace.mqtt_namespace_connection import MqttNamespaceConnection
from ..common.exceptions import *


class OfflineConnection(Connection):
    """

    """

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.namespace_connection = MqttNamespaceConnection(client_id)
        self.local_connections = {}  # {key: Connection} dict of local connections

    def open(self):
        self.namespace_connection.open()
        self.namespace_connection.on_new_data = self.on_new_data_from_namespace
        for key in self.local_connections: self.local_connections[key].open()

    def close(self):
        self.namespace_connection.close()
        for key in self.local_connections: self.local_connections[key].close()

    def read(self, key, **kwargs):
        if key in self.local_connections:
            return self.local_connections[key].read(key, **kwargs)
        else:
            return self.namespace_connection.read(key, **kwargs)

    def write(self, key, data):
        if key in self.local_connections:
            self.local_connections[key].write(key, data)
        self.namespace_connection[key].write(key, data)

    def force_sync(self):
        """
        Retrieves last records from the namespace and inserts them in the local connections
        """

        if not self.namespace_connection.connected(): self.namespace_connection.open()

        for key in self.local_connections:
            last_record = self.local_connections[key].safe_read(key, fields="t", order="-t", limit=1)
            if len(last_record) == 0: last_t = None
            else: last_t = last_record[0]["t"]

            data = self.namespace_connection.safe_read(key, since=last_t, order="-t", limit=10000)
            if len(data) > 0: self.local_connections[key].safe_write(key, data)

        return

    def on_new_data_from_namespace(self, key, data):
        """
        When new data arrives from the namespace, update local connections
        """
        if key in self.local_connections:
            try:
                self.local_connections[key].write(key, data)
            except:
                self.on_sync_error(Error(Exceptions.SyncError, client_id=self.client_id, key=key, data=data))

        self.on_new_data(key, data)

    def on_sync_error(self, error):
        """
        Callback for when a new message from the namespace arrives but writing the data
        to the local connection fails.
        """
        pass


