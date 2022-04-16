"""

"""

from aleph.connections.namespace.mqtt_namespace_connection import MqttNamespaceConnection
from aleph.connections.databases.generic.sqlite import SqliteConnection
import time


# Sqlite connection
class SqliteDB(SqliteConnection):
    def on_read_error(self, error): error.raise_exception()
    def on_write_error(self, error): error.raise_exception()


# Namespace connection
class NamespaceConnection(MqttNamespaceConnection):
    def on_read_error(self, error): error.raise_exception()
    def on_write_error(self, error): error.raise_exception()


# Create connection instances
S = SqliteDB()
N1 = NamespaceConnection()
N2 = NamespaceConnection()

S.open()
N1.open()
N2.open()


# Handle new data
def on_new_data(key, data):
    S.safe_write(key, data)


N1.subscribe_async("test.#")
N1.on_new_data = on_new_data


i = 0
while True:
    i += 1

    time.sleep(4)
    N2.safe_write("test.1", {"x": i})
    N2.safe_write("test.2", {"y": i})

    time.sleep(1)
    print("test.1", len(S.safe_read("test.1", since=None, until=None)))
    print("test.2", len(S.safe_read("test.2", since=None, until=None)))
