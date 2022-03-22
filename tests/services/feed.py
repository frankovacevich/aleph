"""
This is a simple Service that generates and publishes random data
Use it to test that:
- The service changes status correctly when connecting/disconnecting
- Store and forward works correctly
"""


from aleph.connections.namespace.mqtt_namespace_connection import MqttNamespaceConnection
from aleph.connections.testing.random import RandomConnection
from aleph.services.mqtt import MqttService


class FeedService(MqttService):
    connection = RandomConnection()
    namespace_connection = MqttNamespaceConnection()
    connection_subs_keys = ["generic"]

    def __init__(self):
        super().__init__()

    def on_status_change(self, status_code):
        print("STATUS CHANGED", status_code)

    def on_new_data_from_connection(self, key, data):
        super().on_new_data_from_connection(key, data)


if __name__ == "__main__":
    FeedService().run()
