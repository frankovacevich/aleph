
from aleph.connections.namespace.mqtt_namespace_connection import MqttNamespaceConnection
from aleph.connections.testing.random import RandomConnection
from aleph.services.mqtt import MqttService


class GenericService(MqttService):
    read_request_keys = ["generic"]
    connection = RandomConnection()
    namespace_connection = MqttNamespaceConnection()

    def __init__(self):
        super().__init__()

    def on_status_change(self, status_code):
        print("STATUS CHANGED", status_code)

    def on_read_request(self, key, **kwargs):
        print("NEW READ REQUEST", key, kwargs)
        response = super().on_read_request(key, **kwargs)
        print("RESPONSE", response)
        return response


if __name__ == "__main__":
    GenericService().run()
