from aleph.connections.namespace.mqtt_namespace_connection import MqttNamespaceConnection
from aleph.connections.testing.random import RandomConnection
from aleph.services.mqtt import MqttService


class GenericService(MqttService):
    read_request_keys = ["generic"]
    connection = RandomConnection()
    namespace_connection = MqttNamespaceConnection()

    def __init__(self):
        super().__init__()


if __name__ == "__main__":
    GenericService().run()
