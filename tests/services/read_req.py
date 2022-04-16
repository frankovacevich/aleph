
from aleph.connections.namespace.mqtt_namespace_connection import MqttNamespaceConnection
from aleph.connections.testing.random import RandomConnection
from aleph.services.mqtt import MqttService
import time


class GenericService(MqttService):
    read_request_keys = ["generic"]
    namespace_subs_keys = ["ns"]
    connection_subs_keys = ["cs"]

    connection = RandomConnection()
    namespace_connection = MqttNamespaceConnection()
    namespace_connection.multithread = True

    def __init__(self):
        super().__init__()
        self.last_data = []

    def on_status_change(self, status_code):
        print("% STATUS CHANGED", status_code)

    def on_new_data_from_namespace(self, key, data):
        print("& NEW DATA FROM NAMESPACE", key, data)

    def on_new_data_from_connection(self, key, data):
        print("# NEW DATA FROM CONNECTION", key, data)
        self.last_data = data

    def on_read_request(self, key, **kwargs):
        print("$ NEW READ REQUEST", key, kwargs)
        time.sleep(5)
        print("$ RESPONSE", self.last_data)
        return self.last_data


if __name__ == "__main__":
    GenericService().run()
