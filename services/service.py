from ..connections.namespace_connection import NamespaceConnection
from ..connections.connection import Connection
from ..common.exceptions import *


class Service:

    def __init__(self):
        self.namespace_connection = NamespaceConnection()
        self.conn = Connection()

        self.read_request_keys = []
        self.namespace_subs_keys = []
        self.main_conn_subs_keys = []

    # ===================================================================================
    # Read request callback (don't edit)
    # ===================================================================================
    def __on_new_read_request__(self, topic, message):
        key = "unknown"

        try:
            # Get key and arguments
            key = self.namespace_connection.topic_to_key(topic)
            args = self.namespace_connection.mqtt_message_to_data(message)

            # Generate response
            response_topic = self.namespace_connection.key_to_topic(key, args["response_code"])
            response_data = self.on_read_request(key, **args)
            response_message = self.namespace_connection.data_to_mqtt_message(response_data)

            # Publish response
            self.namespace_connection.mqtt_conn.publish(response_topic, response_message)

        except Exception as e:
            self.on_read_request_error(key, get_error_and_traceback_message(e))

    # ===================================================================================
    # Override me
    # ===================================================================================
    def on_read_request(self, key, **kwargs):
        return self.conn.safe_read(key, **kwargs)

    def on_read_request_error(self, key, error_message):
        return

    def on_new_data_from_namespace(self, key, data):
        pass

    def on_new_data_from_connection(self, key, data):
        pass

    def setup(self):
        return

    # ===================================================================================
    # Use me
    # ===================================================================================
    def run(self):
        self.setup()

        for key in self.read_request_keys:
            self.namespace_connection.mqtt_conn.subscribe_topics.append(self.namespace_connection.key_to_topic(key, "r"))

        self.namespace_connection.__on_new_read_request__ = self.__on_new_read_request__

        while True:
            pass
