from ..common.exceptions import *


class Service:

    def __init__(self, service_id):
        self.service_id = service_id
        self.namespace_connection = None
        self.connection = None

        self.read_request_keys = []
        self.namespace_subs_keys = []
        self.connection_subs_keys = []

    # ===================================================================================
    # Read request callback (don't edit)
    # ===================================================================================
    def __on_new_read_request__(self, topic, message):
        key = topic

        try:
            # Get key and arguments
            key = self.namespace_connection.topic_to_key(topic)
            args = self.namespace_connection.mqtt_message_to_data(message)
            args = self.namespace_connection.__clean_read_args__(key, **args)
            if "response_code" not in args: return

            # Generate response
            response_topic = self.namespace_connection.key_to_topic(key, args["response_code"])
            response_data = self.on_read_request(key, **args)
            response_message = self.namespace_connection.data_to_mqtt_message(response_data)

            # Publish response
            self.namespace_connection.mqtt_conn.publish(response_topic, response_message)

        except Exception as e:
            self.on_read_request_error(key, Error(e))

    # ===================================================================================
    # Main events (override me)
    # ===================================================================================
    def setup(self):
        return

    def on_read_request(self, key, **kwargs):
        return self.connection.safe_read(key, **kwargs)

    def on_new_data_from_namespace(self, key, data):
        self.connection.safe_write(key, data)

    def on_new_data_from_connection(self, key, data):
        self.namespace_connection.safe_write(key, data)

    # ===================================================================================
    # Error handling (override me)
    # ===================================================================================
    def on_connection_read_error(self, key, error):
        return

    def on_connection_write_error(self, key, error):
        return

    def on_namespace_read_error(self, key, error):
        return

    def on_namespace_write_error(self, key, error):
        return

    def on_read_request_error(self, key, error):
        return

    # ===================================================================================
    # Use me
    # ===================================================================================
    def run(self):
        self.namespace_connection.__on_new_read_request__ = self.__on_new_read_request__
        self.namespace_connection.on_new_data = self.on_new_data_from_namespace
        self.connection.on_new_data = self.on_new_data_from_connection

        self.namespace_connection.open()
        self.connection.open()

        for key in self.read_request_keys: self.namespace_connection.__subscribe_to_read_requests__(key)
        for key in self.namespace_subs_keys: self.namespace_connection.subscribe_async(key)
        for key in self.connection_subs_keys: self.connection.subscribe_async(key)

        self.namespace_connection.on_read_error = self.on_namespace_read_error
        self.namespace_connection.on_write_error = self.on_namespace_write_error
        self.connection.on_read_error = self.on_connection_read_error
        self.connection.on_write_error = self.on_connection_write_error

        self.setup()

        while True:
            pass
