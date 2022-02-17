import json
import random
import time
from .connection import Connection
from ..common.mqtt_connection import MqttConnection
from ..common.exceptions import *


class NamespaceConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__()
        self.client_id = client_id
        self.username = ""
        self.password = ""
        self.broker_address = "localhost"
        self.port = 1883
        self.tls_enabled = True

        self.read_timeout = 10
        self.store_and_forward = True

        # Aux
        self.mqtt_conn = None
        self.sync_read_topic = None
        self.sync_read_data = None

    # ===================================================================================
    # Main functions
    # ===================================================================================
    def open(self):
        self.mqtt_conn = MqttConnection(self.client_id)
        self.mqtt_conn.on_new_message = self.__on_new_mqtt_message__
        self.mqtt_conn.persistent = self.persistent
        self.mqtt_conn.loop_async()

        super().open()

    def close(self):
        self.mqtt_conn.disconnect()
        super().close()

    def write(self, key, data):
        topic = self.key_to_topic(key, "w")
        message = self.data_to_mqtt_message(data)

        r = self.mqtt_conn.publish(topic, message)
        if r == 1: raise ConnectionWriteException("Connection refused, unacceptable protocol version")
        elif r == 2: raise ConnectionWriteException("Connection refused, identifier rejected")
        elif r == 3: raise ConnectionWriteException("Connection refused, server unavailable")
        elif r == 4: raise ConnectionWriteException("Connection refused, bad username or password")
        elif r == 5: raise ConnectionWriteException("Connection refused, not authorized")
        return

    def read(self, key, **kwargs):
        # Generate read request
        response_code = self.__generate_read_request__(key, **kwargs)
        self.sync_read_topic = self.key_to_topic(key, response_code)
        self.mqtt_conn.subscribe_single(self.sync_read_topic)

        # Wait for response
        t = time.time()
        while self.sync_read_data is None:
            if time.time() - t > self.read_timeout:
                self.sync_read_data = None
                raise ConnectionReadTimeoutException()

        # Return response
        response = self.sync_read_data
        self.sync_read_data = None
        return response

    # ===================================================================================
    # Subscribe and read async (error handling is done on __on_new_mqtt_message__)
    # ===================================================================================
    def subscribe(self, key, time_step=None):
        topic = self.key_to_topic(key)
        self.mqtt_conn.subscribe(topic)
        while topic in self.mqtt_conn.subscribe_topics: pass

    def read_async(self, key, **kwargs):
        response_code = self.__generate_read_request__(key, **kwargs)
        response_topic = self.key_to_topic(key, response_code)
        self.mqtt_conn.subscribe_single(response_topic)

    def subscribe_async(self, key, time_step=None):
        topic = self.key_to_topic(key)
        self.mqtt_conn.subscribe(topic)

    def unsubscribe(self, key):
        self.mqtt_conn.unsubscribe(self.key_to_topic(key))

    # ===================================================================================
    # Birth and last will
    # ===================================================================================
    def set_birth_message(self, key, data):
        self.mqtt_conn.birth_topic = self.key_to_topic(key)
        self.mqtt_conn.birth_message = self.data_to_mqtt_message(data)

    def set_last_will(self, key, data):
        self.mqtt_conn.last_will_topic = self.key_to_topic(key)
        self.mqtt_conn.last_will_message = self.data_to_mqtt_message(data)

    # ===================================================================================
    # Private
    # ===================================================================================
    def __on_new_mqtt_message__(self, topic, message):
        key = topic

        # Read request
        if topic.startswith("alv1/r/"):
            self.__on_new_read_request__(topic, message)
            return

        try:
            # Get key and data from mqtt message
            key = self.topic_to_key(topic)
            data = self.mqtt_message_to_data(message)
            # Response to read request
            if topic == self.sync_read_topic: self.sync_read_data = data
            # New data
            clean_data = self.__clean_read_data__(key, data)
            self.on_new_data(key, clean_data)

        except Exception as e:
            self.on_read_error(key, get_error_and_traceback_message(e))

    def __on_new_read_request__(self, topic, message):
        return

    def __generate_read_request__(self, key, **kwargs):
        # Create request message
        request = {"response_code": str(random.randint(0, 999999999))}
        # Add parameters
        for kw in kwargs: request[kw] = kwargs[kw]
        # Send request
        topic = self.key_to_topic(key, "r")
        message = self.data_to_mqtt_message(request)
        self.mqtt_conn.publish(topic, message)
        # Return response topic
        return request["response_code"]

    # ===================================================================================
    # Aux
    # ===================================================================================
    @staticmethod
    def topic_to_key(topic):
        topic = str(topic)
        if topic.startswith("alv1") and len(topic) > 7:
            sindex = topic.index("/", 5) + 1
            topic = topic[sindex:]
        return topic

    @staticmethod
    def key_to_topic(key, mode="w"):
        topic = "alv1/" + str(mode) + "/" + str(key)
        return topic

    @staticmethod
    def data_to_mqtt_message(data):
        return json.dumps(data, default=str)

    @staticmethod
    def mqtt_message_to_data(message):
        return json.loads(message)
