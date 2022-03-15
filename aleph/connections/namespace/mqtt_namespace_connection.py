import json
import random
import time
from ...connections.connection import Connection
from ...common.mqtt_client import MqttClient
from ...common.exceptions import *


class MqttNamespaceConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__()
        self.client_id = client_id
        self.username = ""
        self.password = ""
        self.broker_address = "localhost"
        self.port = 1883

        self.tls_enabled = False
        self.ca_cert = ""
        self.client_cert = ""
        self.client_key = ""

        self.birth_key = None
        self.birth_message = None
        self.last_will_key = None
        self.last_will_message = None

        self.read_timeout = 10

        # Connection properties
        self.store_and_forward = True
        self.clean_on_read = False

        # Aux
        self.mqtt_conn = None
        self.__read_request_topic__ = None
        self.__read_request_data__ = None

    # ===================================================================================
    # Main functions
    # ===================================================================================
    def create_client(self):
        if self.mqtt_conn is not None: return

        self.mqtt_conn = MqttClient(self.client_id)
        self.mqtt_conn.username = self.username
        self.mqtt_conn.password = self.password
        self.mqtt_conn.broker_address = self.broker_address
        self.mqtt_conn.port = self.port
        self.mqtt_conn.tls_enabled = self.tls_enabled
        self.mqtt_conn.ca_cert = self.ca_cert
        self.mqtt_conn.client_cert = self.client_cert
        self.mqtt_conn.client_key = self.client_key

        self.mqtt_conn.birth_message = self.data_to_mqtt_message(self.birth_message)
        self.mqtt_conn.birth_topic = self.key_to_topic(self.birth_key)
        self.mqtt_conn.last_will_message = self.data_to_mqtt_message(self.last_will_message)
        self.mqtt_conn.last_will_topic = self.key_to_topic(self.last_will_key)

        self.mqtt_conn.persistent = self.persistent
        self.mqtt_conn.on_disconnect = self.on_disconnect
        self.mqtt_conn.on_connect = self.on_connect
        self.mqtt_conn.on_new_message = self.__on_new_mqtt_message__

    def open(self):
        self.create_client()
        # We need loop async because otherwise the connection closes after a few seconds
        self.mqtt_conn.loop_async()

    def close(self):
        self.mqtt_conn.disconnect()

    def is_connected(self):
        if self.mqtt_conn is None: return False
        return self.mqtt_conn.is_connected

    def write(self, key, data):
        topic = self.key_to_topic(key, "w")
        message = self.data_to_mqtt_message(data)

        r = self.mqtt_conn.publish(topic, message)
        if r == 1: raise Exception("Connection refused, unacceptable protocol version (r = 1)")
        elif r == 2: raise Exception("Connection refused, identifier rejected (r = 2)")
        elif r == 3: raise Exception("Connection refused, server unavailable (r = 3)")
        elif r == 4: raise Exception("Connection refused, bad username or password (r = 4)")
        elif r == 5: raise Exception("Connection refused, not authorized (r = 5)")
        elif r > 0: raise Exception("Mqtt error (r = " + str(r) + ")")
        return

    def read(self, key, **kwargs):
        # Generate read request
        self.__generate_read_request__(key, **kwargs)

        # Wait for response
        t = time.time()
        while self.__read_request_data__ is None:
            if time.time() - t > self.read_timeout:
                self.__read_request_data__ = None
                raise Exceptions.ConnectionReadingTimeout()

        # Return response
        response = self.__read_request_data__
        self.__read_request_data__ = None
        return response

    # ===================================================================================
    # Opening async (same as open)
    # ===================================================================================
    def open_async(self, time_step=None):
        self.open()

    # ===================================================================================
    # Subscribe and read async (error handling is done on __on_new_mqtt_message__)
    # ===================================================================================
    def subscribe(self, key, time_step=None):
        topic = self.key_to_topic(key)
        self.mqtt_conn.subscribe(topic)
        while topic in self.mqtt_conn.subscribe_topics: pass  # Block thread

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
    # Private
    # ===================================================================================
    def __on_new_mqtt_message__(self, topic, message):
        self.__process_mqtt_message__(topic, message)

    def __process_mqtt_message__(self, topic, message):
        try:
            # Get key and data from mqtt message
            key = self.topic_to_key(topic)
            data = self.mqtt_message_to_data(message)

            # Response to read request
            if topic == self.__read_request_topic__: self.__read_request_data__ = data

            # Callback
            if len(data) == 0: return
            self.on_new_data(key, data)

        except Exception as e:
            self.on_read_error(Error(e, client_id=self.client_id))

    def __generate_read_request__(self, key, **kwargs):
        # Create request message
        request = {"response_code": str(random.randint(0, 999999999))}

        # Add parameters
        for kw in kwargs: request[kw] = kwargs[kw]
        request["cleaned"] = False

        # Send request
        topic = self.key_to_topic(key, "r")
        message = self.data_to_mqtt_message(request)

        # Subscribe to response
        self.__read_request_topic__ = self.key_to_topic(key, request["response_code"])
        self.mqtt_conn.subscribe_single(self.__read_request_topic__)

        # Publish request
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
            s_index = topic.index("/", 5) + 1
            topic = topic[s_index:]
        return topic.replace("/", ".")

    @staticmethod
    def key_to_topic(key, mode="w"):
        topic = "alv1/" + str(mode) + "/" + str(key).replace("/", ".")
        return topic

    @staticmethod
    def data_to_mqtt_message(data):
        return json.dumps(data, default=str)

    @staticmethod
    def mqtt_message_to_data(message):
        return json.loads(message)
