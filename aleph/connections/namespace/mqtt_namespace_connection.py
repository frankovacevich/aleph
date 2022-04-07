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
        self.force_close_on_read_error = False
        self.force_close_on_write_error = False

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

        self.mqtt_conn.persistent = self.persistent
        self.mqtt_conn.on_disconnect = self.__on_disconnect__
        self.mqtt_conn.on_connect = self.__on_connect__
        self.mqtt_conn.on_new_message = self.__on_new_mqtt_message__

        if self.birth_key is not None and self.birth_message is not None:
            self.mqtt_conn.birth_message = self.data_to_mqtt_message(self.birth_message)
            self.mqtt_conn.birth_topic = self.key_to_topic(self.birth_key)
        if self.last_will_key is not None and self.last_will_message is not None:
            self.mqtt_conn.last_will_message = self.data_to_mqtt_message(self.last_will_message)
            self.mqtt_conn.last_will_topic = self.key_to_topic(self.last_will_key)

    def open(self):
        self.create_client()
        self.mqtt_conn.connect(timeout=self.default_time_step)
        # We need loop async because otherwise the connection closes after a few seconds
        self.mqtt_conn.loop_async()
        # Need this to work correctly (why?)
        time.sleep(0.01)

    def close(self):
        if self.mqtt_conn is None: return
        self.mqtt_conn.disconnect()

    def is_connected(self):
        if self.mqtt_conn is None: return False
        return self.mqtt_conn.connected

    def write(self, key, data):
        topic = self.key_to_topic(key, "w")
        message = self.data_to_mqtt_message(data)

        msg_info = self.mqtt_conn.publish(topic, message)
        if msg_info.rc == 1: raise Exception("Connection refused, unacceptable protocol version (r = 1)")
        elif msg_info.rc == 2: raise Exception("Connection refused, identifier rejected (r = 2)")
        elif msg_info.rc == 3: raise Exception("Connection refused, server unavailable (r = 3)")
        elif msg_info.rc == 4: raise Exception("Connection refused, bad username or password (r = 4)")
        elif msg_info.rc == 5: raise Exception("Connection refused, not authorized (r = 5)")
        elif msg_info.rc > 0: raise Exception("Mqtt error (r = " + str(msg_info.rc) + ")")
        return

    def read(self, key, **kwargs):
        # Generate read request
        self.__generate_read_request__(key, False, **kwargs)

        # Wait for response
        t = time.time()
        while self.__read_request_data__ is None:
            if time.time() - t > self.read_timeout:
                self.__read_request_data__ = None
                raise Exceptions.ConnectionReadingTimeout()

        # Return response
        return self.__read_request_data__

    # ===================================================================================
    # Opening async (same as open)
    # ===================================================================================
    def open_async(self, time_step=None):
        self.create_client()
        self.mqtt_conn.loop_async()

    # ===================================================================================
    # Subscribe and read async (error handling is done on __on_new_mqtt_message__)
    # ===================================================================================
    def subscribe(self, key, time_step=None):
        topic = self.key_to_topic(key)
        self.mqtt_conn.subscribe(topic)
        while topic in self.mqtt_conn.subscribe_topics: # Block thread
            try:
                pass
            except KeyboardInterrupt:
                self.unsubscribe(key)

    def read_async(self, key, **kwargs):
        self.__generate_read_request__(key, True, **kwargs)
        # This does not throw a timeout exception

    def subscribe_async(self, key, time_step=None):
        topic = self.key_to_topic(key)
        self.mqtt_conn.subscribe(topic)

    def unsubscribe(self, key):
        self.mqtt_conn.unsubscribe(self.key_to_topic(key))

    # ===================================================================================
    # Private
    # ===================================================================================
    def __clean_read_data__(self, key, data, **kwargs):
        # Data is list of records
        if not isinstance(data, list): return [data]
        else: return data

    def __on_new_mqtt_message__(self, topic, message):
        if topic.startswith("alv1/r"): return

        try:
            # Get key and data from mqtt message
            key = self.topic_to_key(topic)
            data = self.mqtt_message_to_data(message)

            if data is None: raise Exception("Remote service error")
            if not isinstance(data, list): data = [data]

            if topic == self.__read_request_topic__:
                self.__read_request_topic__ = None
                self.__read_request_data__ = data
            else:
                if len(data) == 0: return
                self.on_new_data(key, data)

        except Exception as e:
            self.on_read_error(Error(e, client_id=self.client_id))

    def __generate_read_request__(self, key, async_request, **kwargs):
        # Create request message
        request = {"t": time.time(), "response_code": str(random.randint(0, 999999999))}

        # Add parameters
        for kw in kwargs: request[kw] = kwargs[kw]
        request["cleaned"] = False

        # Get topic and message
        topic = self.key_to_topic(key, "r")
        message = self.data_to_mqtt_message(request)

        # Subscribe to response
        request_topic = self.key_to_topic(key, request["response_code"])
        if not async_request:
            self.__read_request_topic__ = request_topic
            self.__read_request_data__ = None
        self.mqtt_conn.subscribe_single(request_topic)

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
        topic = "alv1/" + str(mode) + "/" + str(key).replace(".", "/")
        return topic

    @staticmethod
    def data_to_mqtt_message(data):
        return json.dumps(data, default=str)

    @staticmethod
    def mqtt_message_to_data(message):
        return json.loads(message)
