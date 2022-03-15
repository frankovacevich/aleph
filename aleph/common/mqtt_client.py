import paho.mqtt.client as mqtt
import time
from ..common.exceptions import *


class MqttClient:

    def __init__(self, client_id=""):
        self.client_id = client_id

        self.broker_address = "localhost"
        self.port = 1883
        self.qos = 1
        self.ca_cert = ""
        self.client_cert = ""
        self.client_key = ""
        self.tls_enabled = False
        self.username = ""
        self.password = ""
        self.subscribe_topics = []
        self.subscribe_single_topics = []
        self.keepalive = 60
        self.persistent = False

        self.on_connect = None  # function()
        self.on_disconnect = None  # function()
        self.on_new_message = None  # function(topic, message)

        self.birth_topic = None
        self.birth_message = None
        self.last_will_topic = None
        self.last_will_message = None

        self.connected = False
        self.connecting = False
        self.mqtt_client = None

        self.__single_subscribe_topic__ = None
        self.__single_subscribe_message__ = None
        self.single_subscribe_timeout = 10

    # ===================================================================================
    # Callback functions
    # ===================================================================================

    def __on_connect__(self, client, userdata, flags, rc):
        self.connected = True
        self.connecting = False

        for topic in self.subscribe_topics:
            self.mqtt_client.subscribe(topic, qos=self.qos)

        if self.birth_topic is not None and self.birth_message is not None:
            self.publish(self.birth_topic, self.birth_message, qos=1)

        if self.on_connect is not None:
            self.on_connect()

    def __on_disconnect__(self, client, userdata, rc):
        self.connected = False
        self.connecting = False

        if self.on_disconnect is not None:
            self.on_disconnect()

    def __on_new_message__(self, client, userdata, msg):
        # TODO: avoid messages from the same client
        topic = str(msg.topic)
        message = str(msg.payload.decode())

        # Single subscribe
        if topic in self.subscribe_single_topics:
            self.unsubscribe(topic)

        # On new message
        if self.on_new_message is None: return
        self.on_new_message(topic, message)

    # ===================================================================================
    # Connect / Disconnect
    # ===================================================================================

    def __setup__(self):
        if self.mqtt_client is not None: return
        self.mqtt_client = mqtt.Client(self.client_id, clean_session=not self.persistent)
        self.mqtt_client.username_pw_set(username=self.username, password=self.password)
        self.mqtt_client.on_connect = self.__on_connect__
        self.mqtt_client.on_disconnect = self.__on_disconnect__
        self.mqtt_client.on_message = self.__on_new_message__

        if self.tls_enabled:
            self.mqtt_client.tls_set(ca_certs=self.ca_cert, certfile=self.client_cert, keyfile=self.client_key)

        if self.last_will_topic is not None and self.last_will_message is not None:
            self.mqtt_client.will_set(
                topic=self.last_will_topic,
                payload=self.last_will_message,
                qos=1,
            )

    def connect(self, timeout=0):
        if self.connecting: return
        self.connecting = True
        self.__setup__()
        t0 = time.time()
        while not self.connected:
            self.mqtt_client.connect(self.broker_address, self.port, keepalive=self.keepalive)
            time.sleep(0.1)
            self.mqtt_client.loop()
            if 0 < timeout < time.time() - t0:
                raise Exceptions.ConnectionOpeningTimeout("Mqtt Client (client_id = " + self.client_id + " failed to connect)")

    def disconnect(self):
        self.mqtt_client.disconnect()
        self.mqtt_client = None

    def connect_async(self):
        if self.connecting: return
        self.connecting = True
        self.mqtt_client.connect_async(self.broker_address, self.port, keepalive=self.keepalive)

    # ===================================================================================
    # Publish
    # ===================================================================================
    def publish(self, topic, payload, qos=None):
        r = self.mqtt_client.publish(topic, payload, qos if qos is not None else self.qos)
        return r

    # ===================================================================================
    # Loop
    # ===================================================================================
    def loop(self):
        self.mqtt_client.loop()

    def loop_async(self):
        self.__setup__()
        if not self.connecting:
            self.connecting = True
            self.mqtt_client.connect_async(self.broker_address, self.port, keepalive=self.keepalive)
        # The loop start method will handle reconnection automatically
        self.mqtt_client.loop_start()

    def loop_forever(self):
        self.__setup__()
        while not self.connected:
            try:
                self.mqtt_client.connect(self.broker_address, self.port, keepalive=self.keepalive)
                time.sleep(1)
                self.mqtt_client.loop()
            except:
                pass

        # The loop forever method will handle reconnection automatically
        self.mqtt_client.loop_forever()

    # ===================================================================================
    # Subscribe
    # ===================================================================================
    def subscribe(self, topic):
        self.mqtt_client.subscribe(topic)
        self.subscribe_topics.append(topic)

    def unsubscribe(self, topic):
        self.mqtt_client.unsubscribe(topic)
        if topic in self.subscribe_topics: self.subscribe_topics.remove(topic)
        if topic in self.subscribe_single_topics: self.subscribe_single_topics.remove(topic)

    def subscribe_single(self, topic):
        self.mqtt_client.subscribe(topic)
        self.subscribe_single_topics.append(topic)
