import paho.mqtt.client as mqtt
import os
import json
import time


class MqttConnection:

    def __init__(self, client_id):
        self.client_id = client_id

        self.broker_address = "localhost"
        self.port = 1883
        self.qos = 1
        self.certificates_folder = ""
        self.tls_enabled = True
        self.username = ""
        self.password = ""
        self.subscription_topics = []
        self.default_topic = ""
        self.resend_on_failure = True
        self.keepalive = 300

        self.on_connect = None  # function()
        self.on_disconnect = None  # function()
        self.on_new_message = None  # function(topic, message)
        self.on_publish_failure = None  # function()

        self.connected = False
        self.failure_buffer = []
        self.mqtt_client = None
        self.persistent = False

    def __on_connect__(self, client, userdata, flags, rc):
        self.connected = True

        for topic in self.subscription_topics:
            self.mqtt_client.subscribe(topic.replace(".", "/"), qos=1)

        if self.on_connect is not None:
            self.on_connect()

        return

    def __on_disconnect__(self, client, userdata, rc):
        self.connected = False

        if self.on_disconnect is not None:
            self.on_disconnect()
        return

    def __on_new_message__(self, client, userdata, msg):
        if self.on_new_message is not None:
            topic = str(msg.topic).replace("/", ".")
            message = str(msg.payload.decode())
            try:
                message = json.loads(message)
            except:
                pass
            self.on_new_message(topic, message)
        return

    def __setup__(self, persistent):
        self.mqtt_client = mqtt.Client(self.client_id, clean_session=not persistent)
        self.mqtt_client.username_pw_set(username=self.username, password=self.password)
        self.mqtt_client.on_connect = self.__on_connect__
        self.mqtt_client.on_disconnect = self.__on_disconnect__
        self.mqtt_client.on_message = self.__on_new_message__

        if self.tls_enabled:
            try:
                self.mqtt_client.tls_set(
                    ca_certs=os.path.join(self.certificates_folder, "ca", "ca.crt"),
                    certfile=os.path.join(self.certificates_folder, "client", "client.crt"),
                    keyfile=os.path.join(self.certificates_folder, "client", "client.key")
                )
            except:
                pass

    def publish(self, message, topic=None):
        if topic is None: topic = self.default_topic
        topic = topic.replace(".", "/")
        m = message

        if isinstance(m, dict):
            m = json.dumps(message)

        # Publish message and get result code
        # If code == 0: success, else: failure
        r = 1
        if self.connected: r, mid = self.mqtt_client.publish(topic, m, self.qos)

        # Resend on failure
        if self.resend_on_failure:
            if r != 0:
                self.failure_buffer.append({"topic": topic, "message": m})
                if self.on_publish_failure is not None: self.on_publish_failure()
            else:
                if len(self.failure_buffer) > 0:
                    aux = []
                    for q in self.failure_buffer:
                        rq, midq = self.mqtt_client.publish(q["topic"], q["message"], self.qos)
                        if rq != 0:
                            aux.append(q)
                        self.failure_buffer = aux

        return r

    def connect(self, persistent=False, timeout=0):
        if self.mqtt_client is None: self.__setup__(persistent)
        t0 = time.time()
        while not self.connected:
            if timeout > 0 and time.time() - t0 > timeout:
                raise Exception("Connection timeout")

            try:
                r = self.mqtt_client.connect(self.broker_address, self.port, keepalive=self.keepalive)
                time.sleep(1)
                self.mqtt_client.loop()
            except:
                pass

    def loop(self):
        self.mqtt_client.loop()

    def disconnect(self):
        self.mqtt_client.disconnect()

    def loop_async(self, persistent=False):
        self.__setup__(persistent)
        self.mqtt_client.connect_async(self.broker_address, self.port, keepalive=self.keepalive)
        # The loop start method will handle reconnection automatically
        self.mqtt_client.loop_start()

    def loop_forever(self, persistent=False):
        self.connect(persistent)
        self.mqtt_client.loop_forever()
        pass
