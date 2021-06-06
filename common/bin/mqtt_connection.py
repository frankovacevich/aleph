import paho.mqtt.client as mqtt
import os
import json
import time


class MqttConnection:

    def __init__(self, client_id):
        self.client_id = client_id

        self.broker_address = "localhost"
        self.port = 1883
        self.qos = 1  # 0 = no delivery guaranteed, 1 = at least one delivery guaranteed, 2 = exactly delivered once
        self.certificates_folder = ""
        self.tls_enabled = True
        self.username = ""
        self.password = ""
        self.subscription_topics = []
        self.default_topic = "test"
        self.resend_on_failure = True
        self.keepalive = 300
        self.auto_loop = True

        self.on_connect = None  # function ()
        self.on_disconnect = None  # function ()
        self.on_new_message = None  # function (topic, message)

        ##
        self.connected = False
        self.mqtt_client = mqtt.Client(self.client_id)
        self.failure_buffer = []
        self.last_published_message = 0
        return

    def connect(self):
        self.mqtt_client.username_pw_set(username=self.username, password=self.password)
        self.mqtt_client.on_connect = self.__on_connect__
        self.mqtt_client.on_disconnect = self.__on_disconnect__
        self.mqtt_client.on_message = self.__on_new_message__

        if self.tls_enabled:
            try:
                self.mqtt_client.tls_set(
                    ca_certs=os.path.join(self.certificates_folder, "ca/ca.crt"),
                    certfile=os.path.join(self.certificates_folder, "client/client.crt"),
                    keyfile=os.path.join(self.certificates_folder, "client/client.key")
                )
            except:
                pass

        r = self.reconnect()
        return r

    def reconnect(self):
        r = self.mqtt_client.connect(self.broker_address, self.port, keepalive=self.keepalive)
        t = time.time()
        while not self.connected:
            self.mqtt_client.loop()
            if time.time() - t > 5:
                raise Exception("Connection timeout")

        if self.auto_loop:
            self.mqtt_client.loop_stop()
            self.mqtt_client.loop_start()

        return r

    def disconnect(self):
        self.mqtt_client.disconnect()

    def __on_connect__(self, client, userdata, flags, rc):
        self.connected = True

        for topic in self.subscription_topics:
            self.mqtt_client.subscribe(topic.replace(".", "/"))

        if self.on_connect is not None:
            self.on_connect()
        return

    def __on_disconnect__(self, client, userdata, rc):
        self.connected = False

        if self.on_disconnect is not None:
            self.on_disconnect()
        return

    def __on_new_message__(self, client, userdata, msg):
        topic = str(msg.topic).replace("/", ".")
        message = str(msg.payload.decode())
        try:
            message = json.loads(message)
        except:
            pass

        if self.on_new_message is not None:
            self.on_new_message(topic, message)
        return

    def publish(self, message, topic=None):
        if topic is None: topic = self.default_topic
        topic = topic.replace(".", "/")
        m = message

        if isinstance(message, dict):
            m = json.dumps(message)

        # Publish message and get result code
        # If code == 0 : success, else : failure
        if self.last_published_message > self.keepalive: self.loop()
        r = 1
        if self.connected: r, mid = self.mqtt_client.publish(topic, m, self.qos)

        # Resend on failure
        if self.resend_on_failure:
            if r != 0:
                self.failure_buffer.append({"topic": topic, "message": m})
            else:
                if len(self.failure_buffer) > 0:
                    aux = []
                    for q in self.failure_buffer:
                        rq, midq = self.mqtt_client.publish(q["topic"], q["message"], self.qos)
                        if rq != 0:
                            aux.append(q)
                    self.failure_buffer = aux

        return r

    def loop(self):
        self.mqtt_client.loop()

    def loop_start(self):
        self.mqtt_client.loop_start()

    def loop_stop(self):
        self.mqtt_client.loop_stop()

    def loop_forever(self):
        self.auto_loop = False
        self.mqtt_client.loop_forever()
