from aleph.common.exceptions import *
from ..service import Service
from ..gateway import GatewayService


# ===================================================================================
# Main service
# ===================================================================================
class MqttService(Service):
    read_requests_server = None

    def accept_read_requests(self):
        self.read_requests_server = MqttReadRequestsServer(
            self.namespace_connection,
            self.read_request_keys,
            self.on_read_request,
            self.on_read_request_error
        ).start()


# ===================================================================================
# Gateway service
# ===================================================================================
class MqttGatewayService(GatewayService):
    local_server = None
    remote_server = None

    def accept_read_requests(self):
        self.local_server = MqttReadRequestsServer(
            self.local_namespace_connection,
            self.local_root_key,
            self.on_read_request,
            self.on_read_request_error
        ).start()

        self.remote_server = MqttReadRequestsServer(
            self.remote_namespace_connection,
            self.remote_root_key,
            self.on_read_request,
            self.on_read_request_error
        ).start()


# ===================================================================================
# Helper class
# ===================================================================================
class MqttReadRequestsServer:

    def __init__(self, conn, keys, on_read_request, on_read_request_error):
        self.keys = keys
        self.conn = conn
        self.on_read_request = on_read_request
        self.on_read_request_error = on_read_request_error

    def start(self):
        self.conn.mqtt_conn.on_new_message = self.__on_new_mqtt_message__
        for key in self.keys:
            topic = self.conn.key_to_topic(key, "r")
            self.conn.mqtt_conn.subscribe(topic)

        return self

    def __on_new_mqtt_message__(self, topic, message):
        if topic.startswith("alv1/r/"):
            try:
                key = self.conn.topic_to_key(topic)
                args = self.conn.mqtt_message_to_data(message)
                if "response_code" not in args: return

                # Generate response
                response_topic = self.conn.key_to_topic(key, args["response_code"])
                response_data = self.on_read_request(key, **args)
                response_message = self.conn.data_to_mqtt_message(response_data)

                # Publish response
                self.conn.mqtt_conn.publish(response_topic, response_message)

            except Exception as e:
                self.on_read_request_error(Error(e))

        else:
            self.conn.__on_new_mqtt_message__(topic, message)

