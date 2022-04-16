from aleph.common.exceptions import Error
from aleph import Service
import threading
import logging
import time

logger = logging.getLogger(__name__)


# ===================================================================================
# Main service
# ===================================================================================
class MqttService(Service):
    read_requests_server = None

    def accept_read_requests(self):
        logger.info("Creating read requests server")
        self.read_requests_server = MqttReadRequestsServer(
            self.namespace_connection,
            self.read_request_keys,
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
        if self.conn.multithread:
            threading.Thread(target=self.__on_new_mqtt_message_thread__, args=(topic, message), daemon=True).start()
        else:
            self.__on_new_mqtt_message_thread__(topic, message)

    def __on_new_mqtt_message_thread__(self, topic, message):
        if topic.startswith("alv1/r/"):
            try:
                key = self.conn.topic_to_key(topic)
                args = self.conn.mqtt_message_to_data(message)
                logger.info(f"Read request on {key} ? {args}")

                if "response_code" not in args: return
                if "t" not in args or time.time() - args["t"] > 10: return

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
