from aleph.connections.namespace.mqtt_namespace_connection import MqttNamespaceConnection
from aleph.common.exceptions import Error
from aleph import Service, Connection

import threading
import logging
import time

logger = logging.getLogger(__name__)


class MqttService(Service):

    # Private
    read_requests_server = None

    def on_read_request(self, key, **kwargs):
        return self.connection.safe_read(key, **kwargs)

    def on_read_request_error(self, error):
        return

    def setup(self):
        # Create read requests server
        logger.info("Creating read requests server")
        self.namespace_connection.mqtt_conn.on_new_message = self.__on_new_mqtt_message__
        for key in self.read_request_keys:
            topic = self.namespace_connection.key_to_topic(key, "r")
            self.namespace_connection.mqtt_conn.subscribe(topic)

    # ===================================================================================
    # Private
    # ===================================================================================
    def __on_new_mqtt_message__(self, topic, message):
        if topic.startswith("alv1/r/"):
            if self.connection.multi_threaded:
                threading.Thread(target=self.__on_new_mqtt_message_thread__, args=(topic, message), daemon=True).start()
            else:
                self.connection.__run_on_async_thread__(self.__on_new_mqtt_message_async__(topic, message))
        else:
            self.namespace_connection.__on_new_mqtt_message__(topic, message)

    def __on_new_mqtt_message_thread__(self, topic, message):
        try:
            key = self.namespace_connection.topic_to_key(topic)
            args = self.namespace_connection.mqtt_message_to_data(message)
            logger.info(f"Read request on {key} ? {args}")

            if "response_code" not in args: return
            if "t" not in args or time.time() - args["t"] > 10: return

            # Generate response
            response_topic = self.namespace_connection.key_to_topic(key, args["response_code"])
            response_data = self.on_read_request(key, **args)
            if response_data is None: return
            response_message = self.namespace_connection.data_to_mqtt_message(response_data)

            # Publish response
            self.namespace_connection.mqtt_conn.publish(response_topic, response_message)

        except Exception as e:
            self.on_read_request_error(Error(e))

    async def __on_new_mqtt_message_async__(self, topic, message):
        return self.__on_new_mqtt_message_thread__(topic, message)
