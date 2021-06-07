from ..bin.mqtt_connection import MqttConnection
from ..root_folder import aleph_root_folder
import os


def custom_mqtt_connection(client_id):
    mqtt_connection = MqttConnection(client_id)
    mqtt_connection.broker_address = "192.168.0.160"
    mqtt_connection.port = 8883
    mqtt_connection.certificates_folder = os.path.join(aleph_root_folder, "common", "certs")
    mqtt_connection.username = "maincnt"
    mqtt_connection.password = ""
    mqtt_connection.tls_enabled = True
    return mqtt_connection
