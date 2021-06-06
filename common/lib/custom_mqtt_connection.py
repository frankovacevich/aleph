from ..bin.mqtt_connection import MqttConnection


def custom_mqtt_connection(client_id):
    mqtt_connection = MqttConnection(client_id)
    mqtt_connection.broker_address = "192.168.0.160"
    mqtt_connection.port = 8883
    mqtt_connection.certificates_folder = "common/certs"
    mqtt_connection.username = "maincnt"
    mqtt_connection.password = "NAJ6K5uJXaeNN28ZaRFRpdtLgXag77ef"
    mqtt_connection.tls_enabled = True
    return mqtt_connection
