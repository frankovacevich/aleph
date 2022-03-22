"""
This is a simple Namespace Connection
Use with tests/services/feed.py to test that:
- messages are being received
- store and forward works correctly
- the service and connection handle connecting, disconnecting and reconnecting correctly
This requires turning on/off the MQTT Broker during the tests.
"""

from aleph.connections.namespace.mqtt_namespace_connection import MqttNamespaceConnection


N = MqttNamespaceConnection("")

N.on_connect = lambda: print("CONNECTED")
N.on_disconnect = lambda: print("DISCONNECTED")
N.on_new_data = lambda k, d: print(k, d)
N.on_read_error = lambda e: print(e)

N.open_async()
N.subscribe("generic")
