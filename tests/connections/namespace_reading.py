"""
tests/services/generic.py must be running
"""

from aleph.connections.namespace.mqtt_namespace_connection import MqttNamespaceConnection

N = MqttNamespaceConnection()
N.on_read_error = lambda e: print(e)
data = N.safe_read("generic", since=None, limit=10)
print("DATA", data)
