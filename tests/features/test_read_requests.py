"""
tests/services/read_req.py must be running
"""
from aleph.connections.namespace.mqtt_namespace_connection import MqttNamespaceConnection
import unittest


class TestReadRequests(unittest.TestCase):

    def test1(self):
        N = MqttNamespaceConnection()
        N.open()
        N.on_read_error = lambda e: e.raise_exception()
        data = N.safe_read("generic", since=None, limit=10)
        print("DATA", data)
        N.close()
        self.assertIsNotNone(data)
        self.assertTrue(data)  # Not empty


if __name__ == '__main__':
    unittest.main()

