import unittest
from aleph.connections.system.ping import PingConnection


# Test case
class TestPing(unittest.TestCase):

    myPingConnection = PingConnection()

    def test1(self):
        r = self.myPingConnection.safe_read("8.8.8.8")
        self.assertEqual(len(r), 1)
        self.assertTrue("8_8_8_8" in r[0])
        self.assertTrue(isinstance(r[0]["8_8_8_8"], bool))


if __name__ == '__main__':
    unittest.main()
