from ..connection import Connection
import snap7


class SiemensS7Connection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.force_close_on_read_error = True
        self.clean_on_read = False
        self.multithread = False

        # Parameters
        self.ip_address = "localhost"
        self.port = 102
        self.rack = 0
        self.slot = 1

        # Private
        self.connection = None

    def open(self):
        self.connection = snap7.client.Client()
        self.connection.connect(self.ip_address, rack=self.rack, slot=self.slot, tcpport=self.port)

    def close(self):
        if self.connection is None: return
        self.connection.disconnect()
        self.connection = None

    def is_connected(self):
        if self.connection is None: return False
        return self.connection.get_connected()

    def read(self, key, **kwargs):
        """
        Implement this function yourself.
        Use the snap7 library. For example:

        import snap7
        from snap7 import util

        result = {}
        tag = conn.db_read(1, 0, 19)
        result["DB1_18_0"] = util.get_bool(tag, 18, 0)
        ...
        """
        return []

    def write(self, key, data):
        """
        Implement this function yourself.
        Use the snap7 library.
        """
        return
