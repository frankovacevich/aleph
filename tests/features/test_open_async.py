from aleph import Connection
import time


class MyConnection(Connection):

    def __init__(self):
        super().__init__()
        self.connected_at = time.time()
        self.default_time_step = 1
        self.i = 0

    def open(self):
        if time.time() - self.connected_at > 10:
            raise Exception("AHH")

    def is_connected(self):
        if self.connected_at is None:
            return False

        print("Checking...", self.i, time.time() - self.connected_at)
        self.i += 1

        if time.time() - self.connected_at > 10:
            return False

        return True


M = MyConnection()
M.on_connect = lambda: print("CONNECTED")
M.on_disconnect = lambda: print("DIS-CONNECTED")
M.open_async()

while True:
    pass
