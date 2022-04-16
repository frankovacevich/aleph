from aleph import Connection
import time


class MyConnection(Connection):

    def __init__(self):
        super().__init__()
        self.t0 = time.time()
        self.default_time_step = 5
        self.multithread = True
        self.i = 0

    def read(self, key, **kwargs):
        print("Reading", self.i)
        return {"i": self.i}

    def open(self):
        if 60 > time.time() - self.t0 > 30:
            raise Exception("Can't open")

    def is_connected(self):
        print("Checking...", self.i, time.time() - self.t0)
        self.i += 1

        if 60 > time.time() - self.t0 > 30: return False
        else: return True


M = MyConnection()
M.on_connect = lambda: print("CONNECTED")
M.on_disconnect = lambda: print("DIS-CONNECTED")
M.open_async()
M.subscribe_async("")

while True:
    pass
