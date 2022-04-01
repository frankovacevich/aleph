from aleph import Connection
import time


class MyConnection(Connection):

    def __init__(self):
        super().__init__()
        self.connected_at = time.time()
        self.default_time_step = 5
        self.multithread = False
        self.t0 = time.time()

    def open(self):
        return

    def read(self, key, **kwargs):
        print("Reading ...", time.time() - self.t0)
        time.sleep(1 if key == "A" else 2)
        return {"x": 1}


M = MyConnection()
M.on_new_data = lambda k, d: print(k, d)
M.on_read_error = lambda e: print(e)
M.subscribe_async("A")
M.subscribe_async("B")

while True:
    pass