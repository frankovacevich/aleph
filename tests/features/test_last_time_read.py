from aleph import Connection
import datetime
import time


class TestConn(Connection):
    t0 = datetime.datetime.today()
    data = [
        {"x": 1, "t": t0 - datetime.timedelta(seconds=15)},
        {"x": 2, "t": t0 - datetime.timedelta(seconds=10)},
        {"x": 3, "t": t0 - datetime.timedelta(seconds=5)},
        {"x": 4, "t": t0},
        {"x": 5, "t": t0 + datetime.timedelta(seconds=5)},
        {"x": 6, "t": t0 + datetime.timedelta(seconds=10)},
        {"x": 7, "t": t0 + datetime.timedelta(seconds=15)},
        {"x": 8, "t": t0 + datetime.timedelta(seconds=20)},
        {"x": 9, "t": t0 + datetime.timedelta(seconds=25)},
        {"x": 10, "t": t0 + datetime.timedelta(seconds=30)},
    ]

    def read(self, key, **kwargs):

        print(kwargs["since"], kwargs["until"])

        return self.data


T = TestConn()
for t in T.data:
    print(t["x"], t["t"])


time.sleep(2)
print(T.safe_read(""))
time.sleep(5)
print(T.safe_read(""))
time.sleep(5)
print(T.safe_read(""))
time.sleep(5)
print(T.safe_read(""))
time.sleep(5)
print(T.safe_read(""))
time.sleep(5)
print(T.safe_read(""))
time.sleep(5)
print(T.safe_read(""))
time.sleep(5)
print(T.safe_read(""))
time.sleep(5)
print(T.safe_read(""))
time.sleep(5)
print(T.safe_read(""))
