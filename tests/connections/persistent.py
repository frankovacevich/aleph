from aleph import Connection
import time
import datetime
from dateutil.tz import tzutc


class MyConnection(Connection):

    def __init__(self):
        super().__init__()
        self.persistent

    def read(self, key, **kwargs):
        t = self.local_storage.get(self.local_storage.LAST_TIME_READ, "??")
        if t != "??": print(t, datetime.datetime.utcfromtimestamp(t[key]).replace(tzinfo=tzutc()))
        else: print(t)
        print(kwargs["since"])
        return []


M = MyConnection()
M.on_read_error = lambda e: print(e)
M.open()

print("===================================================")
print("READ @", datetime.datetime.today().astimezone(tz=tzutc()).strftime("%Y-%m-%d %H:%M:%S"), round(time.time()))
M.safe_read("A")
print(M.local_storage.get(M.local_storage.LAST_TIME_READ))
time.sleep(5)

print("===================================================")
print("READ @", datetime.datetime.today().astimezone(tz=tzutc()).strftime("%Y-%m-%d %H:%M:%S"), round(time.time()))
M.safe_read("A")
print(M.local_storage.get(M.local_storage.LAST_TIME_READ))
time.sleep(5)

print("===================================================")
print("READ @", datetime.datetime.today().astimezone(tz=tzutc()).strftime("%Y-%m-%d %H:%M:%S"), round(time.time()))
M.safe_read("A")
print(M.local_storage.get(M.local_storage.LAST_TIME_READ))