import time
from aleph.connections.testing.random import RandomConnection


R = RandomConnection()
R.write_async("test", {"x": 1})

while True:
    time.sleep(1)

