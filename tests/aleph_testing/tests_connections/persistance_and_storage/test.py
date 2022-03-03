from aleph.common.local_storage import *
from aleph import Connection
import openpyxl
import time
import pandas as pd


#########################################

L = SqliteDictLocalStorage("test.txt")
print(L.data)
L.set("hello", 66)
L.get("hello")
L.set("hij", {"x": 5})
L.get("hij")
L.set("hij", {"x": {"y": 9}})
L.get("hij")




#########################################








class TestConnection(Connection):

	DATA = [
		{"A": 1, "B": True, "C": 0},
		{"A": 2, "B": False},
		{"A": 3, "B": True, "C": 0},
		{"A": 4, "B": True},
		{"A": 5, "C": 0},
		{"A": 6, "C": 0},
		{"A": 7, "B": True, "C": 1},
	]

	i = 0

	def __init__(self):
		super().__init__()
		self.persistent = True
		self.local_storage = RedisLocalStorage("local_storage.db")

	def read(self, key, **kwargs):
		data = self.DATA[self.i % 7]
		self.i += 1
		return data

X = TestConnection()
X.on_read_error = lambda x: print(x.raise_exception())
X.open()

print(X.read("s"))

data = X.safe_read("s")
print(data)

data = X.safe_read("s")
print(data)