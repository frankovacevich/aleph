from aleph.common.local_storage import FileLocalStorage
from aleph.connections.files.generic.excel import ExcelConnection
import openpyxl
import time
import pandas as pd


class ExcelTest(ExcelConnection):

    def __init__(self):
        super().__init__()
        self.file = "test.xlsx"
        self.local_storage = FileLocalStorage("local_storage.txt")

    def read(self, key, **kwargs):
        data = super().read(key, **kwargs)
        return data

# Create and open connection
X = ExcelTest()
X.on_read_error = lambda x: print(x.raise_exception())
X.open()

# Create workbook
wb = openpyxl.Workbook()
ws = wb.active

# 0
# 
# Append data
ws.append([0, 1, 2])
ws.append([3, 4, 5])
ws.append([6, 7, 8])
wb.save('test.xlsx')


# Read once
data = X.safe_read("Sheet")

for x in X.local_storage.data["_past_values"]: print("  ", x, X.local_storage.data["_past_values"][x])

print("DATA:")
for x in data: print(x)
print("###################")

# 1
#
# Append more data
ws.append([99, 99, 99])
ws.append([12, 12, 88])
wb.save('test.xlsx')

# Read again
data = X.safe_read("Sheet")

for x in X.local_storage.data["_past_values"]: print("  ", x, X.local_storage.data["_past_values"][x])

print("DATA:")
for x in data: print(x)
print("###################")

# Read again
data = X.safe_read("Sheet")
print("DATA:")
for x in data: print(x)
print("###################")

# 2
#
# Append more data
ws.append([4, 4, 4])
ws.append([2, 1, 3])
wb.save('test.xlsx')

# Read again
data = X.safe_read("Sheet")

for x in X.local_storage.data["_past_values"]: print("  ", x, X.local_storage.data["_past_values"][x])

print("DATA:")
for x in data: print(x)
print("###################")
