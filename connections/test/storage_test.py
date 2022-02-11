from ..connection import Connection
import time


class StorageTestConnection(Connection):

    def __init__(self):
        super().__init__()
        self.data = {}

    def read(self, key, **kwargs):
        if key not in self.data: return []

        result = []
        for record in self.data[key]:
            #
            #
            result.append(record)

        return result

    def write(self, key, data):
        if key not in self.data: self.data[key] = []

        for record in data:
            if "t" not in record: record["t"] = time.time()
            self.data[key].append(record)
