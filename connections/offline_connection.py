from .connection import Connection


class OfflineConnection(Connection):

    def __init__(self):
        self.namespace_connection = None
        self.local_connections = {}           # {key: Connection} dict of local connections

    def open(self):
        pass

    def close(self):
        pass

    def read(self, key, **kwargs):
        pass

    def write(self, key, data):
        pass


"""
sync:
- open namespace
- for each key:
  - open connection
  - read local connection and get last record
  - read namespace since last record
  - store data on local connection
  - subscribe to each topic on the namespace connection
  
on new message from namespace
- write to local connection

on read
- read from local connection

on write
- write to local connection
- write to namespace (use snf)
"""

