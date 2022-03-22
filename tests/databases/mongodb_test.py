from aleph.connections.databases.generic.mongodb import MongoDBConnection


M = MongoDBConnection()
M.on_write_error = lambda e: e.raise_exception()
M.on_read_error  = lambda e: e.raise_exception()

key = "test6"

#print(M.safe_write(key, [{"id_": "1", "a": 1, "b": 2}, {"id_": "2", "a": 2, "b": 3}]))
#print(M.safe_write(key, [{"id_": "2", "a": 5, "b": 5}]))

#print(M.safe_read(key, since=None))
#print("##")
print(M.safe_read(key, since=None, timezone="Local"))

#print(M.read(key, since=None))