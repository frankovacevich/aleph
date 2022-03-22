from aleph.connections.databases.timeseries.mongodb import MongoDBTimeSeriesConnection


M = MongoDBTimeSeriesConnection()
M.on_write_error = lambda e: e.raise_exception()
M.on_read_error  = lambda e: e.raise_exception()



#M.safe_write("test56", {"a": 1, "b": 2})
M.safe_write("test57", {"x.y": 1, "b": 1})

print(M.safe_read("test57", since=None, timezone="Local"))
