import pymongo
from ..generic.mongodb import MongoDBConnection
from ....common.database_field_parse import *
import datetime


class MongoDBTimeSeriesConnection(MongoDBConnection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.add_del_filter = False

    def __on_write_map__(self, record):
        record["t"] = datetime.datetime.strptime(record["t"], "%Y-%m-%dT%H:%M:%SZ")
        return {db_parse_field(f): record[f] for f in record}
        
    def write(self, key, data):
        collection = self.client[self.database][db_parse_key(key)]  # [database][collection]
        collection.create_index('t')
        collection.insert_many(list(map(self.__on_write_map__, data)))
