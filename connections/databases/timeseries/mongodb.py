"""
This database connection only stores time series data
"""

import pymongo
from ..interfaces import MongoDBInterfaceConnection
from ....common.database_field_parse import *
from ....common.data_filter import DataFilter


class MongoDBTimeSeries(MongoDBInterfaceConnection):

    def read(self, key, **kwargs):
        # Parse args and key
        args = self.__clean_read_args__(key, **kwargs)
        self.skip_read_cleaning = True
        key = db_parse_key(key)

        # Prepare filter (time and filter)
        time_filter = {}
        if args["since"] is not None and args["until"] is not None:
            time_filter = {"t": {"$gte": args["since"], "$lte": args["until"]}}
        elif args["since"] is not None:
            time_filter = {"t": {"$gte": args["since"]}}
        elif args["until"] is not None:
            time_filter = {"t": {"$lte": args["until"]}}

        find_filter = DataFilter.load(args["filter"]).to_mongodb_filter()
        find_filter = {"$and": [time_filter, find_filter]}

        # Prepare projection (fields)
        projection = {}
        if args["fields"] != "*":
            projection = {x: True for x in args["fields"]}
            projection["t"] = True
        projection["_id"] = False

        # Prepare ordering field and direction
        if args["order"][0] == "-":
            sorting_field = args["order"][1:]
            sorting_order = pymongo.DESCENDING
        else:
            sorting_field = args["order"]
            sorting_order = pymongo.ASCENDING

        # Get data from collection
        collection = self.client[self.database][key]
        found = collection.find(find_filter, projection=projection, limit=args["limit"], skip=args["offset"])
        found = found.sort([(sorting_field, sorting_order)])

        return list(found)

    def write(self, key, data):
        collection = self.client[self.database][db_parse_key(key)]  # [database][collection]
        collection.create_index('t')
        collection.insert_many(data)
