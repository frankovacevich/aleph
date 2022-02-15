"""
With this connection records are stored as flat. It does not support
nested records. Nested records are flattened on safe_write and dots
are replaced here by two underscores ("__").
"""

import pymongo
from ..interfaces import MongoDBInterfaceConnection
from ....common.database_field_parse import *
from ....common.data_filter import DataFilter


class MongoDB(MongoDBInterfaceConnection):

    def read(self, key, **kwargs):
        # Parse args and key
        args = self.__clean_read_args__(key, **kwargs)
        self.skip_read_cleaning = True
        key = db_parse_key(key)

        # Prepare filter (time and filter)
        time_filter = {"t": {"$gte": args["since"], "$lte": args["until"]}}
        find_filter = DataFilter.load(args["filter"]).to_mongodb_filter()
        deleted_filter = {"deleted_": False}
        filter_ = {"$and": [time_filter, find_filter, deleted_filter]}

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
        found = collection.find(filter_, projection=projection, limit=args["limit"], skip=args["offset"])
        found = found.sort([(sorting_field, sorting_order)])

        return list(found)

    def write(self, key, data):
        collection = self.client[self.database][db_parse_key(key)]  # [database][collection]

        # Upsert
        for record in data:
            record["t_"] = record["t"]
            set_record = {f: db_parse_field(record[f]) for f in record}
            set_record["deleted_"] = False
            update_record = {f: db_parse_field(record[f]) for f in record if f != "t"}

            # Insert
            if "id_" in record:
                collection.update({"id_": record["id_"]}, {
                    "$set": set_record,
                    "$setOnInsert": update_record,
                }, upsert=True)

            else:
                collection.insert_one(record)

        # Create indices
        collection.create_index('t')
        collection.create_index('id_')
