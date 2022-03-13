from ....common.database_field_parse import *
from ...connection import Connection
import urllib.parse
import pymongo


class MongoDBConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.server = "localhost"
        self.port = 27017
        self.username = None
        self.password = None
        self.database = "main"

        # Client
        self.check_filters_on_read = False
        self.check_timestamp_on_read = False

        self.client = None

    def open(self):
        url = "mongodb://"
        if self.username is not None: url += urllib.parse.quote(self.username)
        if self.password is not None: url += ":" + urllib.parse.quote(self.password)
        if self.username is not None: url += "@"
        url += self.server + ":" + str(self.port)
        self.client = pymongo.MongoClient(url)
        super().open()

    def close(self):
        if self.client is not None: self.client.close()
        self.client = None
        super().close()

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

        find_filter = {}
        if args["filter"] is not None: find_filter = args["filter"].to_mongodb_filter()
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

