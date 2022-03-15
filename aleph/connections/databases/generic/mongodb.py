from ....common.database_field_parse import *
from ...connection import Connection
import urllib.parse
import pymongo
import datetime


class MongoDBConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.server = "localhost"
        self.port = 27017
        self.username = None
        self.password = None
        self.database = "main"

        # Client
        self.clean_on_read = False
        self.add_del_filter = True

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

        deleted_filter = {}
        if self.add_del_filter: deleted_filter = {"deleted_": False}

        filter_ = {"$and": [time_filter, find_filter, deleted_filter]}

        # Prepare projection (fields)
        projection = {}
        if args["fields"] != "*":
            projection = {x: True for x in args["fields"]}
            # projection["t"] = True
        projection["_id"] = False

        # Get data from collection
        collection = self.client[self.database][key]
        found = collection.find(filter_, projection=projection, limit=args["limit"], skip=args["offset"])

        # Order
        if args["order"] is None:
            pass
        elif args["order"][0] == "-":
            found = found.sort([(args["order"][1:], pymongo.DESCENDING)])
        else:
            found = found.sort([(args["order"], pymongo.ASCENDING)])

        return list(map(self.__on_read_map__, found))

    def __on_read_map__(self, record):
        if "t" in record:  record["t"]  = record["t"].strftime("%Y-%m-%dT%H:%M:%SZ")
        if "t_" in record: record["t_"] = record["t_"].strftime("%Y-%m-%dT%H:%M:%SZ")
        return {db_deparse_field(f): record[f] for f in record}

    def write(self, key, data):
        collection = self.client[self.database][db_parse_key(key)]  # [database][collection]

        # Upsert
        for record in data:

            # Correct time
            record["t_"] = datetime.datetime.strptime(record.pop("t"), "%Y-%m-%dT%H:%M:%SZ")

            # Parse fields
            set_record = {db_parse_field(f): record[f] for f in record}

            # Insert
            if "id_" in record:
                collection.update_one({"id_": record["id_"]}, {
                    "$set": set_record,
                    "$setOnInsert": {"deleted_": False, "t": record["t_"]},
                }, upsert=True)

            else:
                collection.insert_one(record)

        # Create indices
        collection.create_index('t')
        collection.create_index('id_')
