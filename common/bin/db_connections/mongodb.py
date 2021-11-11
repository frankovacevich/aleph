import pymongo
import time
import math
import datetime
import urllib.parse
from dateutil.tz import tzutc


class MongoDBConnection:

    def __init__(self, username, password, database, server="localhost", port=27017):
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.database = database

        self.buffer_size = 1
        self.buffer_count = 0
        self.buffer = {}

        ##
        self.client = None
        return

    def connect(self):
        self.client = pymongo.MongoClient("mongodb://" + urllib.parse.quote(self.username) + ":" +
                                          urllib.parse.quote(self.password) + "@" + self.server + ":" + str(self.port))
        return

    def close(self):
        self.client.close()

    # ==========================================================================
    # Operations (save, get, delete)
    # ==========================================================================

    def save_data(self, key, data):
        if key == "metadata": raise Exception("Invalid key")
        coll = self.client[self.database][key]

        # Remove invalid values and update metadata
        metadata = self.client[self.database]["metadata"]
        dat = {}
        for field in data:
            v = data[field]
            if v is None: continue
            if isinstance(v, float) and (math.isinf(v) or math.isnan(v)): continue
            dat[field.replace(".", "_")] = v

            updt = {'key': key, 'field': field, 'alias': field, 'description': ''}
            metadata.update_one({'key': key, 'field': field}, {'$set': updt}, upsert=True)

        # Parse t
        dat["t"] = datetime.datetime.strptime(dat["t"], '%Y-%m-%dT%H:%M:%SZ')

        # Insert data
        if "id_" in data:
            # coll.update_one({"id_": dat["id_"]}, {"$set": dat}, upsert=True)
            if coll.find_one({"id_": dat["id_"]}) is None:
                coll.insert_one(dat)
            else:
                dat["t_"] = dat["t"]
                del dat["t"]
                coll.update_one({"id_": dat["id_"]}, {"$set": dat})

        else:
            coll.insert_one(dat)
            #if key not in self.buffer: self.buffer[key] = []
            #self.buffer[key].append(dat)
            #self.buffer_count += 1
            #if self.buffer_count >= self.buffer_size:
            #    self.buffer_count = 0
            #    for key in self.buffer:
            #        if len(self.buffer[key]) == 0: continue
            #        coll.insert_many(self.buffer[key])
            #        self.buffer[key].clear()

    def get_data(self, key, field, since, until, count, where=None):
        if key not in self.get_keys(): return []

        t_filter = {"t": {"$gte": since, "$lte": until}, "deleted_": {"$not": True}}

        if field != "*":
            or_filter = []
            if isinstance(field, str): field = [field]
            for f in field:
                or_filter.append({f: {"$exists": True}})

        if where is not None: f_filter = {"$and": [t_filter, {"$or": or_filter}, where]}
        else: f_filter = {"$and": [t_filter, {"$or": or_filter}]}

        collection = self.client[self.database][key]
        found = collection.find(f_filter).sort("t", pymongo.DESCENDING).limit(count)

        #result = [{x.replace("_", "."): y[x] for x in y if x != "_id" and x == "t" ir x == "t_" or x == "id_" or field == "*" or x.replace("_", ".") in field } for y in found]

        result = []
        for item in found:
            nitem = {}
            for x in item:
                if x == "_id": continue
                if field == "*" or x == "t" or x == "id_" or x.replace("_", ".") in field:
                    nitem[x.replace("_", ".")] = item[x]

            result.append(nitem)

        return result

    def get_data_by_id(self, key, id_):
        if key not in self.get_keys(): return None

        field_filter = {"id_": id_}
        collection = self.client[self.database][key]

        found = collection.find_one(field_filter)
        result = {x.replace("_", "."): found[x] for x in found if x != "_id"}
        return result

    def delete_data(self, key, since, until):
        t_filter = {"t": {"$gte": since, "$lte": until}}
        coll = self.client[self.database][key]
        q = coll.delete_many(t_filter)
        return q.deleted_count

    def delete_data_by_id(self, key, id_):
        coll = self.client[self.database][key]
        q = coll.delete_one({"id_": id_})
        return q.deleted_count

    # ==========================================================================
    # Get keys and fields
    # ==========================================================================

    def get_keys(self):
        collections = [x for x in list(self.client[self.database].list_collection_names()) if x != "metadata"]
        collections.sort()
        return collections

    def get_fields(self, key):
        coll = self.client[self.database]["metadata"]
        fields = coll.find({'key': key})
        return [x["field"] for x in fields]

    def remove_key(self, key):
        self.client[self.database][key].drop()
        self.client[self.database]["metadata"].delete_many({'key': key})
        return

    def remove_field(self, key, field):
        self.client[self.database]["metadata"].delete_one({'key': key, 'field': field.replace(".", "_")})
        return

    def rename_key(self, key, new_key):
        self.client[self.database][key].rename(new_key)
        self.client[self.database]["metadata"].update_many({'key': key}, {'$set': {'key': new_key}})
        return

    def rename_field(self, key, field, new_field):
        self.client[self.database][key].update_many({}, {'$rename': {"name." + field.replace('.', '_'): "name." + new_field.replace('.', '_')}})
        self.client[self.database]["metadata"].update_one({'key': key, 'field': field}, {"$set": {'field': new_field}})
        return

    # ==========================================================================
    # Metadata
    # ==========================================================================

    def get_metadata(self, key):
        r = list(self.client[self.database]["metadata"].find({'key': key}))
        for x in r: del x["_id"]
        return r

    def set_metadata(self, key, field, alias, description):
        self.client[self.database]["metadata"].update({'key': key, 'field': field}, {'$set': {'alias': alias, 'description': description}}, upsert=True)
        return

    # ==========================================================================
    # Other
    # ==========================================================================

    def count_all_records(self):
        keys = self.get_keys()

        all = {}
        total = 0
        for key in keys:
            count = self.client[self.database][key].count()
            total += count
            all[key] = self.client[self.database][key].count()

        all["__total__"] = total
        return all
