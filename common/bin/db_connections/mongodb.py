import pymongo
import time
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
        self.buffer = []

        ##
        self.client = None
        return

    def connect(self):
        self.client = pymongo.MongoClient("mongodb://" + urllib.parse.quote(self.username) + ":" +
                                          urllib.parse.quote(self.password) + "@" + self.server + ":" + str(self.port))
        return

    def close(self):
        self.client.close()

    def __prepare_data_for_saving__(self, data, result={}, prefix=""):
        for f in data:
            if type(data[f]) == dict:
                self.__prepare_data_for_saving__(data[f], result, prefix + f + ".")
            else:
                result[prefix + f] = data[f]
        return result

    def __update_fields__(self, key, fields):
        coll = self.client[self.database]["__db_fields__"]
        for field in fields:
            updt = {'key': key, 'field': field}
            if coll.find_one(updt) is None: coll.insert_one(updt)
        return

    def save_to_database(self, key, data):
        coll = self.client[self.database][key]
        # dat = data.copy()

        dat = {}
        for field in data:
            if data[field] == float('inf'): continue
            dat[field.replace(".", "[dot]")] = data[field]

        self.__update_fields__(key, data)
        dat["t"] = datetime.datetime.strptime(dat["t"], '%Y-%m-%dT%H:%M:%SZ')

        if "id_" in data:
            # coll.update_one({"id_": dat["id_"]}, {"$set": dat}, upsert=True)
            if coll.find_one({"id_": dat["id_"]}) is None:
                coll.insert_one(dat)
            else:
                dat["t_"] = dat["t"]
                del dat["t"]
                coll.update_one({"id_": dat["id_"]}, {"$set": dat})

        else:
            self.buffer.append(dat)
            if len(self.buffer) >= self.buffer_size:
                coll.insert_many(self.buffer)
                self.buffer.clear()

    def get_from_database(self, key, field, since, until, count, ffilter):
        if key not in self.get_all_keys(): return []
        # since = datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=since)
        # until = datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=until)

        field_filter = {"t": {"$gte": since, "$lte": until}}

        if field != "*":
            field_filter[field.replace(".", "[dot]")] = {"$exists": True}

        collection = self.client[self.database][key]
        found = collection.find(field_filter).sort("t", pymongo.DESCENDING).limit(count)

        result = []
        for item in found:
            nitem = {}
            for x in item:
                if x == "_id":
                    continue
                if field == x.replace("[dot]", ".") or field == "*" or x == "t" or x == "id_":
                    nitem[x.replace("[dot]", ".")] = item[x]

            result.append(nitem)

        return result

    def get_from_database_by_id(self, key, id_):
        if key not in self.get_all_keys(): return None

        field_filter = {"id_": id_}
        collection = self.client[self.database][key]

        found = collection.find_one(field_filter)

        result = {}
        for x in found:
            if x == "_id":
                continue
            result[x.replace("[dot]", ".")] = found[x]

        return result

    def delete_records(self, key, since, until):
        since = datetime.datetime.now() - datetime.timedelta(days=since)
        until = datetime.datetime.now() - datetime.timedelta(days=until)
        qfilter = {"t": {"$gte": since, "$lte": until}}
        coll = self.client[self.database][key]
        q = coll.delete_many(qfilter)
        return q.deleted_count

    def delete_record_by_id(self, key, id_):
        coll = self.client[self.database][key]
        q = coll.delete_one({"id_": id_})
        return q.deleted_count

    def get_all_keys(self):
        collections = [x for x in list(self.client[self.database].list_collection_names()) if x != "__db_fields__"]
        collections.sort()
        return collections

    def get_fields(self, key):
        coll = self.client[self.database]["__db_fields__"]
        fields = coll.find({'key': key})
        return [x["field"] for x in fields]

    def remove_key(self, key):
        self.client[self.database][key].drop()
        self.client[self.database]["__db_fields__"].delete_many({'key': key})
        return

    def remove_field(self, key, field):
        self.client[self.database]["__db_fields__"].delete_one({'key': key, 'field': field.replace(".", "[dot]")})
        return

    def rename_key(self, key, new_key):
        self.client[self.database][key].rename(new_key)
        self.client[self.database]["__db_fields__"].update_many({'key': key}, {'$set': {'key': new_key}})
        return

    def rename_field(self, key, field, new_field):
        self.client[self.database][key].update_many({}, {'$rename': {f"name.{field.replace('.', '[dot]')}": f"name.{new_field.replace('.', '[dot]')}"}})
        self.client[self.database]["__db_fields__"].update_one({'key': key, 'field': field}, {"$set": {'field': new_field}})
        return

    def count_all_records(self):
        keys = self.get_all_keys()

        all = {}
        total = 0
        for key in keys:
            count = self.client[self.database][key].count()
            total += count
            all[key] = self.client[self.database][key].count()

        all["__total__"] = total
        return all
