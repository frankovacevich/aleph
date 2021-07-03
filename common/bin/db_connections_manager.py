"""
Namespace Manager
-----------------

The namespace manager is the interface between data and the databases. Use the
namespace manager to save data to the database and perform simple queries.

The namespace manager can handle many DBMS. See the db_connections folder to
see the files that connect different types of databases.

Modify the namespace manager to use the database system you want. By default,
the namespace manager uses an SQLite connection.

connect()

close()

get_keys()

get_fields(key)

save_data(key, data)

get_data(key, field, since, until, count, ffilter)

get_data_by_id(key, id_)

delete_data(key, since, until)

remove_key(key)

remove_field(key, field)

rename_key(key, new_key)

rename_field(key, field, new_field)

backup_save(since, until, file_name)

backup_restore(file_name)

"""

import traceback
import json
import datetime
import os
from dateutil.tz import tzutc, tzlocal
from dateutil import parser
from .logger import Log
from .root_folder import aleph_root_folder


class DBConnectionsManager:

    def __init__(self):
        # Create the database connections
        # self.connections must be a dict {name: connection}
        # The connection used to get keys and fields must be named "main"
        # There always has to be a "main" connection
        self.connections = {}

        # Log
        self.log = Log("")

        # Open connections
        self.connect()

    def connect(self):
        for conn in self.connections: self.connections[conn].connect()

    def close(self):
        for conn in self.connections: self.connections[conn].close()

    def get_keys(self):
        keys = self.connections["main"].get_all_keys()
        return keys

        try:
            keys = self.connections["main"].get_all_keys()
            return keys
        except Exception as e:
            error_as_string = ''.join(traceback.format_exception(None, e, e.__traceback__))
            self.log.write("Error getting namespace. " + str(e) + "\n\n" + error_as_string)
            return []

    def get_fields(self, key):
        try:
            key = key.replace("/", ".")
            fields = self.connections["main"].get_fields(key)
            return fields
        except Exception as e:
            error_as_string = ''.join(traceback.format_exception(None, e, e.__traceback__))
            self.log.write("Error getting fields (key=" + key + "). " + str(e) + "\n\n" + error_as_string)
            return []

    def __flatten_dict__(self, source, result={}, separator=".", root=''):
        for key in source:
            new_root = root + separator + key
            if root == '':
                new_root = key
            if isinstance(source[key], dict):
                self.__flatten_dict__(source[key], result, separator, new_root)
            else:
                result[new_root] = source[key]
        return

    def on_save_data(self, key, data):
        self.connections["main"].save_to_database(key, data)

    def save_data(self, key, data):
        try:
            # Check that "t" value is present
            if "t" not in data: raise Exception("Data missing 't' (timestamp) field")

            # Change time from local to UTC
            if isinstance(data["t"], datetime.datetime):
                data["t"] = data["t"].astimezone(tzutc()).strftime("%Y-%m-%dT%H:%M:%SZ")
            elif not data["t"].endswith("Z"):
                data["t"] = parser.parse(data["t"]).astimezone(tzutc()).strftime("%Y-%m-%dT%H:%M:%SZ")

            # Replace slashes with dot on key and flatten the data on a 1dim dict
            key = key.replace("/", ".")
            flattened_data = {}
            self.__flatten_dict__(data, flattened_data, ".", '')

            # Save to the database
            self.on_save_data(key, flattened_data)

        except Exception as e:
            error_as_string = ''.join(traceback.format_exception(None, e, e.__traceback__))
            self.log.write("Error saving data (key=" + key + "). " + str(e) + " \n\n" + str(data) + "\n\n" + error_as_string)

        return

    def on_get_data(self, key, field, since, until, count, ffilter):
        return self.connections["main"].get_from_database(key, field, since, until, count, ffilter)

    def get_data(self, key, field="*", since=365, until=0, count=100000, ffilter=None):
        """
        returns data from the database (as a list of dict).
        - key (string): is the key in the namespace
        - field (string): a single field or "*" for all fields
        - since and until: represent the time frame where we want to search,
          they can be:
          - ints: number of days from the present day
          - strings: representing datetimes (in UTC or local time)
          - datetime
        - count (int): how many records we want

        if you want to query for multiple fields, you need to call the function multiple
        times or get all fields and filter later the ones you are interested in. We do
        this to avoid problems where not all fields are present in all records.
        """

        try:
            key = key.replace("/", ".")

            # transform since and until into datetimes
            if isinstance(since, int):
                since = datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=since)
            elif since == "":
                since = datetime.datetime.today().replace(hour=0, minute=0, second=0).astimezone(tzutc())
            elif isinstance(since, str):
                since = parser.parse(since).astimezone(tzutc())
            elif isinstance(since, datetime.datetime):
                since = since.astimezone(tzutc())

            if isinstance(until, int):
                until = datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=until)
            elif until == "":
                until = datetime.datetime.now().replace(hour=23, minute=59, second=59).astimezone(tzutc())
            elif isinstance(until, str):
                until = parser.parse(until)
                if until.hour == 0 and until.minute == 0 and until.second == 0:
                    until = until.replace(hour=23, minute=59, second=59)
                until = until.astimezone(tzutc())
            elif isinstance(until, datetime.datetime):
                until = until.astimezone(tzutc())

            return self.on_get_data(key, field, since, until, count, ffilter)

        except Exception as e:
            error_as_string = ''.join(traceback.format_exception(None, e, e.__traceback__))
            self.log.write("Error retrieving data (key=" + key + "). " + str(e) + "\n\n" + error_as_string)
            return []

    def on_get_data_by_id(self, key, id_):
        return self.connections["main"].get_from_database_by_id(key, id_)

    def get_data_by_id(self, key, id_):
        """
        returns data from the database by id_ (as a dict)
        - key (string): the key on the namespace
        - id_ (string): the id_
        """

        try:
            key = key.replace("/", ".")
            return self.on_get_data_by_id(key, id_)

        except Exception as e:
            error_as_string = ''.join(traceback.format_exception(None, e, e.__traceback__))
            self.log.write("Error retrieving data by id (key=" + key + ", id_=" + id_ + "). " + str(
                e) + "\n\n" + error_as_string)
            return {}

    def delete_data(self, key, since, until):
        key = key.replace("/", ".")
        count_deleted = 0

        for conn in self.connections:
            try:
                if key in self.connections[conn].get_all_keys():
                    q = self.connections[conn].delete_records(key, since, until)
                    if conn == "main": count_deleted = q
            except Exception as e:
                self.log.write("Could not delete data (" + str(e) + ")")
                continue

        return count_deleted

    def remove_key(self, key):
        key = key.replace("/", ".")
        removed_success = False

        for conn in self.connections:
            try:
                if key in self.connections[conn].get_all_keys():
                    self.connections[conn].remove_key(key)
                    if conn == "main": removed_success = True
            except Exception as e:
                self.log.write("Could not remove key (" + str(e) + ")")
                continue

        return removed_success

    def remove_field(self, key, field):
        key = key.replace("/", ".")
        removed_success = False

        for conn in self.connections:
            try:
                if key in self.connections[conn].get_all_keys():
                    self.connections[conn].remove_field(key, field)
                    if conn == "main": removed_success = True
            except Exception as e:
                self.log.write("Could not remove field (" + str(e) + ")")
                continue

        return removed_success

    def rename_key(self, key, new_key):
        key = key.replace("/", ".")
        new_key = new_key.replace("/", ".")

        namespace = self.get_keys()
        if new_key in namespace:
            raise Exception("There's already another key with at '" + new_key + "')")

        rename_success = False
        for conn in self.connections:
            try:
                if key in self.connections[conn].get_all_keys():
                    self.connections[conn].rename_key(key, new_key)
                    if conn == "main": rename_success = True
            except Exception as e:
                self.log.write(f"Could not rename key on mongodb ({str(e)})")
                continue

    def rename_field(self, key, field, new_field):
        key = key.replace("/", ".")

        rename_success = False
        for conn in self.connections:
            try:
                if key in self.connections[conn].get_all_keys():
                    self.connections[conn].rename_field(key, field, new_field)
                    if conn == "main": rename_success = True

            except Exception as e:
                if self.log is not None:
                    self.log.write(f"Could not rename field on mongodb ({str(e)})")
                continue

        return rename_success

    def backup_save(self, since=365, until=0, file_name="backup.pickle"):
        import lzma
        import pickle
        """

        """

        backup_file = lzma.open(os.path.join(aleph_root_folder, "local", "backup", file_name), "wb")
        self.log.write("Saving backup on " + file_name)
        total = 0

        keys = self.get_keys()
        for key in keys:
            print("Backing up records for", key)
            for u in range(until, since):
                data = self.get_data(key, "*", u + 1, u)
                if len(data) > 0:
                    total += len(data)
                    pickle.dump(data, backup_file)
                continue

        backup_file.close()
        self.log.write("Backup completed successfully. Total: " + str(total) + " records.")

    def backup_restore(self, file_name="backup.pickle"):
        """

        """
        import pickle
        import lzma
        import time
        t0 = time.time()
        self.log.write("Restoring backup from " + file_name)

        with lzma.open(os.path.join(aleph_root_folder, "local", "backup", file_name), "rb") as backup_file:
            while True:
                try:
                    chunk = pickle.load(backup_file)
                    for key in chunk:
                        print("Restoring data for", key, "from", chunk[key][-1]["t"], "to", chunk[key][0]["t"])
                        for record in chunk[key]:
                            self.save_data(key, record)

                except EOFError:
                    self.log.write("Backup restore completed successfully")
                    break

        print(time.time() - t0)
