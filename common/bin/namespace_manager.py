"""
Namespace Manager
-----------------

The namespace manager is the interface between data and the databases. Use the
namespace manager to save data to the database and perform simple queries.

The namespace manager can handle many DBMS. See the db_connections folder to
see the files that connect different types of databases.

Modify the namespace manager to use the database system you want. By default,
the namespace manager uses an SQLite connection.

"""

import traceback
import json
import datetime
import os
from dateutil.tz import tzutc, tzlocal

from .db_connections.sqlite import SqliteConnection
from .db_connections.postgres import PostgresConnection
from .logger import Log
from .root_folder import aleph_root_folder


class NamespaceManager:

    def __init__(self):

        # Get credentials if needed
        f = open(os.path.join(aleph_root_folder, "local", "config", "db_auth.json"))
        cred = json.loads(f.read())
        f.close()

        # Create the database connections
        # self.connections must be a dict {name: connection}
        # The connection used to get keys and fields must be named "main"
        # There always has to be a "main" connection
        self.connections = {
            "main": SqliteConnection(os.path.join(aleph_root_folder, "local", "backup", "main.db"))
        }

        # Log
        self.log = Log('namespace_manager_log.txt')

        # Open connections
        self.connect()

    def connect(self):
        for conn in self.connections: self.connections[conn].connect()
        return

    def close(self):
        for conn in self.connections: connections[conn].close()
        return

    def get_namespace(self):
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

    def __flatten_dict__(self, source,  result={}, separator=".", root=''):
        for key in source:

            new_root = root + separator + key
            if root == '':
                new_root = key

            if isinstance(source[key], dict):
                self.__flatten_dict__(source[key], result, separator, new_root)
            else:
                result[new_root] = source[key]
        return

    def save_data(self, key, data):
        try:
            # Check that "t" value is present
            if "t" not in data: raise Exception("Data missing 't' (timestamp) field")

            # Change time from local to UTC
            if not data["t"].endswith("Z"):
                data["t"] = datetime.datetime.strptime(data["t"], "%Y-%m-%d %H:%M:%S").astimezone(tzutc())
                data["t"] = data["t"].strftime("%Y-%m-%dT%H:%M:%SZ")

            # Replace slashes with dot on key and flatten the data on a 1dim dict
            key = key.replace("/", ".")
            flattened_data = {}
            self.__flatten_dict__(data, flattened_data, ".", '')

            # Save to the database
            self.connections["main"].save_to_database(key, flattened_data)

            # ADD MORE CODE BETWEEN THE LINES IF YOU NEED TO
            # ----------------------------------------------

            # ----------------------------------------------

        except Exception as e:
            error_as_string = ''.join(traceback.format_exception(None, e, e.__traceback__))
            self.log.write("Error saving data (key=" + key + "). " + str(e) + " \n\n" + str(data) + "\n\n" + error_as_string)

        return

    def get_data(self, key, field="*", since=365, until=0, count=100000):
        """
        returns data from the database (as a list of dict).
        - key (string): is the key in the namespace
        - field (string): a single field or "*" for all fields
        - since (int) and until (int): represent the time frame where we want to search,
          where both represent the number of days since the present day
          (for example, since=14 and until=7 represents the previous week)
        - count (int): how many records we want

        if you want to query for multiple fields, you need to call the function multiple
        times or get all fields and filter later the ones you are interested in. We do
        this to avoid problems where not all fields are present in all records.
        """

        try:
            key = key.replace("/", ".")
            data = self.connections["main"].get_from_database(key, field, since, until, count)

            # ADD MORE CODE BETWEEN THE LINES IF YOU NEED TO
            # ----------------------------------------------

            # ----------------------------------------------

            return data

        except Exception as e:
            error_as_string = ''.join(traceback.format_exception(None, e, e.__traceback__))
            self.log.write("Error retrieving data (key=" + key + "). " + str(e) + "\n\n" + error_as_string)
            return []

    def get_data_by_date(self, key, field="*", since='', until='', count=100000):
        """
        Returns data from the database (as a list of dict).
        Same as get_data but the parameters since and until are dates expressed as
        strings "%Y-%m-%d" (for example "2000-01-01").
        """
        if since == "":
            since = datetime.datetime.today().strftime("%Y-%m-%d")
        if until == "":
            until = datetime.datetime.today().strftime("%Y-%m-%d")

        try:
            since = datetime.datetime.strptime(since, "%Y-%m-%d").astimezone(tzutc())
            until = datetime.datetime.strptime(until, "%Y-%m-%d").astimezone(tzutc()) + datetime.timedelta(days=1)
            today = datetime.datetime.today().astimezone(tzutc())
            if since >= until or since > today:
                return []

            data = self.get_data(key, field=field, since=(today - since).days + 1, until=(today - until).days, count=count)
            mdata = []
            for d in data:
                if d["t"].astimezone(tzlocal()) < since: continue
                if d["t"].astimezone(tzlocal()) >= until: continue
                mdata.append(d)

            return mdata

        except Exception as e:
            error_as_string = ''.join(traceback.format_exception(None, e, e.__traceback__))
            self.log.write("Error retrieving data (key=" + key + "). " + str(e) + "\n\n" + error_as_string)
            return []

    def get_data_by_id(self, key, id_):
        """
        returns data from the database by id_ (as a dict)
        - key (string): the key on the namespace
        - id_ (string): the id_
        """

        try:
            key = key.replace("/", ".")
            data = self.connections["main"].get_from_database_by_id(key, id_)

            # ADD MORE CODE BETWEEN THE LINES IF YOU NEED TO
            # ----------------------------------------------

            # ----------------------------------------------

            return data
        except Exception as e:
            error_as_string = ''.join(traceback.format_exception(None, e, e.__traceback__))
            self.log.write("Error retrieving data by id (key=" + key + ", id_=" + id_ + "). " + str(
                e) + "\n\n" + error_as_string)
            raise

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
                raise

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
                raise

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
                raise

        return removed_success

    def rename_key(self, key, new_key):
        key = key.replace("/", ".")
        new_key = new_key.replace("/", ".")

        namespace = self.get_namespace()
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
                raise

    def rename_field(self, key, field, new_field):
        key = key.replace("/", ".")

        rename_success = False
        for conn in self.connections:
            try:
                if key in self.connections[conn].get_all_keys():
                    self.connections[conn].rename_field(key, field, new_field)
                    if conn == "main": rename_success = True

            except Exception as e:
                self.log.write(f"Could not rename field on mongodb ({str(e)})")
                raise
        return rename_success
