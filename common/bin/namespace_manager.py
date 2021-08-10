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
from dateutil import parser
from .logger import Log
from .root_folder import aleph_root_folder
from .db_connections import functions as fn
from .db_connections.sqlite import SqliteConnection
from .db_connections.mariadb import MariaDBConnection
from .db_connections.mongodb import MongoDBConnection
from .db_connections.postgres import PostgresConnection
from .db_connections.influxdb import InfluxDBConnection


class NamespaceManager:

    def __init__(self):
        #self.conn = SqliteConnection(os.path.join(aleph_root_folder, "local", "backup", "msql.db"))
        self.conn = MariaDBConnection("admin", "password", "main")
        #self.conn = MongoDBConnection("al", "password", "main")
        #self.conn = PostgresConnection("admin", "password", "main")
        #self.conn = InfluxDBConnection("admin", "password", "main")
        self.log = Log("namespace_manager.log")

    # ==========================================================================
    # Connect and close
    # ==========================================================================

    def connect(self):
        self.conn.connect()

    def close(self):
        self.conn.close()

    # ==========================================================================
    # Operations (save, get, delete)
    # ==========================================================================

    def save_data(self, key, data):
        data = fn.__format_data_for_saving__(data)
        self.conn.save_data(key, data)

    def get_data(self, key, field="*", since=365, until=0, count=100000):
        since = fn.__parse_date__(since)
        until = fn.__parse_date__(until, True)
        return self.conn.get_data(key, field, since, until, count)

    def get_data_by_id(self, key, id_):
        return self.conn.get_data_by_id(key, id_)

    def delete_data(self, key, since, until):
        since = fn.__parse_date__(since)
        until = fn.__parse_date__(until, True)
        return self.conn.delete_data(key, since, until)

    def delete_data_by_id(self, key, id_):
        return self.conn.delete_data_by_id(key, id_)

    # ==========================================================================
    # Get keys and fields. Get and set metadata
    # ==========================================================================

    def get_keys(self):
        return self.conn.get_keys()

    def get_fields(self, key):
        return self.conn.get_fields(key)

    def set_metadata(self, key, field, alias, description=""):
        if field in ["t", "id", "id_", "t_"]: raise Exception("Invalid field")
        self.conn.set_metadata(key, field, str(alias), str(description))

    def get_metadata(self, key):
        return self.conn.get_metadata(key)

    # ==========================================================================
    # Remove and rename keys and fields
    # ==========================================================================

    def remove_key(self, key):
        self.conn.remove_key(key)

    def remove_field(self, key, field):
        if field in ["t", "id", "id_", "t_"]: raise Exception("Invalid field")
        self.conn.remove_field(key, field)

    def rename_key(self, key, new_key):
        self.conn.rename_key(key, new_key)

    def rename_field(self, key, field, new_field):
        if field in ["t", "id", "id_", "t_"]: raise Exception("Invalid field")
        self.conn.rename_field(key, field, new_field)

    # ==========================================================================
    # Backup save and restore
    # ==========================================================================

    def backup_save(self, since=365, until=0, file_name="backup.pickle"):
        import lzma
        import pickle

        since = fn.__parse_date__(since)
        until = fn.__parse_date__(until, True)

        backup_file = lzma.open(os.path.join(aleph_root_folder, "local", "backup", file_name), "wb")
        self.log.write("Saving backup on " + file_name)
        total = 0

        keys = self.get_keys()
        for key in keys:
            print("Backing up records for", key)
            s = since

            while s < until:
                u = s + datetime.timedelta(days=1)
                if u > until: u = until

                data = self.get_data(key, "*", s, u)
                if len(data) > 0:
                    total += len(data)
                    pickle.dump({key: data}, backup_file)

                s = s + datetime.timedelta(days=1)

        backup_file.close()
        self.log.write("Backup completed successfully. Total: " + str(total) + " records.")

    def backup_restore(self, file_name="backup.pickle"):
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
