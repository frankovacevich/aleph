import os
import json
import datetime
from dateutil.tz import tzutc, tzlocal
from .logger import Log
from .db_connections_manager import DBConnectionsManager
from .root_folder import aleph_root_folder

from .db_connections.sqlite import SqliteConnection
from .db_connections.mariadb import MariaDBConnection
from .db_connections.mongodb import MongoDBConnection
from .db_connections.influxdb import InfluxDBConnection


class NamespaceManager(DBConnectionsManager):

    def __init__(self):
        # Get credentials from file
        self.connections = {
            "main": SqliteConnection(os.path.join(aleph_root_folder, "local", "backup", "main.db")),
        }
        self.log = Log('namespace_manager.txt')
        self.connect()
        return

    def on_save_data(self, key, data):
        self.connections["main"].save_to_database(key, data)

    def on_get_data(self, key, field, since, until, count, ffilter):
        return self.connections["main"].get_from_database(key, field, since, until, count, ffilter)

    def on_get_data_by_id(self, key,id_):
        return self.connections["main"].get_from_database_by_id(key, id_)

    ##

    def set_metadata(self, key, field, alias, description) :
        pass

    def get_metadata(self, key, field):
        pass
