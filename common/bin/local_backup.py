import os
from .logger import Log
from .db_connections_manager import DBConnectionsManager
from .root_folder import aleph_root_folder

from .db_connections.sqlite import SqliteConnection


class LocalBackup(DBConnectionsManager):

    def __init__(self):
        self.connections = {
            "main": SqliteConnection(os.path.join(aleph_root_folder, "local", "backup", "backup.db"))
        }
        self.log = Log('local_backup.txt')
        return
