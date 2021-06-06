import os
from .logger import Log
from .db_connections.sqlite import SqliteConnection
from .namespace_manager import NamespaceManager
from .root_folder import aleph_root_folder

class LocalBackup(NamespaceManager):

    def __init__(self, file="backup"):

        self.connections = {
            "main": SqliteConnection(os.path.join(aleph_root_folder, "local", "backup", "backup.db"))
        }

        self.log = Log('local_backup.txt')

        return
