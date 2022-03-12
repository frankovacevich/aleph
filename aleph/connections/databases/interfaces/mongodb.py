from ...connection import Connection
import pymongo
import urllib.parse


class MongoDBInterfaceConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.server = "localhost"
        self.port = 27017
        self.username = None
        self.password = None
        self.database = "main"

        # Client
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
