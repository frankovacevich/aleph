from ..connection import Connection


# ===================================================================================
# MongoDB
# ===================================================================================
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
        import pymongo
        import urllib.parse

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


# ===================================================================================
# MariaDB / MySQL
# ===================================================================================
class MySQLInterfaceConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.server = "localhost"
        self.username = ""
        self.password = ""
        self.port = 3306
        self.database = "main"

        # Private
        self.client = None

    def open(self):
        import mysql.connector
        self.client = mysql.connector.connect(host=self.server,
                                              port=self.port,
                                              database=self.database,
                                              username=self.username,
                                              password=self.password)
        super().open()

    def close(self):
        self.client.close()
        super().close()


# ===================================================================================
# Sqlite
# ===================================================================================
class SqliteInterfaceConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.file = "./main.db"

        # Private
        self.client = None

    def open(self):
        import sqlite3
        self.client = sqlite3.connect(self.file)
        super().open()

    def close(self):
        self.client.close()
        super().close()


# ===================================================================================
# Postgres
# ===================================================================================
class PostgresInterfaceConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.server = "localhost"
        self.username = ""
        self.password = ""
        self.port = 5432
        self.database = "main"

        # Private
        self.client = None

    def open(self):
        import psycopg2
        self.client = psycopg2.connect(database=self.database,
                                       user=self.username,
                                       password=self.password,
                                       host=self.server,
                                       port=self.port
                                       )
        super().open()

    def close(self):
        self.client.close()
        super().close()
