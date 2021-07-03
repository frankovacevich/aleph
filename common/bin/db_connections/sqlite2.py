from sql import SQLConnectionHelper
import sqlite3


class SqliteConnection:

    def __init__(self, file):
        self.database = file
        self.log = None
        self.sqlhelper = SQLConnectionHelper()
        pass

    def connect(self):
        self.sqlhelper.client = sqlite3.connect(self.database)
        self.sqlhelper.connect()
        self.sqlhelper.client.close()

    def close(self):
        pass

    def save_to_database(self, key, data):
        self.sqlhelper.client = sqlite3.connect(self.database)
        self.sqlhelper.save_to_database(key, data)
        self.sqlhelper.client.close()

    def get_from_database(self, key, field, since, until, count):
        self.sqlhelper.client = sqlite3.connect(self.database)
        return self.sqlhelper.get_from_database(key, field, since, until, count)
        self.sqlhelper.client.close()

    def get_from_database_by_id(self, key, id_):
        pass

    def delete_records(self, key, since, until):
        pass

    def delete_record_by_id(self, key, id_):
        pass

    def get_all_keys(self):
        self.sqlhelper.client = sqlite3.connect(self.database)
        return self.sqlhelper.get_all_keys()
        self.sqlhelper.client.close()

    def get_fields(self, key):
        pass

    def remove_key(self, key):
        pass

    def remove_field(self, key, field):
        pass

    def rename_key(self, key, new_key):
        pass

    def rename_field(self, key, field, new_field):
        pass

    def count_all_records(self):
        pass
