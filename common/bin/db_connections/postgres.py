from .sql2 import SQLConnection
import psycopg2


class PostgresConnection(SQLConnection):

    def __init__(self, username, password, database, server="localhost", port=5432):
        self.server = server
        self.username = username
        self.password = password
        self.port = port
        self.database = database

        super().__init__("postgres")
        self.client = None


    # ==========================================================================
    # Connect and close
    # ==========================================================================

    def __client_connect__(self):
        self.client = psycopg2.connect(
            database=self.database,
            user=self.username,
            password=self.password,
            host=self.server,
            port=self.port
        )

    def __client_close__(self):
        #self.client.close()
        pass

    def connect(self):
        self.__client_connect__()
        super().connect()
        self.__client_close__()

    def close(self):
        return

    # ==========================================================================
    # Get keys and fields. Get and set metadata
    # ==========================================================================

    def get_keys(self):
        self.__client_connect__()
        return super().get_keys()
        self.__client_close__()
        return r

    def get_fields(self, key):
        self.__client_connect__()
        r = super().get_fields(key)
        self.__client_close__()
        return r

    def remove_key(self, key):
        self.__client_connect__()
        r = super().remove_key(key)
        self.__client_close__()
        return r

    def remove_field(self, key, field):
        self.__client_connect__()
        r = super().remove_field(key, field)
        self.__client_close__()
        return r

    def rename_key(self, key, new_key):
        self.__client_connect__()
        r = super().rename_key(key, new_key)
        self.__client_close__()
        return r

    def rename_field(self, key, field, new_field):
        self.__client_connect__()
        r = super().rename_field(key, field, new_field)
        self.__client_close__()
        return r

    def get_metadata(self, key):
        self.__client_connect__()
        r = super().get_metadata(key)
        self.__client_close__()
        return r

    def set_metadata(self, key, field, alias, description):
        self.__client_connect__()
        r = super().set_metadata(key, field, alias, description)
        self.__client_close__()
        return r

    def count_all_records(self):
        self.__client_connect__()
        r = super().count_all_records()
        self.__client_close__()
        return r

    # ==========================================================================
    # Operations (save, get, delete)
    # ==========================================================================

    def save_data(self, key, data):
        self.__client_connect__()
        super().save_data(key, data)
        self.__client_close__()

    def get_data(self, key, field, since, until, count):
        self.__client_connect__()
        r = super().get_data(key, field, since, until, count)
        self.__client_close__()
        return r

    def get_data_by_id(self, key, id_):
        self.__client_connect__()
        r = super().get_data_by_id(key, id_)
        self.__client_close__()
        return r

    def delete_data(self, key, since, until):
        self.__client_connect__()
        r = super().delete_data(key, since, until)
        self.__client_close__()
        return r

    def delete_data_by_id(self, key, id_):
        self.__client_connect__()
        r = super().delete_data_by_id(key, id_)
        self.__client_close__()
        return r

    def run_sql_query(self, query, return_fields=[]):
        self.__client_connect__()
        r = super().run_sql_query(query, return_fields)
        self.__client_close__()
        return r
