"""
Different ways to store key:value pairs on the local storage
Safe methods must fail silently
"""
import json


# ===================================================================================
# Parent Class
# ===================================================================================
class LocalStorage:

    class Pre:
        SNF_BUFFER = "_snf_buffer"
        LAST_RECORD_SENT = "_last_record"
        LAST_TIME_READ = "_last_time"
        PAST_VALUES = "_past_values"

    def __init__(self):
        self.data = {}
        self.prefix = ""
        self.safe_load()

    def pkey(self, key):
        return self.prefix + key

    def load(self):
        pass

    def get(self, key, null_value=None):
        key = self.pkey(key)
        if key not in self.data: return null_value
        return self.data[key]

    def set(self, key, value):
        key = self.pkey(key)
        self.data[key] = value

    def safe_load(self):
        try:
            self.load()
        except:
            pass

    def safe_get(self, key, null_value=None):
        try:
            return self.get(key, null_value)
        except:
            key = self.pkey(key)
            if key not in self.data: return null_value
            return self.data[key]

    def safe_set(self, key, value):
        try:
            self.set(key, value)
        finally:
            key = self.pkey(key)
            self.data[key] = value


# ===================================================================================
# File Storage
# ===================================================================================
class FileLocalStorage(LocalStorage):

    def __init__(self, file):
        self.file = file
        super().__init__()

    def load(self):
        f = open(self.file)
        self.data = json.loads(f.read())
        f.close()

    def get(self, key, null_value=None):
        return super().get(key, null_value)

    def set(self, key, value):
        f = open(self.file, "w+")
        f.write(json.dumps(self.data))
        f.close()
        return super().set(key, value)


# ===================================================================================
# Sqlite Dict Storage (Uses pickle and sqlite)
# ===================================================================================
class SqliteDictLocalStorage(LocalStorage):
    def __init__(self, file):
        self.file = file
        self.sqlitedict = None
        super().__init__()

    def load(self):
        from sqlitedict import SqliteDict
        self.sqlitedict = SqliteDict(self.file, autocommit=True)

    def get(self, key, null_value=None):
        return self.sqlitedict[self.pkey(key)]
        
    def set(self, key, value):
        self.sqlitedict[self.pkey(key)] = value
        

# ===================================================================================
# Redis Storage
# ===================================================================================
class RedisLocalStorage(LocalStorage):

    def __init__(self):
        self.red = None
        super().__init__()

    def load(self):
        import redis
        self.red = redis.Redis(host='localhost', port=6379, db=0)

    def get(self, key, null_value=None):
        return self.red.get(self.pkey(key))

    def set(self, key, value):
        self.red.set(self.pkey(key), value)
