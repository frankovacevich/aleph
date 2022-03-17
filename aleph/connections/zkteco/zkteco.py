from ..connection import Connection
from zk import ZK


class ZKTecoConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)

        # Connection parameters
        self.ip_address = "192.168.0.99"
        self.port = 4370

        # Internal
        self.persistent = True  # Important!
        self.zk = None
        self.conn = None

    # ===================================================================================
    # Open and close
    # ===================================================================================
    def open(self):
        self.zk = ZK(self.ip_address, port=self.port)
        self.conn = self.zk.connect()
        super().open()

    def close(self):
        if self.conn is not None: self.conn.disconnect()
        self.conn = None
        super().close()

    def is_connected(self):
        if self.conn is None: return False
        return self.conn.is_connect

    # ===================================================================================
    # Read
    # ===================================================================================
    def read(self, key, **kwargs):
        if key == "attendance": return self.read_attendance(**kwargs)
        if key == "users": return self.read_users(**kwargs)
        if key == "system": return self.read_system(**kwargs)
        return []

    def read_attendance(self, **kwargs):
        attendance = self.conn.get_attendance()
        result = []
        for a in attendance:
            result.append({
                "t": a.timestamp,
                "user_id": a.user_id,
                "punch": a.punch,
                "status": a.status,
            })
        return result

    def read_users(self, **kwargs):
        kwargs["compare_to_previous"] = True
        users = self.conn.get_users()
        result = []
        for user in users:
            result.append({
                "id_": user.user_id,
                "name": user.name,
                "group_id": user.group_id,
            })

    def read_system(self, **kwargs):
        pass

    # ===================================================================================
    # Write
    # ===================================================================================
    def write(self, key, data):
        if key == "time":
            self.write_time()

    def write_time(self, key, data):
        pass
