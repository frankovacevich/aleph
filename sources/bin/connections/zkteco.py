"""
Use this connection to get attendance from a zkteco machine
"""

from zk import ZK, const


class ZKTecoConnection:

    def __init__(self, ip_address, port=4370):
        self.ip_address = ip_address
        self.port = port

        self.connected = False
        self.zk = None
        self.conn = None
        self.last_t = None

    def connect(self):
        self.zk = ZK(self.ip_address, port=self.port)
        self.conn = self.zk.connect()
        self.connected = True

    def do(self):
        if not self.connected: raise Exception("Not connected")
        attendance = self.conn.get_attendance()
        res = []
        for a in attendance:
            if self.last_t is not None and a.timestamp < self.last_t: continue
            res.append({
                "t": a.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "user_id": a.user_id,
                "punch": a.punch,
                "status": a.status
            })

        self.last_t = attendance[-1].timestamp
        return res
