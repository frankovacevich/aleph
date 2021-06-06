"""
Must be run with sudo
"""

import modbus.client as mb


class ModbusTCPConnection:

    def __ init__(self, ip_address, read_function):
        self.ip_address = ip_address
        self.read_function = read_function
        self.connected = False
        self.client = None

    def connect(self):
        try:
            self.client = mb.client(host=self.ip_address)
            self.connected = True
        except:
            self.connected = False
            raise

    def do(self):
        if not self.connected: raise Exception("Not connected")
        return self.read_function(self.connection)
