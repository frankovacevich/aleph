"""
Use this connection to read data from an Allen Bradley PLC (compatible with
Compactlogix, Controllogix, Micrologix and SLC models).

=====================================
== CAUTION! It requires Python 2.7 ==
=====================================

To install the pycomm library use 'pip install pycomm'

Read the documentation to learn how to use this connection. You'll need to create
a read_function that reads the Tags you want from the PLC.
"""

class PLCABConnection:

    def __ init__(self, model, ip_address, read_function):
        self.model = model
        self.ip_address = ip_address
        self.read_function = read_function

        self.connected = False
        self.connection = None

        ##
        MODELS = [
            "Compactlogix",
            "Controllogix",
            "Micrologix",
            "SLC",
        ]

        if model not in MODELS: raise Exception("Invalid PLC Model")

    def connect(self):
        if self.model == MODELS[0] or self.model == MODELS[1]:
            from pycomm.ab_comm.clx import Driver as driver
        elif self.model == MODELS[2] or self.model == MODELS[3]:
            from pycomm.ab_comm.slc import Driver as driver
        else:
            self.connected = False
            return

        self.connection = driver()
        self.connection.open(self.ip_address)
        self.connected = True

    def do(self):
        if not self.connected: raise Exception("Not connected")
        return self.read_function(self.connection)
