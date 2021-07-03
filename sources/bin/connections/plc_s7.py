"""
Use this connection to read data from a Siemens S7 PLC.

To install the snap7 package use 'pip install python-snap7'
You may need to install the repositories:
'sudo add-apt-repository ppa:gijzelaar/snap7'
'sudo apt-get update'
'sudo apt-get install libsnap7-1 libsnap7-dev'

Read the documentation to learn how to use this connection. You'll need to create
a read_function that reads the Tags you want from the PLC.
"""


class PLCS7Connection:

    def __init__(self, ip_address, read_function):
        self.ip_address = ip_address
        self.read_function = read_function
        self.connected = False
        self.connection = None

    def connect(self):
        import snap7
        self.connection = snap7.client.Client()
        self.connection.connect(self.ip_address, 0, 1)
        self.connected = True

    def do(self):
        if not self.connected: raise Exception("Not connected")
        return self.read_function(self.connection)
