"""

"""
import serial


class SerialConnection:

    def __ init__(self, port, read_function):
        self.port = port
        self.read_function = read_function
        self.connected = False

        self.baudrate = 9600
        self.parity = serial.PARITY_NONE
        self.stopbits = serial.STOPBITS_ONE
        self.timeout = 1
        self.bytesize = serial.EIGHTBITS

    def connect(self):
        try:
            self.serial = serial.Serial(self.port,
                                        self.baudrate,
                                        parity=self.parity,
                                        stopbits=self.stopbits,
                                        timeout=self.timeout,
                                        bytesize=self.bitesize)
            self.connected = True
        except:
            self.connected = False

    def do(self):
        if not self.connected: raise Exception("Not connected")
        x = self.serial.readline()
        if len(x) > 0:
            x = x.decode()
            return self.read_function(x)
