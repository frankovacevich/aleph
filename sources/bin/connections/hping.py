"""
Use this connection to ping a single device to see if it's connected to the
network.

You can also us it to check if a device is listening on some port

If you need to ping more than one device, use threading.
                                              ---------
"""

import socket
import os


class HPingConnection:

    def __init__(self, ip_address=None, port=None):
        self.connected = False
        self.ip = ip_address
        self.port = port

        self.device_name = ip_address.replace(".", "_")
        if port is not None:
            self.device_name += "__" + str(port)

        self.device_state = True

    def connect(self):
        self.connected = True

    def __do_ping__(self, ip, port=None):
        if port is None:
            result = os.system("ping -c 1 " + ip + " > /dev/null 2>&1") == 0
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex((ip, port)) == 0
            sock.close()
        return result

    def do(self):
        if not self.connected:
            raise Exception("Not connected")

        if self.device_state:
            for i in range(0, 7):
                r = self.__do_ping__(self.ip, self.port)
                if r:
                    self.device_state = True
                    break
                else:
                    self.device_state = False
                    continue

        else:
            r = self.__do_ping__(self.ip, self.port)
            if r:
                self.device_state = True
            else:
                self.device_state = False

        return {self.device_name: self.device_state}
