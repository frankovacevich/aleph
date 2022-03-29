from ..connection import Connection
from ping3 import ping
import socket


class PingConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.clean_on_read = False
        self.report_by_exception = True

    def read(self, key, **kwargs):
        """
        Returns True if there's ping to the given IP, else False
        The key is the IP address "X.X.X.X"
        You can also check if a port is open using "X.X.X.X:PORT"
        """
        if ":" in key:
            k = key.split(":")
            r = self.check_port(k[0], int(k[1]))
            key = key.replace(".", "_").replace(":", "__")
        else:
            r = self.ping(key)
            key = key.replace(".", "_")

        return {key: r}

    @staticmethod
    def ping(ip_address):
        result = ping(ip_address)
        return result is not None and result is not False

    @staticmethod
    def check_port(ip_address, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((ip_address, port)) == 0
        sock.close()
        return result
