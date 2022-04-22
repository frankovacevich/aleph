from ..connection import Connection
from ping3 import ping
import socket
import time


class PingConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.multi_threaded = True
        self.clean_on_read = False
        self.report_by_exception = True

        # Other options
        self.consecutive_pings_to_declare_offline = 2

        # Internal
        self.__ping_count__ = {}

    def read(self, key, **kwargs):
        """
        Returns True if there's ping to the given IP, else False
        The key is the IP address "X.X.X.X"
        You can also check if a port is open using "X.X.X.X:PORT"
        """

        # Get ping status (r = True or False)
        if ":" in key:
            k = key.split(":")
            r = self.check_port(k[0], int(k[1]))
            key = key.replace(".", "_").replace(":", "__")
        else:
            r = self.ping(key)
            key = key.replace(".", "_")

        # Determine result based on ping count
        if r or key not in self.__ping_count__:
            self.__ping_count__[key] = 0
        else:
            self.__ping_count__[key] += 1
            if self.__ping_count__[key] >= self.consecutive_pings_to_declare_offline:
                self.__ping_count__[key] = self.consecutive_pings_to_declare_offline
                r = False
            else:
                r = True

        return {key: r}
        # TODO: return {"device": key, "status": r}

    @staticmethod
    def ping(ip_address):
        try:
            result = ping(ip_address)
            return result is not None and result is not False
        except AttributeError:
            return False

    @staticmethod
    def check_port(ip_address, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((ip_address, port)) == 0
        sock.close()
        return result
