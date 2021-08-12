"""
Use this connection to get data from an http endpoint.

Read the documentation to learn how to use this connection. You will nead to
write a read_function to handle the response.
"""
import requests
from request.exceptions import Timeout


class HttpConnection:

    def __init__(self, endpoint, method, parameters, read_function):
        self.endpoint = endpoint
        self.method = method.upper()
        self.parameters = parameters
        self.connected = False

        METHODS = ["POST", "GET", "PUT", "DELETE", "HEAD", "PATCH", "OPTIONS"]
        if self.method not in METHODS: raise Exception("Invalid method")

    def connect(self):
        try:
            requests.get(endpoint, timeout=5)
            self.connected = True
        except:
            self.connected = False
            raise

    def do(self):
        if not self.connected: raise Exception("Not connected")

        if self.method == "GET":
            r = requests.get(endpoint, data=parameters, timeout=5)
        elif self.method == "POST":
            r = requests.post(endpoint, data=parameters, timeout=5)
        elif self.method == "PUT":
            r = requests.put(endpoint, data=parameters, timeout=5)
        elif self.method == "DELETE":
            r = requests.delete(endpoint, timeout=5)
        elif self.method == "HEAD":
            r = requests.head(endpoint, timeout=5)
        elif self.method == "PATCH":
            r = requests.patch(endpoint, data=parameters, timeout=5)
        elif self.method == "OPTIONS":
            r = requests.options(endpoint, timeout=5)

        return read_function(r)
