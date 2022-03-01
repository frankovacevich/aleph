import random
import string
from ..connection import Connection
import time
from ...common.exceptions import *


class RandomConnection(Connection):

    def __init__(self):
        super().__init__()
        self.i = 0

    def read(self, key, **kwargs):
        data = {
            "string": ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)),
            "int": random.randint(0, 100),
            "float": random.random(),
            "bool": random.random() > 0.5,
        }
        time.sleep(1)
        return data

    def write(self, key, data):
        pass
