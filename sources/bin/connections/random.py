"""
Connection
----------

The connection class is used to get data from different sources. Notice that
there are other .py files on this same directory that also contain a Connection
class. Each Connection class communicates with a different data source.

Use the Connection class with the Loop class (loop.py) to read get data every
x seconds.

Connections must have:
- a "connect()" method
- a "do()" method
- a "self.connected" property

The connect() method returns None and sets the self.connected value to True if the
device can be connected. For example, if you're trying to read a file, you might
want to check if the file exists.

The do() method returns a dict {field: value, ...} (single record) or a list of
dict [{field: value, ...}, {field2: value2, ...}, ...] (multiple records).

You can create more connections for any data source you need to connect to.

This Connection simple returns a random value.
"""

import random, string


class RandomConnection:

    def __init__(self):
        self.connected = False
        return

    def connect(self):
        self.connected = True
        return

    def do(self):
        if not self.connected:
            raise Exception("Not Connected")

        # Use this to generate a random string
        letters = string.ascii_lowercase

        data = {
            "x": random.random(),
            "y": random.randint(0, 100),
            "z": ''.join(random.choice(letters) for i in range(random.randint(5,10)))
        }

        return data
