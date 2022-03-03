"""
See https://github.com/ruscito/pycomm
"""

from ..connection import Connection
from ...common.exceptions import *


class AllenBradleyConnection(Connection):

    def __init__(self):
        super().__init__("")
        self.ip_address = ""
        self.model = "Micrologix"  # "Compactlogix", "Controllogix", "Micrologix", "SLC"
        self.connection = None

    def open(self):
        if self.model == "Compactlogix" or self.model == "Controllogix":
            from pycomm.ab_comm.clx import Driver
        elif self.model == "Micrologix" or self.model == "SLC":
            from pycomm.ab_comm.slc import Driver
        else:
            raise ConnectionOpenException("Invalid PLC model: " + str(self.model))

        self.connection = Driver()
        self.connection.open(self.ip_address)
        super().open()

    def close(self):
        self.connection.close()
        super().close()

    def read(self, key, **kwargs):
        """
        Implement this function yourself.
        Use the pycomm connection object. For example:

        result = {}
        tag = conn.read_tag("F52:0", 3)
        result["F52_0"] = tag[0]
        result["F52_1"] = tag[1]
        result["F52_2"] = tag[2]
        ...

        Use the int2bits helper function for boolean values:

        from aleph.common.inttobits import int2bits
        tag = conn.read_tag("B59:0", 3)
        tag_ = int2bits(tag[0])
        result["B59_0_0"] = tag_[0]
        result["B59_0_1"] = tag_[1]
        result["B59_0_2"] = tag_[2]
        ...
        """
        return []

    def write(self, key, data):
        """
        Implement this function yourself.
        Use the pycomm connection object. For example:

        conn.write_tag('F8:0', 21)
        """
        return
