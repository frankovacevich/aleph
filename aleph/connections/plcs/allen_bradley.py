from ..connection import Connection
import pycomm3


class AllenBradleyConnection(Connection):

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.clean_on_read = False
        self.ip_address = ""
        self.model = "MicroLogix"  # ControlLogix, CompactLogix, Micro800, SLC500, MicroLogix
        self.plc = None

    def open(self):
        if self.model in ["ControlLogix", "CompactLogix", "Micro800"]:
            plc = pycomm3.LogixDriver(self.ip_address)
        elif self.model in ["SLC500", "MicroLogix"]:
            plc = pycomm3.SLCDriver(self.ip_address)
        else:
            raise Exception("Invalid PLC Model: '" + str(self.model) + "'. Valid models are 'ControlLogix', "
                            "'CompactLogix', 'Micro800', 'SLC500' and 'MicroLogix'")

        self.plc = plc
        self.plc.open()

    def close(self):
        if self.plc is None: return
        self.plc.close()
        self.plc = None

    def is_connected(self):
        if self.plc is None: return False
        return self.plc.connected

    def read(self, key, **kwargs):
        """
        Implement this function yourself.
        Use the pycomm3 connection object. For example:

        self.plc.read('F8:0')

        See https://github.com/ottowayi/pycomm3
        """
        tag = key

        # tag = X:I{L}
        if "{" not in tag: tag = tag + "{1}"
        tag_ = tag.split(":")
        X = tag_[0]
        tag_ = tag_[1].replace("}", "").split("{")
        I = int(tag_[0])
        L = int(tag_[1])

        values = self.plc.read(tag).value
        if not isinstance(values, list): values = [values]

        result = {}
        for i in range(0, len(values)):
            result[X + ":" + str(I + i)] = values[i]

        return result

    def write(self, key, data):
        """
        Implement this function yourself.
        Use the pycomm3 connection object. For example:

        self.plc.write('F8:0', 21)

        See https://github.com/ottowayi/pycomm3
        """
        return
