"""
Test the excel connection with a local file created with openpyxl
Modify the file path to an available path in your local directory
"""

from aleph.common.local_storage import LocalStorage, FileLocalStorage
from aleph.connections.files.generic.excel import ExcelConnection
import unittest
import openpyxl


# Define excel connection to file
class TestExcelConnection(ExcelConnection):

    def __init__(self):
        super().__init__()
        self.read_from_copy = False
        self.file = "/home/al/Desktop/test.xlsx"
        self.local_storage = FileLocalStorage("/home/al/Desktop/local_storage.txt")

    def read(self, key, **kwargs):
        data_ = super().read(key, **kwargs)
        return data_

    def on_read_error(self, error):
        error.raise_exception()


# Test case
class TestExcel(unittest.TestCase):

    def test1(self):
        # Create connection
        X = TestExcelConnection()
        X.open()

        # Create workbook
        wb = openpyxl.Workbook()
        ws = wb.active

        # Test 1
        ws.append([0, 1, 2])
        ws.append([3, 4, 5])
        ws.append([6, 7, 8])
        wb.save(X.file)

        data = X.safe_read("Sheet")
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]["n"], 1)
        self.assertEqual(data[1]["n"], 2)
        self.assertEqual(data[2]["n"], 3)

        # Test 2
        ws.append([99, 99, 99])
        ws.append([12, 12, 88])
        wb.save(X.file)

        data = X.safe_read("Sheet")
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["n"], 4)
        self.assertEqual(data[1]["n"], 5)
        data = X.safe_read("Sheet")
        self.assertEqual(len(data), 0)

        # Test 3
        ws.append([4, 4, 4])
        ws.append([2, 1, 3])
        wb.save(X.file)

        data = X.safe_read("Sheet")
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["n"], 6)
        self.assertEqual(data[1]["n"], 7)

        # Close connection
        X.close()

    def test2(self):
        """
        Reread
        """

        # Create connection
        X = TestExcelConnection()
        X.open()

        data = X.safe_read("Sheet")
        self.assertEqual(len(data), 0)

        X.close()

    def test3(self):
        """
        Same than test2 but now we don't load the last values from the local
        file storage. Thus, the connection should read the whole file again.
        """

        # Create connection
        X = TestExcelConnection()
        X.local_storage = LocalStorage()
        X.open()

        data = X.safe_read("Sheet")
        self.assertEqual(len(data), 7)

        X.close()


if __name__ == '__main__':
    unittest.main()
