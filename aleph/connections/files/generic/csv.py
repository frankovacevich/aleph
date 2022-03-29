from ...connection import Connection
from ....common.file_handler import FileHandler
import csv

COLUMN_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
COLUMN_LETTERS = [x for x in COLUMN_LETTERS]
COLUMN_LETTERS += ["A" + x for x in COLUMN_LETTERS]
COLUMN_LETTERS += ["B" + x for x in COLUMN_LETTERS]


class CsvConnection(Connection):

    def __init__(self):
        self.__init__()
        self.clean_on_read = False
        self.report_by_exception = True

        # File path to the CSV
        self.file = ""

        # Optional parameters
        self.read_from_copy = False                 # Copy file before reading
        self.number_of_watching_rows = 100          # Watch changes on the last number_of_watching_rows rows
        self.columns = FileHandler.COLUMN_LETTERS   # Column names as list
        self.delimiter = ','                        # CSV Field delimiter
        self.quotechar = '"'                        # CSV Quote char
        self.include_row_number = False             # Include row number in returned records

        # Private
        self.file_handler = None

    # ===================================================================================
    # Open
    # ===================================================================================
    def open(self):
        self.file_handler = FileHandler(self.file, self.read_from_copy)
        super().open()

    def close(self):
        self.file_handler = None
        super().close()

    # ===================================================================================
    # Read
    # ===================================================================================
    def read(self, key, **kwargs):
        # If data hasn't changed, don't do anything
        if not self.file_handler.file_has_been_modified(key): return []

        # Open file
        file = self.file_handler.get_file_for_reading(key)
        f = open(file, "r")
        reader = csv.reader(f, delimiter=self.delimiter, quotechar=self.quotechar, newline='')

        data = []
        r = 1
        # For each row in range
        for row in reader:

            # Add row number
            row_data = {}
            if self.include_row_number: row_data["n"] = r

            # For each column, get cell value
            for c in range(0, len(row)):
                row_data[self.columns[c]] = row[c]

            # Add data
            data.append(row_data)
            r += 1

        # Close file
        f.close()

        # Shorten data
        if len(data) > self.number_of_watching_rows: data = data[-self.number_of_watching_rows:]
        return data
