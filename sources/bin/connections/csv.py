"""

"""

import pandas as pd


def CSVConnection:

    def __ init__(self, file, read_function):
        self.file = file
        self.read_function = read_function
        self.connected = False

        self.has_headers = True
        self.read_from_copy = False
        self.include_past_records = False

        # Aux
        self.temp_file = os.path.join(aleph_root_folder, "local", "temp", os.path.basename(self.file))
        self.las_row_file = self.temp_file + ".dat"
        self.last_modified = 0
        self.last_row = None
        pass

    def connect(self):
        # Check if file exists
        if os.path.isfile(self.file):
            self.connected = True
        else:
            raise Exception("File does not exist")

        # Check if we have a .dat file that contains the last scanned row
        if os.path.isfile(self.last_row_file):
            try:
                f = open(self.last_row_file)
                content = json.loads(f.read())
                f.close()
                self.last_row = content["last_row"]
                self.last_modified = content["last_modified"]
            except:
                pass

    def do(self):
        if not self.connected: raise Exception("Not connected")

        # Get the last modified timestamp from file
        # If file hasn't changed, do nothing
        last_modified = os.path.getmtime(self.file)
        if not last_modified > self.last_modified: return {}
        self.last_modified = last_modified

        # Copy the file because if it might be locked if open
        if self.read_from_copy:
            copyfile(self.file, self.temp_file)
            dataframe = pd.read_csv(self.temp_file, headers=self.headers)
        else:
            dataframe = pd.read_csv(self.file, headers=self.headers)


        # Main -----
        data = []

        # if last row is unkown, it's the first run ever
        if self.last_row is None:
            if self.include_past_records:
                data = dataframe.values.tolist()

            self.last_row = len(dataframe)

        elif len(dataframe) > self.last_row:
            data = dataframe.iloc[self.last_row:].values.tolist()
            self.last_row = len(dataframe)

        # ----------

        # Save last row to file
        try:
            f = open(self.last_row_file, "w+")
            f.write(json.dumps({"last_row": self.last_row, "last_modified": self.last_modified}))
            f.close()
        except:
            pass

        # Pass data to read_function
        return self.read_function(data)
