"""

"""
from shutil import copyfile
import os
import sqlite3
import json
from common.bin.root_folder import aleph_root_folder


class SqliteConnection:

    def __init__(self, file, read_function):
        self.file = file
        self.id_col = id_col

        self.read_function = read_function
        self.connected = False

        # Optional
        self.id_col = "id"
        self.tables_to_read = []
        self.watch_complete_file = False
        self.read_from_copy = False
        self.include_past_records = False

        # Aux
        self.temp_file = os.path.join(aleph_root_folder, "local", "temp", os.path.basename(self.file))
        self.last_id_file = self.temp_file + ".dat"
        self.last_id = {}
        self.records = {} # Used only for watch_complete_file
        self.last_modified = 0

    def connect(self):
        self.connected = False

        # Check if the database file exists
        if os.path.isfile(self.file):
            self.connected = True
        else:
            raise Exception("File does not exist")

        # Check if we have a .dat file that contains the last id scanned
        if os.path.isfile(self.last_id_file):
            try:
                f = open(self.last_id_file)
                content = json.loads(f.read())
                f.close()
                self.last_id = content["last_id"]
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

        # Copy the file because otherwise it will be locked and other processes
        # won't be able to write data
        if self.read_from_copy:
            copyfile(self.file, self.temp_file)
            conn = sqlite3.connect(self.temp_file)
        else:
            conn = sqlite3.connect(self.file)

        # Create cursor to perform queries
        cur = conn.cursor()

        # Get all tables if self.tables_to_read is empty
        tables_to_read = self.tables_to_read
        if len(tables_to_read) == 0:
            query = "SELECT * FROM sqlite_master where type='table';"
            cur.execute(query)
            r = cur.fetchall()
            tables_to_read = [x[1] for x in r if x[1] != 'sqlite_sequence']

        # Create a data dict that will store the data read from the database
        # {table1: [(value1, value2, value3), (value1, value2, value3), ...], ...}
        data = {}

        # Main loop ---

        # For each table
        for table in tables_to_read:
            data[table] = []

            # Check if last id is known (if it's not is the first run ever)
            if table not in self.last_id:

                # If watch_complete_file or include_past_records we'll need
                # to get first all records from the table. We'll then get the
                # last id from there.
                if self.include_past_records or self.watch_complete_file:

                    # Get all records in table
                    query = "SELECT `" + self.id_col + "`, * FROM `" + table + "` ORDER BY `" + self.id_col + "` ASC"
                    cur.execute(query)
                    r = cur.fetchall()
                    if len(r) == 0: continue # If table is empty

                    # Save recrods for later comparison
                    if self.watch_complete_file:
                        self.records[table] = {x[0]: x for x in r}

                    # Add to data if include past records
                    if self.include_past_records:
                        data[table] = r

                    # Get last id
                    self.last_id[table] = r[-1][0]

                # Else get last id normally
                else:

                    # Get last id
                    query = "SELECT `" + self.id_col + "` FROM `" + table + "` ORDER BY `" + self.id_col + "` DESC LIMIT 1"
                    cur.execute(query)
                    r = cur.fetchall()
                    if len(r) == 0: continue # If table is empty
                    self.last_id[table] = r[0][0]

                continue # to next table

            # If watch_complete_file
            if self.watch_complete_file:

                # Get all records from table
                query = "SELECT `" + self.id_col + "`, * FROM `" + table + "` ORDER BY `" + self.id_col + "` ASC"
                cur.execute(query)
                r = cur.fetchall()
                if len(r) == 0: continue

                # If no memory of records, scan all the table again
                if table not in self.records or len(self.records[table] == 0):
                    self.records[table] = {x[0]: x for x in r}

                # If there are records in memory, compare them with the table to
                # see if there are any changes
                else:

                    # Create dict from queried records
                    dr = {x[0]: x for x in r}

                    for record in self.records[table]:

                        # If record not in query, id must have been removed
                        if record not in dr:
                            del self.records[table][record]

                        # If records length has changed, the number of columns
                        # changed. Then clear all memory records
                        if len(self.records[table][record]) != len(dr[record]):
                            self.records[table] = {}
                            break

                        # See if values have changed
                        if False in [self.records[table][record][i] == dr[record][i] for i in range(0, len(dr[record]))]:

                            # Update record
                            self.records[table][record] = dr[record]

                            # Add to data
                            data[table].append(dr[record])

            # Get new records
            query = "SELECT `" + self.id_col + "`, * FROM `" + table + "` WHERE `" + self.id_col + "`>" + str(self.last_id[table]) + " ORDER BY `" + self.id_col + "` ASC"
            cur.execute(query)
            r = cur.fetchall()
            if len(r) == 0: continue

            # Add records to data
            data[table] += r

            # Add records to memory if watch_complete_file
            if self.watch_complete_file:
                self.records[table].update({x[0]: x for x in r})

            # Update last id
            self.last_id[table] = r[-1][0]

        # -------------

        # Close connection
        cur.close()
        conn.close()

        # Save last ids to dat file
        try:
            f = open(self.last_id_file, "w+")
            f.write(json.dumps({"last_id": self.last_id, "last_modified": self.last_modified}))
            f.close()
        except:
            pass

        # Pass data to read_function
        return self.read_function(data)
