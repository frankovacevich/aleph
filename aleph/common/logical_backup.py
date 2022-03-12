"""
Stores records with pickle to a file
"""
import lzma
import json
import time


class LogicalBackup:

    def __init__(self, **kwargs):
        self.key_read = ""
        self.key_write = ""
        self.connection = None

        self.since = None
        self.until = None
        self.verbose = True
        self.filename = "backup.lzma"
        self.step = 100000  # Records per step

        self.__dict__.update(kwargs)

    def backup(self):
        if self.connection is None: raise Exception("Undefined connection to write to")

        backup_file = lzma.open(self.filename, "wb")

        total = 0   # Count how many records are backed up
        off = 0  # Use limit and offset to read data in chunks

        if self.verbose:
            print("Saving backup ...")
            self.connection.on_read_error = lambda e: print(e.message_and_traceback())

        while True:
            if self.verbose: print("Reading records from", off, " to", off + self.step)

            # Read data
            data = self.connection.safe_read(self.key_read, since=self.since, until=self.until, limit=self.step, offset=off)

            # Update total and offset
            if len(data) == 0: break
            total += len(data)
            off += self.step

            # Save data to file
            backup_file.write((json.dumps(data) + "\n").encode())

        # Finish
        if self.verbose:
            print()
            print("Finished backup. Total number of records:", total)
        return

    def restore(self):
        if self.connection is None: raise Exception("Undefined connection to write to")

        if self.verbose:
            print("Restoring data")
            self.connection.on_write_error = lambda e: print(e.message_and_traceback())

        i = 0
        total = 0
        t0 = time.time()

        # Open backup file
        with lzma.open(self.filename, "rb") as backup_file:

            # For each chunk
            for chunk in backup_file:
                if self.verbose:
                    print("Reading chunk", i)

                # Get data from chunk and write
                data = json.loads(chunk.decode())
                total += len(data)
                self.connection.safe_write(self.key_write, data)

                if self.verbose:
                    print("Elapsed time:", round(time.time() - t0, 2), "seconds.", "Total", len(data), "records")
                i += 1

        # Done
        if self.verbose:
            print()
            print("Finished restoring backup")
            print("Total number of records:", total)
            print("Total number of chunks:", i)
            print("Time elapsed:", round(time.time() - t0, 2), "seconds")
