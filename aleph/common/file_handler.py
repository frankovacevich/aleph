"""

"""

import os
from shutil import copyfile
import time


class FileHandler:

    def __init__(self, file, read_from_copy=False):
        self.file = file

        # If true, the file will be copied to the temp_folder before reading (avoiding locking the file)
        self.read_from_copy = read_from_copy

        # Where the file will be copied if read_from_copy = True
        self.temp_folder = ""

        # We keep a timestamp of when the file has last been accessed
        self.last_time_read_key = {}

        # Get file creation timestamp
        self.file_creation_timestamp = 0

    def file_has_been_modified(self, key):
        """
        Returns True if the file has changed since the last time the function
        get_file_for_reading() has been called.
        """
        if key not in self.last_time_read_key:
            self.last_time_read_key[key] = 0

        self.file_creation_timestamp = os.path.getctime(self.file)
        last_modified = os.path.getmtime(self.file)
        if last_modified > self.last_time_read_key[key]:
            return True
        else:
            return False

    def get_file_for_reading(self, key):
        """
        Returns the file path for reading
        """

        file_ = self.file

        if self.read_from_copy:
            temp_file = os.path.join(self.temp_folder, "_" + os.path.basename(self.file))

            if self.file_has_been_modified(key):
                copyfile(self.file, temp_file)
                file_ = temp_file

        self.last_time_read_key[key] = time.time()
        return file_
