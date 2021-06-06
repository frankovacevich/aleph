"""
Use the Log class on any script to log any events (instead of printing)
Modify this script to send notifications if you want.
"""

import time
import os
from . import notifier
from .root_folder import aleph_root_folder


class Log:

    def __init__(self, file):
        self.enabled = True

        try:
            self.file = file
            if "/" not in file and "\\" not in file and file != "":
                self.file = os.path.join(aleph_root_folder, "local", "log", file)

            if self.file != "":
                # do a first write
                f = open(self.file, "a+")
                f.close()

            self.notifier = None ## Add a notifier if you want to receive notifications

        except Exception as e:
            print("Error while initializing log (" + str(e) + ")")
            self.enabled = False

    def write(self, line):
        t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        # if not self.enabled:
        #     print(t + " | " + line)
        #     return
        print(t + " | " + line)

        if self.file != '':
            f = open(self.file, "a+")
            f.write(t + " | " + line + "\n")
            f.close()

        if self.notifier is not None:
            self.notifier.send("<b>" + self.file.split("/")[-1].split(".")[0] + "</b>\n" + line)

        return
