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
        self.sent_messages_ts = {}
        self.title = file

        try:
            self.file = file
            if "/" not in file and "\\" not in file and file != "":
                self.file = os.path.join(aleph_root_folder, "local", "log", file)

            if self.file != "":
                # do a first write
                f = open(self.file, "a+")
                f.close()
                self.title = self.file.split("/")[-1].split(".")[0]

            self.notifier = None  # Add a notifier if you want to receive notifications

        except Exception as e:
            print("Error while initializing log (" + str(e) + ")")
            self.enabled = False

    def write(self, line, message_id=None):
        t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        # Save message id to avoid sending too many consecutive messages
        if message_id is not None:
            if message_id not in self.sent_messages_ts:
                self.sent_messages_ts[message_id] = time.time()
            elif time.time() - self.sent_messages_ts[message_id] > 3600:
                return

        if not self.enabled:
            print(t + " | " + line)
            return

        if self.file != '' and self.enabled:
            f = open(self.file, "a+")
            f.write(t + " | " + line + "\n")
            f.close()

        if self.notifier is not None:
            self.notifier.send("<b>" + self.title + "</b>\n" + line)

        return
