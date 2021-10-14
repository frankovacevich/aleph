"""
Saves process data to database and forwards messages to upstream server
"""

import datetime
import traceback
from common.bin.logger import Log
from common.bin.namespace_manager import NamespaceManager
from common.lib.custom_mqtt_connection import custom_mqtt_connection

# Create log
log = Log("main_supervisor.txt")
# Create mqtt client
mqtt_connection = custom_mqtt_connection("main_supervisor")
# Namespace Manager
namespace_manager = NamespaceManager()


def process_new_message(topic, data):
    # Get message
    try:
        if not isinstance(data, dict): raise Exception("Badly formatted data (data must be a dict)")
    except Exception as e:
        error_as_string = ''.join(traceback.format_exception(None, e, e.__traceback__))
        log.write("Error while trying to decode data. " + str(e) + "\n\n" + error_as_string)
        return

    # Add t if not present
    if "t" not in data: data["t"] = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    # Save to database
    if "__test__" in data: return
    namespace_manager.save_data(topic.replace("/", "."), data)

    return


# Subscribe and start connection
log.write("Starting main supervisor")
mqtt_connection.on_new_message = process_new_message
mqtt_connection.subscription_topics = ["#"]
mqtt_connection.loop_forever(persistent=True)
