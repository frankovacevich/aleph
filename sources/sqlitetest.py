"""

"""

# Imports from common
from common.lib.custom_mqtt_connection import custom_mqtt_connection
from common.bin.logger import Log

# Imports from sources
from sources.bin.loop import Loop
from sources.bin.connections.excel_simple import SimpleExcelConnection

# read function
def read_function(data):
    print(data)
    print("###############")

# Create random connection
SC = SimpleExcelConnection("/home/aleph/Desktop/x.xlsx", read_function=read_function)
SC.include_past_records = True

# Create MQTT connection
mqtt_connection = custom_mqtt_connection("test_sqlite_connection")
mqtt_connection.connect()

# Create log file
log = Log("test_sqlite_connection.log")

# Create ĺoop
main_loop = Loop(2)
main_loop.source_connection = SC
main_loop.mqtt_connection = mqtt_connection
main_loop.log = log

# Start
main_loop.start()
