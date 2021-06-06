"""

"""

# Imports from common
from common.lib.custom_mqtt_connection import custom_mqtt_connection
from common.bin.logger import Log

# Imports from sources
from sources.bin.loop import Loop
from sources.bin.connections.random import RandomConnection

# Create random connection
RC = RandomConnection()

# Create MQTT connection
mqtt_connection = custom_mqtt_connection("test_random_connection")
mqtt_connection.connect()

# Create log file
log = Log("test_random_connection.log")

# Create ĺoop
main_loop = Loop(10)
main_loop.source_connection = RC
main_loop.mqtt_connection = mqtt_connection
main_loop.log = log

# Start
main_loop.start()
