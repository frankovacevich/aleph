import time
import datetime
import traceback
from common.bin.wait_one_step import WaitOneStep


class Loop:

    def __init__(self, time_step=1):
        self.source_connection = None  # See devices/bin/connections
        self.log = None  # See common/bin/logger
        self.mqtt_connection = None  # See common/bin/mqtt_connection
        self.local_backup = None  # See common/bin/local_backup
        self.on_new_data = None  # function(data)
        self.publish_by_exception = True

        # Set time step and create waitonestep class
        self.time_step = time_step
        self.wait_one_step = WaitOneStep(time_step)

        # Create some dicts to store data when publishing by exception
        self.known_data = {}
        self.known_data_t = {}

        return

    def start(self):

        # Start counting the time
        t = self.wait_one_step.step(0)

        # Write to log
        self.log.write("Starting loop")

        # Start loop thread for the mqtt client
        self.mqtt_connection.connect()
        self.mqtt_connection.loop_on_background()

        # Keep the time an error is display to space out the messages
        # so that we do not flood the user's log
        __last_errors__ = {"mqtt_conn": 0, "device_conn": 0, "reading_device": 0}

        while True:

            # 0. make time step
            t = self.wait_one_step.step(t)

            # 1. connect to mqtt
            try:
                # When step interval is too long, force a loop so that we're sure we are connected
                if self.time_step > 10:
                    self.mqtt_connection.loop()
                if not self.mqtt_connection.connected:
                    r = self.mqtt_connection.reconnect()

            except Exception as e:
                if time.time() - __last_errors__["mqtt_conn"] > 3600:
                    self.log.write("Error while connecting to mqtt server (" + str(e) + ").")
                    __last_errors__["mqtt_conn"] = time.time()
                continue

            # 2. Establish device connection
            try:
                if not self.source_connection.connected:
                    self.source_connection.connect()
                    if self.source_connection.connected:
                        self.log.write("Connected to device")

                __last_errors__["device_conn"] = ""
            except Exception as e:
                if time.time() - __last_errors__["device_conn"] > 3600:
                    self.log.write("Error while connecting to source (" + str(e) + ")")
                    __last_errors__["device_conn"] = time.time()
                self.source_connection.connected = False
                pass

            # 3. Read values from device
            many_values = []
            try:
                t0 = time.time()
                many_values = self.source_connection.do()  # This can be a dict or a list
                t1 = time.time()

                if isinstance(many_values, list):
                    for v in many_values:
                        v["t"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(0.5 * (t0 + t1)))
                        v["dt"] = t1 - t0
                elif isinstance(many_values, dict):
                    many_values["t"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(0.5 * (t0 + t1)))
                    many_values["dt"] = t1 - t0
                elif many_values is None:
                    continue
                else:
                    raise Exception("Bad format for data returned by do() (must be a dict or a list of dict)")

                __last_errors__["reading_device"] = ""
            except Exception as e:
                if time.time() - __last_errors__["reading_device"] > 3600:
                    error_as_string = " (" + str(e) + ")" + "\n\n" + traceback.format_exc()
                    self.log.write("Error while reading source: " + error_as_string + "")
                    __last_errors__["reading_device"] = time.time()
                self.source_connection.connected = False
                continue

            # Encapsulate list of values in a list
            if isinstance(many_values, dict):
                many_values = [many_values]

            # For each dict values
            for values in many_values:

                # If dict is empty
                if len(values) == 0:
                    continue

                # If data is malformed
                if not isinstance(values, dict):
                    self.log.write("Device reading should return a dict or a list of dict, got " + str(values))
                    self.source_connection.connected = False
                    continue

                # 4. Remove known data to publish by exception
                if self.publish_by_exception:
                    fields_to_delete = []
                    for field in values:
                        if field == "t" or field == "dt": continue

                        # Publish anyway if it's been more than 12 hours since last pub
                        if field in self.known_data_t and time.time() - self.known_data_t[field] > 3600*12:
                            self.known_data_t[field] = time.time()
                            continue

                        # If value hasn't changed, do not publish
                        if field in self.known_data and self.known_data[field] == values[field]:
                            fields_to_delete.append(field)
                        else:
                            self.known_data[field] = values[field]
                            self.known_data_t[field] = time.time()

                    # Remove fields whose value hasn't changed
                    for field in fields_to_delete:
                        del values[field]

                    if len(values) <= 2:
                        continue

                # 5. On new data
                if self.on_new_data is not None:
                    try:
                        self.on_new_data(values)
                    except Exception as e:
                        error_as_string = " (" + str(e) + ")" + "\n\n" + traceback.format_exc()
                        self.log.write("Error while executing on_new_data function on master " + error_as_string + "")

                # 6. Send mqtt message
                if self.mqtt_connection is not None:
                    try:
                        self.mqtt_connection.publish(values)
                    except Exception as e:
                        self.log.write("Error while sending mqtt message (" + str(e) + ")")
                        continue

                # 6. Local backup
                if self.local_backup is not None:
                    self.local_backup.save_data(values)

                continue

        return
