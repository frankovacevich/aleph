"""



"""

import sys


# RUN
def run(program):
    import importlib
    program = program.replace("/", ".").replace(".py", "")
    importlib.import_module(program)


# NAMESPACE / LOCALBACKUP
def namespace(namespace_manager):
    # Show the namespace
    for x in namespace_manager.get_namespace(): print(x)

def namespace_peek(namespace_manager, key):
    # Show the last inserted data
    from dateutil.tz import tzlocal

    first_row = True
    for x in namespace_manager.get_data(key, count=10):
        if first_row:
            first_row = False
            if "id_" in x: print("t (UTC)", "\t\t\t", "#", "\t", "id_")
            else: print("t (UTC)", "\t\t\t", "#")

        x["t"] = x["t"].astimezone(tzlocal())
        if "id_" in x: print(x["t"], "\t", len(x),"\t", x["id_"])
        else: print(x["t"], "\t", len(x))

def namespace_rename(namespace_manager, key, new_key):
    if key not in namespace_manager.get_namespace():
        print("The key", key, "is not in the namespace")
        exit()
    namespace_manager.rename_key(key, new_key)
    if new_key in namespace_manager.get_namespace():
        print("Key", key, "successfully renamed to", new_key)
    else:
        print("Unable to rename key due to unkown error")

def namespace_remove(namespace_manager, key):
    if key not in namespace_manager.get_namespace():
        print("The key", key, "is not in the namespace")
        exit()
    namespace_manager.remove_key(key)
    if key not in namespace_manager.get_namespace():
        print("Key", key, "successfully removed from namespace")
    else:
        print("Unable to remove key due to unkown error")

def namespace_delete_by_id(namespace_manager, key, id_):
    if key not in namespace_manager.get_namespace():
        print("The key", key, "is not in the namespace")
        exit()
    x = namespace_manager.delete_data_by_id(key, id_)
    print("Deleted", x, "records")

def namespace_delete_since(namespace_manager, key, since, until):
    if key not in namespace_manager.get_namespace():
        print("The key", key, "is not in the namespace")
        exit()
    s = int(since)
    u = int(until)
    x = namespace_manager.delete_data(key, s, u)
    print("Deleted", x, "records")

def mqttconnection():
    from common.lib.custom_mqtt_connection import custom_mqtt_connection
    import random
    mqttconn = custom_mqtt_connection("aleph_" + str(random.randint(0, 9999)))
    return mqttconn

def publish(key, data):
    import time
    import json

    mqttconn = mqttconnection()
    mqttconn.connect()
    time.sleep(1)
    r = mqttconn.publish(json.loads(data), key)
    time.sleep(1)
    mqttconn.loop()
    mqttconn.disconnect()
    print("Done (r=" + str(r) + ")")

def listen(key):
    import time
    import random
    import json

    mqttconn = mqttconnection()
    mqttconn.on_connect = lambda : print("Listening...")
    mqttconn.subscription_topics = [key]
    mqttconn.on_new_message = lambda topic, msg: print(msg)
    mqttconn.connect()
    time.sleep(2)
    if not mqttconn.connected: raise Exception("Could not connect to MQTT")
    mqttconn.loop_forever()


def restore(namespace_manager, key, since, until):
    from common.lib.custom_mqtt_connection import custom_mqtt_connection
    import time
    import random
    import json

    mqttconn = mqttconnection()
    mqttconn.connect()
    time.sleep(2)
    if not mqttconn.connected: raise Exception("Could not connect to MQTT")

    s = int(since)
    u = int(until)

    if key == "": keys = namespace_manager.get_namespace()
    else:  keys = [key]

    for k in keys:
        data = namespace_manager.get_data(k, since=s, until=u, count=9999999)
        print("Restoring", len(data), "records from", k)
        for record in data:
            r = {x: record[x] for x in record if record[x] is not None}
            mqttconn.publish(data, k)
        time.sleep(5)

    print("Done")

# SERVICES FOR LINUX

def __get_service_info__(program):
    import os
    from common.bin.root_folder import aleph_root_folder

    program = program.replace("/", ".").replace(".py", "")

    python_file = program.replace(".", "/") + ".py"
    aleph_file = os.path.join(aleph_root_folder, "aleph.py")
    woring_directory = aleph_root_folder
    service_description = "Aleph " + program.replace(".", " ").replace("_", " ").title()
    service_name = service_description.lower().replace(" ", "_")
    service_file = os.path.join("etc", "systemd", "system", service_name + ".service")

    info = {
        "python_file": python_file,
        "working_directory": woring_directory,
        "service_name": service_name,
        "service_description": service_description,
        "service_file": service_file,
        "aleph_file": aleph_file,
    }

    return info

srvc_lx = """[Unit]
Description=[description]
After=network.target

[Service]
Type=simple
Restart=always
RestartSec=1
User=[user]
ExecStart=/usr/bin/python3 [aleph_file] run [python_file]
WorkingDirectory=[working_directory]

[Install]
WantedBy=multi-user.target
"""

def install(program):
    import getpass
    import os

    info = __get_service_info__(program)

    try:
        f = open(os.path.join("/etc", "systemd", "system", info["service_name"] + ".service"), "w+")
    except:
        print("Cannot install service. Are you using 'sudo'?")
        exit()

    srvc = srvc_lx.replace("[description]", info["service_description"])
    srvc = srvc.replace("[working_directory]", info["working_directory"])
    srvc = srvc.replace("[python_file]", info["python_file"])
    srvc = srvc.replace("[aleph_file]", info["aleph_file"])
    srvc = srvc.replace("[user]", getpass.getuser())

    try:
        f.write(srvc)
        f.close()
        os.system("sudo systemctl daemon-reload")
        os.system("sudo systemctl enable " + info["service_name"])
        os.system("sudo systemctl start " + info["service_name"])
        print("Service", "'" + info["service_name"] + "'", "installed correctly.")
        print("You can use 'sudo python3 aleph.py status", info["service_name"] + "'", "to check the status")
    except:
        raise

def status(program):
    import os
    from colorama import Fore, Style
    info = __get_service_info__(program)
    f = os.system("sudo systemctl status " + info["service_name"] + " > /dev/null 2>&1")
    if f == 0:
        print(Fore.GREEN + "running")
    else:
        print(Fore.RED + "stopped")

def status_all():
    import os
    from colorama import Fore, Style
    for file in os.listdir("/etc/systemd/system/"):
        if file.startswith("aleph_") and file.endswith(".service"):
            f = os.system("sudo systemctl status " + file + " > /dev/null 2>&1")
            if f == 0:
                print(Fore.GREEN + "running", Style.RESET_ALL, "\t", file)
            else:
                print(Fore.RED + "stopped", Style.RESET_ALL, "\t", file)

# MAN

man = """
MANUAL
"""

# MAIN

if __name__ == "__main__":
    cmd = sys.argv[1]

    if cmd == "namespace" or cmd == "localbackup":
        if cmd == "namespace":
            from common.bin.namespace_manager import NamespaceManager
            manager = NamespaceManager()
        else:
            from common.bin.local_backup import LocalBackup
            manager = LocalBackup()

        cmd2 = ""
        if len(sys.argv) > 2: cmd2 = sys.argv[2]

        if cmd2 == "": namespace(manager)
        elif cmd2 == "peek": namespace_peek(manager, key=sys.argv[3])
        elif cmd2 == "rename": namespace_rename(manager,key=sys.argv[3], new_key=sys.argv[4])
        elif cmd2 == "remove": namespace_remove(manager, key=sys.argv[3])
        elif cmd2 == "delete":
            pass
        elif cmd == "restore":
            pass

    elif cmd == "mqtt":
        cmd2 =sys.argv[2]
        if cmd2 == "publish": publish(key=sys.argv[3], data=sys.argv[4])
        if cmd2 == "listen": listen(key=sys.argv[3])

    elif cmd == "install":
        install(program=sys.argv[2])
        pass

    elif cmd == "status":
        if len(sys.argv) > 2:
            status(program=sys.argv[2])
        else:
            status_all()

    elif cmd == "run":
        run(sys.argv[2])
        pass

    elif cmd == "man":
        print(man)
