

def get_nm():
    from .namespace_manager import NamespaceManager
    NM = NamespaceManager()
    NM.connect()
    return NM

def get_lb():
    from .local_backup import LocalBackup
    LB = LocalBackup()
    LB.connect()
    return LB

def get_mqtt():
    from common.lib.custom_mqtt_connection import custom_mqtt_connection
    import random
    mqttconn = custom_mqtt_connection("aleph_" + str(random.randint(0, 9999)))
    return mqttconn

# ==============================================================================
# MQTT
# ==============================================================================
def dprint(txt):
    print(txt)


# Publish
def mqtt_publish():
    import time
    import json

    mqttconn = get_mqtt()
    mqttconn.connect()
    time.sleep(1)
    r = mqttconn.publish(json.loads(data), key)
    time.sleep(1)
    mqttconn.loop()
    mqttconn.disconnect()
    print("Done (r=" + str(r) + ")")


# Listen
def mqtt_listen():
    import time
    import random
    import json

    mqttconn = get_mqtt()
    mqttconn.on_connect = lambda : dprint("Listening...")
    mqttconn.subscription_topics = [key]
    mqttconn.on_new_message = lambda topic, msg: dprint(msg)
    mqttconn.connect()
    time.sleep(2)
    if not mqttconn.connected: raise Exception("Could not connect to MQTT")
    mqttconn.loop_forever()

# ==============================================================================
# Namespace / Localbackup
# ==============================================================================

# Get keys
def namespace_keys(lb=False):
    namespace_manager = get_lb() if lb else get_nm()
    for x in namespace_manager.get_keys(): print(x)


# Peek
def namespace_peek(key, lb=False):
    namespace_manager = get_lb() if lb else get_nm()

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


# Rename key
def namespace_rename(key, new_key, lb=False):
    namespace_manager = get_lb() if lb else get_nm()

    if key not in namespace_manager.get_keys():
        print("The key", key, "is not in the namespace")
        exit()
    namespace_manager.rename_key(key, new_key)
    if new_key in namespace_manager.get_keys():
        print("Key", key, "successfully renamed to", new_key)
    else:
        print("Unable to rename key due to unkown error")


# Remove key
def namespace_remove(namespace_manager, key, lb=False):
    namespace_manager = get_lb() if lb else get_nm()

    if key not in namespace_manager.get_keys():
        print("The key", key, "is not in the namespace")
        exit()
    namespace_manager.remove_key(key)
    if key not in namespace_manager.get_keys():
        print("Key", key, "successfully removed from namespace")
    else:
        print("Unable to remove key due to unkown error")


# Delete data by id
def namespace_delete_by_id(namespace_manager, key, id_, lb=False):
    namespace_manager = get_lb() if lb else get_nm()

    if key not in namespace_manager.get_keys():
        print("The key", key, "is not in the namespace")
        exit()
    x = namespace_manager.delete_data_by_id(key, id_)
    print("Deleted", x, "records")

# Deleta data since
def namespace_delete_since(namespace_manager, key, since, until, lb=False):
    namespace_manager = get_lb() if lb else get_nm()

    if key not in namespace_manager.get_keys():
        print("The key", key, "is not in the namespace")
        exit()
    s = int(since)
    u = int(until)
    x = namespace_manager.delete_data(key, s, u)
    print("Deleted", x, "records")

# ==============================================================================
# Backup
# ==============================================================================

def defalt(obj):
    import datetime
    if isinstance(obj, datetime.datetime): return obj.strftime("%Y-%m-%dT%H:%M:%SZ")

# Backup save
def backup_save(since=365, until=0, file_location=""):
    import lzma
    import json
    import os
    import time
    import datetime
    from .root_folder import aleph_root_folder
    from .db_connections import functions as fn

    t0 = time.time()
    nm = get_nm()

    since = fn.__parse_date__(since)
    until = fn.__parse_date__(until, True)

    file_name = datetime.datetime.today().strftime("%Y%m%d") + ".backup"
    if file_location == "": file_name = os.path.join(aleph_root_folder, "local", "backup", file_name)
    else: file_name = os.path.join(file_location, file_name)

    print("Saving backup ...")
    backup_file = lzma.open(file_name, "wb")
    total = 0

    keys = nm.get_keys()
    for key in keys:
        print("Backing up records for", key)
        s = since

        while s < until:
            u = s + datetime.timedelta(days=1)
            if u > until: u = until

            data = nm.get_data(key, "*", s, u)
            if len(data) > 0:
                total += len(data)
                #pickle.dump({key: data}, backup_file)
                backup_file.write((json.dumps({key: data}, default=defalt) + "\n").encode())

            s = s + datetime.timedelta(days=1)

    backup_file.close()
    print("Backup completed successfully. Total: " + str(total) + " records.")
    print("Elapsed time:", time.time() - t0)
    print("Backup saved at", file_name)


# Backup restore
def backup_restore(file_name="backup.pickle"):
    import lzma
    import time
    import os
    import datetime
    import json
    from .root_folder import aleph_root_folder

    nm = get_nm()

    t0 = time.time()
    print("Restoring backup from " + file_name)

    count = 0
    with lzma.open(os.path.join(aleph_root_folder, "local", "backup", file_name), "rb") as backup_file:
        for chunk in backup_file:
            data = json.loads(chunk.decode()[:-1])
            for key in data:
                print("Restoring", len(data[key]), "records at", key, "between", data[key][-1]["t"], "and", data[key][0]["t"])
                
                for record in data[key]:
                    count += len(record)
                    nm.save_data(key, record)

                print(time.time()-t0, count)


        """
        while True:
            try:
                chunk = pickle.load(backup_file)
                for key in chunk:
                    print("Restoring", len(chunk[key]), "records at", key, "from", chunk[key][-1]["t"], "to", chunk[key][0]["t"])
                    for record in chunk[key]:
                        #nm.save_data(key, {x: record[x] for x in record if record[x] is not None})
                        print(record)
                        print()

            except EOFError:
                print("Backup restore completed successfully")
                break
        """

    print("Elapsed time:", round(time.time() - t0, 2), "seconds")

# ==============================================================================
# Localbackup: resend
# ==============================================================================

# Resend

def resend(key, since, until):
    import time, datetime
    from .db_connections.functions import __parse_date__

    lb = get_lb()

    mqttconn = get_mqtt()
    mqttconn.connect()
    time.sleep(2)
    if not mqttconn.connected: raise Exception("Could not connect to MQTT")

    s = __parse_date__(since)
    u = __parse_date__(until)

    if key == "": keys = lb.get_keys()
    else:  keys = [key]

    for k in keys:
        data = lb.get_data(k, since=s, until=u, count=9999999)
        print("Resending", len(data), "records from", k)
        for record in data:
            r = {x: record[x] for x in record if record[x] is not None}
            if "t" in r and isinstance(r["t"], datetime.datetime): r["t"] = r["t"].strftime("%Y-%m-%d %H:%M:%S")
            mqttconn.publish(r, k)
        time.sleep(5)

    print("Done")
