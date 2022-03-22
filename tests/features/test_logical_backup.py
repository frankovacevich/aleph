from aleph.connections.databases.generic.sqlite import SqliteConnection
from aleph.connections.testing.random import RandomConnection
from aleph.common.logical_backup import LogicalBackup


key = "test"

# Create features and backup
RC = RandomConnection()
RC.delay = 0
SQ = SqliteConnection()
SQ.file = key + ".db"
LB = LogicalBackup(key=key)
LB.step = 500

RC.open()
SQ.open()

# Write some data on db
for i in range(0, 1000):
    data = RC.safe_read(key)
    SQ.write(key, data)

# Do backup
LB.backup(SQ)

LB.key = key + "_restored"
LB.restore(SQ)
