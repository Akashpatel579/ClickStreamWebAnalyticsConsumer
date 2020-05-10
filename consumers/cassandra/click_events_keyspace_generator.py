from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

import logging

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

KEYSPACE = "webanalytics"

cluster = Cluster(['localhost'])
session = cluster.connect()

session.execute("""CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """ % KEYSPACE)

session.set_keyspace(KEYSPACE)

session.execute("DROP TABLE " + "clickevents")

session.execute("""CREATE TABLE clickevents (datetime timestamp, value text, PRIMARY KEY (datetime,value))""")

# session.execute("""CREATE TABLE clickevents (id timestamp, value text, PRIMARY KEY (id,value))""")


# session.execute("""INSERT INTO clickevents (id,value) VALUES (toUnixTimestamp(now()), 'My Name is Akash Patel')""")


# future = session.execute_async("SELECT * FROM clickevents")
#
# try:
#     print("result")
#     rows = future.result()
# except Exception:
#     print("ERROR")
#
# print("id  \t\t\t | value")
# for row in rows[0:5]:
#     print(row[0], " | ", row[1])


# session.execute("DROP KEYSPACE " + KEYSPACE)
