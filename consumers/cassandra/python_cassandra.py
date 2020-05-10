
# https://cassandra.apache.org/download/
# https://cassandra.apache.org/doc/latest/getting_started/installing.html#prerequisites
# pip install cassandra-driver

import cassandra

print (cassandra.__version__)

import logging

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KEYSPACE = "testkeyspace"


cluster = Cluster(['localhost'])
session = cluster.connect()

log.info("creating keyspace...")
session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)

log.info("setting keyspace...")
session.set_keyspace(KEYSPACE)


log.info("creating table...")
session.execute("""
        CREATE TABLE IF NOT EXISTS mytable (
            thekey text,
            col1 text,
            col2 text,
            PRIMARY KEY (thekey, col1)
        )
        """)

query = SimpleStatement("""
       INSERT INTO mytable (thekey, col1, col2)
       VALUES (%(key)s, %(a)s, %(b)s)
       """, consistency_level = ConsistencyLevel.ONE)

prepared = session.prepare("""
        INSERT INTO mytable (thekey, col1, col2)
        VALUES (?, ?, ?)
        """)

for i in range(10):
        log.info("inserting row %d" % i)
        session.execute(query, dict(key="key%d" % i, a='a', b='b'))
        session.execute(prepared, ("key%d" % i, 'b', 'b'))

future = session.execute_async("SELECT * FROM mytable")
log.info("key\tcol1\tcol2")
log.info("---\t----\t----")


try:
    rows = future.result()
except Exception:
    log.exception("Error reading rows:")

for row in rows:
    log.info('\t'.join(row))


# session.execute("DROP KEYSPACE " + KEYSPACE)
