#!/bin/env python3
import argparse, subprocess, os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
parser = argparse.ArgumentParser(description="cqldump, a primitive cassandra dumper.")
parser.add_argument("host", help="Cassandra server hostname.",type=str)
parser.add_argument("-u","--user", help="Username for authentication.",type=str)
parser.add_argument("-p","--password", help="Password for authentication.",type=str)
args = parser.parse_args()
cluster = Cluster([args.host], auth_provider=PlainTextAuthProvider(username=args.user, password=args.password))
session = cluster.connect()
name = session.execute("SELECT cluster_name FROM system.local;").one()[0]
cluster_tables:list=[]
print(f"Cluster name: {name}\nObtaining table names...")
keyspaces = session.execute('SELECT keyspace_name FROM system_schema.keyspaces;')
for keyspace_row in keyspaces:
    keyspace_name = keyspace_row.keyspace_name
    try: tables = session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace_name}';")
    except: continue
    for table_row in tables:
        table_name = table_row.table_name
        try: session.execute(f"SELECT * FROM {keyspace_name}.{table_name} LIMIT 1;")
        except: continue
        cluster_tables.append(f"{keyspace_name}.{table_name}")
cluster.shutdown()
os.mkdir(name)
os.chdir(name)
cqlsh_process = subprocess.Popen(f"cqlsh {args.host} -p {args.password} -u {args.user}".split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
for number, table in enumerate(cluster_tables):
    cqlsh_process.stdin.write(f"COPY {table} TO '{table}.csv' WITH HEADER=true AND DELIMITER=';';\n")
    cqlsh_process.stdin.flush()
    while not os.path.isfile(f"{table}.csv"): None
    print(f"Successfully exported {table} to {name}/{table}.csv  [{number+1}/{len(cluster_tables)}]")
cqlsh_process.stdin.close()
print("Waiting the process to terminate...")
cqlsh_process.wait()
