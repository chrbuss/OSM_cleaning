# -*- coding: utf-8 -*-
# Reads cleaned data from CSV files and writes them into a SQLite DB

import sqlite3
import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt
import csv

# Connect to DB and set cursor
connection = sqlite3.connect("osm.db")
cursor = connection.cursor()

# Drop tables if already created
cursor.execute('''DROP TABLE IF EXISTS nodes''')
cursor.execute('''DROP TABLE IF EXISTS nodes_tags''')
cursor.execute('''DROP TABLE IF EXISTS ways''')
cursor.execute('''DROP TABLE IF EXISTS ways_tags''')
cursor.execute('''DROP TABLE IF EXISTS ways_nodes''')
connection.commit()

# Creating tables
sql_nodes = """
CREATE TABLE nodes (
    id INTEGER PRIMARY KEY NOT NULL,
    lat REAL,
    lon REAL,
    user TEXT,
    uid INTEGER,
    version INTEGER,
    changeset INTEGER,
    timestamp TEXT
);"""

sql_nodes_tags = """
CREATE TABLE nodes_tags (
    id INTEGER,
    key TEXT,
    value TEXT,
    type TEXT,
    FOREIGN KEY (id) REFERENCES nodes(id)
);"""

sql_ways = """
CREATE TABLE ways (
    id INTEGER PRIMARY KEY NOT NULL,
    user TEXT,
    uid INTEGER,
    version TEXT,
    changeset INTEGER,
    timestamp TEXT
);"""

sql_ways_tags = """
CREATE TABLE ways_tags (
    id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    type TEXT,
    FOREIGN KEY (id) REFERENCES ways(id)
);"""

sql_ways_nodes = """
CREATE TABLE ways_nodes (
    id INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    position INTEGER NOT NULL,
    FOREIGN KEY (id) REFERENCES ways(id),
    FOREIGN KEY (node_id) REFERENCES nodes(id)
);"""

# Execute table commands
cursor.execute(sql_nodes)
cursor.execute(sql_nodes_tags)
cursor.execute(sql_ways)
cursor.execute(sql_ways_tags)
cursor.execute(sql_ways_nodes)

connection.commit()

# Read in the csv file as a dictionary, format the data as a list of tuples

# nodes
with open('nodes.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), 
              i['lat'].decode("utf-8"),
              i['lon'].decode("utf-8"), 
              i['user'].decode("utf-8"), 
              i['uid'].decode("utf-8"), 
              i['version'].decode("utf-8"), 
              i['changeset'].decode("utf-8"), 
              i['timestamp'].decode("utf-8")) for i in dr]
cursor.executemany("INSERT INTO nodes(id, lat, lon, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?);", to_db)

# node_tags
with open('nodes_tags.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), 
              i['key'].decode("utf-8"),
              i['value'].decode("utf-8"), 
              i['type'].decode("utf-8")) for i in dr]
cursor.executemany("INSERT INTO nodes_tags(id, key, value,type) VALUES (?, ?, ?, ?);", to_db)
    
#ways
with open('ways.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), 
              i['user'].decode("utf-8"),
              i['uid'].decode("utf-8"), 
              i['version'].decode("utf-8"), 
              i['changeset'].decode("utf-8"), 
              i['timestamp'].decode("utf-8")) for i in dr]
cursor.executemany("INSERT INTO ways(id, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?);", to_db)

#ways_tags
with open('ways_tags.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), 
              i['key'].decode("utf-8"),
              i['value'].decode("utf-8"), 
              i['type'].decode("utf-8")) for i in dr]
cursor.executemany("INSERT INTO ways_tags(id, key, value,type) VALUES (?, ?, ?, ?);", to_db)

#ways_nodes
with open('ways_nodes.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), 
              i['node_id'].decode("utf-8"),
              i['position'].decode("utf-8")) for i in dr]
cursor.executemany("INSERT INTO ways_nodes(id, node_id, position) VALUES (?, ?, ?);", to_db)

connection.commit()
connection.close()

# Analyse SQL data

connection = sqlite3.connect("osm.db")
cursor = connection.cursor()

cursor.execute("""SELECT count(*) FROM nodes""")
result = cursor.fetchall() 
for r in result:
    print "Number of nodes:", r[0]
    
cursor.execute("""SELECT count(*) FROM ways""")
result = cursor.fetchall() 
for r in result:
    print "Number of ways:", r[0]
    
cursor.execute("""SELECT count(distinct(e.uid)) FROM (SELECT uid FROM nodes UNION ALL SELECT uid FROM ways) e""")
result = cursor.fetchall() 
for r in result:
    print "Number of unique users:", r[0]
    
cursor.execute("""SELECT count(key) FROM nodes_tags""")
result = cursor.fetchall() 
for r in result:
    print "Number of node tags:", r[0]

command =  """
    SELECT e.user, COUNT(*) as num 
    FROM (SELECT user FROM nodes UNION ALL SELECT user FROM ways) e
    GROUP BY e.user
    ORDER BY num DESC
    LIMIT 10;"""

cursor.execute(command)
result = cursor.fetchall() 
authors = []
contributions = []
for r in result:
    print r[0], r[1]
    contributions.append(r[1])
    authors.append(r[0])
    
y_pos = np.arange(len(contributions))
plt.barh(y_pos, contributions, align='center', alpha=0.5)
plt.yticks(y_pos, authors)
plt.xlabel('Number of contributions')
plt.title('Top 10 contributors')
 
plt.show()

# Total number of contributions
command =  """
    SELECT COUNT(*) as num 
    FROM (SELECT user FROM nodes UNION ALL SELECT user FROM ways) e;"""

cursor.execute(command)
result = cursor.fetchall() 

for r in result:
    print r[0]

# Most frequent keys
cursor.execute("""SELECT key, count(key) FROM nodes_tags GROUP BY key ORDER BY count(key) DESC LIMIT 10""")
result = cursor.fetchall() 
for r in result:
    print r[0],r[1]

# Most frequent amenities
cursor.execute("""SELECT key, value, count(value) FROM nodes_tags WHERE key='amenity' GROUP BY value ORDER BY count(value) DESC LIMIT 10""")
result = cursor.fetchall() 
for r in result:
    print r[1],r[2]

# Most frequent names
cursor.execute("""SELECT value, count(value) FROM nodes_tags WHERE key='name' GROUP BY value ORDER BY count(value) DESC LIMIT 20""")
result = cursor.fetchall() 
for r in result:
    print r[0],r[1]

# Most frequent postalcodes
cursor.execute("""SELECT value, count(value) FROM ways_tags WHERE key='postal_code' GROUP BY value ORDER BY count(value) DESC LIMIT 20""")
result = cursor.fetchall() 
for r in result:
    print r[0],r[1]

# Opening hours
cursor.execute("""SELECT value, count(value) FROM nodes_tags WHERE key='opening_hours' GROUP BY value ORDER BY count(value) DESC""")
result = cursor.fetchall() 
for r in result:
    print r[0],r[1]

# Most frequent postalcodes
cursor.execute("""SELECT value, count(value) FROM nodes_tags WHERE key='maxspeed' GROUP BY value ORDER BY count(value) DESC LIMIT 20""")
result = cursor.fetchall() 
for r in result:
    print r[0],r[1]

# Cafes and opening times
command = """SELECT DISTINCT c.value, b.value
FROM nodes_tags as a, nodes_tags as b, nodes_tags as c 
WHERE a.id = b.id AND b.id = c.id AND 
a.value = "cafe" AND b.key = "name" AND c.key = "opening_hours"; """

cursor.execute(command)
result = cursor.fetchall() 
for r in result:
    print r[0], r[1]

# Cafes and wheelchair accessability
command = """SELECT COUNT(b.value), b.value
FROM nodes_tags as a, nodes_tags as b
WHERE a.id = b.id AND 
a.value = "cafe" AND b.key = "wheelchair"
GROUP BY b.value
;"""

cursor.execute(command)
result = cursor.fetchall() 
for r in result:
    print r

# Restaurants and wheelchair accessability
command = """SELECT COUNT(b.value), b.value
FROM nodes_tags as a, nodes_tags as b
WHERE a.id = b.id AND 
a.value = "restaurant" AND b.key = "wheelchair"
GROUP BY b.value
;"""

cursor.execute(command)
result = cursor.fetchall() 
for r in result:
    print r



