import sqlite3
import time
import re
import zlib
import json
from datetime import datetime, timedelta

# Not all systems have this
try:
    import dateutil.parser as parser
except:
    pass

dnsmapping = dict()
mapping = dict()


def clean_record(record):
    record = record.decode()

    try:
        record_json = json.loads(record)
    except:
        record_json = None
    return record_json

conn = sqlite3.connect(r'c:\python\data_analysis\database\index.sqlite')
cur = conn.cursor()

cur.execute('''DROP TABLE IF EXISTS State ''')
cur.execute('''DROP TABLE IF EXISTS Year_Range ''')
cur.execute('''DROP TABLE IF EXISTS School_Computers ''')


cur.execute('''CREATE TABLE IF NOT EXISTS State
    (state_id INTEGER PRIMARY KEY autoincrement not null, state_name TEXT UNIQUE)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Year_Range
    (year_id INTEGER PRIMARY KEY autoincrement not null, year_range TEXT UNIQUE)''')
cur.execute('''CREATE TABLE IF NOT EXISTS School_Computers
    (id INTEGER PRIMARY KEY not null, 
     fk_state_id INTEGER,
     fk_year_id INTEGER,
     primary_only REAL,
     primary_with_u_primary REAL,
     primary_with_u_primary_sec_hrsec REAL,
     u_primary_only REAL,
     u_primary_with_sec_hrsec REAL,
     primary_with_u_primary_sec REAL,
     u_primary_with_sec REAL,
     sec_only REAL,
     sec_with_hrsec REAL,
     hrsec_only REAL,
     all_schools REAL)''')

conn.commit()

# Open the main content (Read only)
content_db = r'c:\python\data_analysis\database\datadump.sqlite'
#conn_1 = sqlite3.connect('file:content.sqlite?mode=ro', uri=True)
conn_1 = sqlite3.connect('file:%s?mode=ro' %content_db, uri=True)

cur_1 = conn_1.cursor()

cur_1.execute('''SELECT id, record FROM School_With_Computers''')
for message_row in cur_1 :
    message_id = message_row[0]
    record_json = clean_record(message_row[1])
    if len(record_json) < 1 or record_json == None:
        print("Unable to retrive json record from base table message:", message_id)
        continue

#    print("Loaded allsenders",len(allsenders),"and mapping",len(mapping),"dns mapping",len(dnsmapping))

    cur.execute('INSERT OR IGNORE INTO State (state_name) VALUES ( ? )', (record_json["state_ut"], ))
    conn.commit()
    cur.execute('SELECT state_id FROM State WHERE state_name=? LIMIT 1', (record_json["state_ut"], ))
    try:
            row = cur.fetchone()
            state_id = row[0]
    except:
            print('Could not retrieve State id',record_json["state_ut"])
            break

    cur.execute('INSERT OR IGNORE INTO Year_Range (year_range) VALUES ( ? )', (record_json["year"], ) )
    conn.commit()
    cur.execute('SELECT year_id FROM Year_Range WHERE year_range=? LIMIT 1', (record_json["year"], ))
    try:
        row = cur.fetchone()
        year_id = row[0]
    except:
        print('Could not retrieve subject id',record_json["year"])
        break
    # print(sender_id, subject_id)
    cur.execute("""INSERT OR IGNORE INTO School_Computers 
     (id, 
     fk_state_id, 
     fk_year_id, 
     primary_only,
     primary_with_u_primary,
     primary_with_u_primary_sec_hrsec,
     u_primary_only,
     u_primary_with_sec_hrsec,
     primary_with_u_primary_sec,
     u_primary_with_sec,
     sec_only,
     sec_with_hrsec,
     hrsec_only,
     all_schools)
     VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,?,? )""",
            (message_id, state_id, year_id,
             record_json["primary_only"],
             record_json["primary_with_u_primary"],
             record_json["primary_with_u_primary_sec_hrsec"],
             record_json["u_primary_only"],
             record_json["u_primary_with_sec_hrsec"],
             record_json["primary_with_u_primary_sec"],
             record_json["u_primary_with_sec"],
             record_json["sec_only"],
             record_json["sec_with_hrsec_"],
             record_json["hrsec_only"],
             record_json["all_schools"]) )
    conn.commit()
    cur.execute('SELECT id FROM School_Computers WHERE id=? LIMIT 1', ( message_id, ))
    try:
        row = cur.fetchone()
        message_id = row[0]
    except:
        print('Could not retrieve row id',message_id)
        break

cur.close()
cur_1.close()
