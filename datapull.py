import sqlite3
import time
import ssl
import urllib.request, urllib.parse, urllib.error
from urllib.parse import urljoin
from urllib.parse import urlparse
import re
from datetime import datetime, timedelta
import json

# Not all systems have this so conditionally define parser
try:
    import dateutil.parser as parser
except:
    pass

def parsemaildate(md) :
    # See if we have dateutil
    try:
        pdate = parser.parse(tdate)
        test_at = pdate.isoformat()
        return test_at
    except:
        pass

    # Non-dateutil version - we try our best

    pieces = md.split()
    notz = " ".join(pieces[:4]).strip()

    # Try a bunch of format variations - strptime() is *lame*
    dnotz = None
    for form in [ '%d %b %Y %H:%M:%S', '%d %b %Y %H:%M:%S',
        '%d %b %Y %H:%M', '%d %b %Y %H:%M', '%d %b %y %H:%M:%S',
        '%d %b %y %H:%M:%S', '%d %b %y %H:%M', '%d %b %y %H:%M' ] :
        try:
            dnotz = datetime.strptime(notz, form)
            break
        except:
            continue

    if dnotz is None :
        # print 'Bad Date:',md
        return None

    iso = dnotz.isoformat()

    tz = "+0000"
    try:
        tz = pieces[4]
        ival = int(tz) # Only want numeric timezone values
        if tz == '-0000' : tz = '+0000'
        tzh = tz[:3]
        tzm = tz[3:]
        tz = tzh+":"+tzm
    except:
        pass

    return iso+tz

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect(r'c:\python\data_analysis\database\datadump.sqlite')
cur = conn.cursor()

apikey = 'YOUR KEY'
#baseurl = "https://api.data.gov.in/resource/dd29fc42-00c1-4e55-94e9-d9eda13b5d5d?"
#State Level Consumer Price Index (Rural/Urban) Up to January 2015:
#baseurl = "https://api.data.gov.in/resource/d5effc0c-a79f-4867-8843-4442f9ae9060?"
#Percentage of Schools with Computers from 2013-14 to 2015-16:
baseurl = "https://api.data.gov.in/resource/8ee1905b-5949-4a60-9954-18776659ec44?"

cur.execute('''CREATE TABLE IF NOT EXISTS School_With_Computers
    (id INTEGER UNIQUE primary key not null, state_ut TEXT, status TEXT,
     offset INTEGER, count INTEGER, record BLOB)''')

#Ask user if wanted to start fresh

inp = input("Do you want to start from a fresh database OR from where you left? (Y/N): ")
if len(inp) > 0:
    inp = inp.upper()
    if inp == 'Y':
        cur.execute("Delete From School_With_Computers")
        conn.commit()
# Pick up where we left off
start = None
cur.execute('SELECT max(id) FROM School_With_Computers' )
try:
    row = cur.fetchone()
    if row is None :
        start = 0
    else:
        start = row[0]
except:
    start = 0

if start is None : start = 0

many = 0
count = 0
fail = 0
while True:
    if ( many < 1 ) :
        conn.commit()
        sval = input('How many messages:')
        if ( len(sval) < 1 ) : break
        many = int(sval)

    start = start + 1
    cur.execute('SELECT id FROM School_with_Computers WHERE id=?', (start,) )
    try:
        row = cur.fetchone()
        if row is not None : continue
    except:
        row = None

    many = many - 1
    key = urllib.parse.urlencode({"format":'json', "api-key":apikey, "offset":start, "limit":1})
    url = baseurl + key
#    print(" Retrieving url:", url)

    api_data = "None"
    try:
        # Open with a timeout of 30 seconds
        document = urllib.request.urlopen(url, None, 30, context=ctx)
        api_data = document.read().decode()

#        print(json.dumps(api_json, indent = 2))

#        break
        if document.getcode() != 200 :
            print("Error code=",document.getcode(), url)
            break
    except KeyboardInterrupt:
        print('')
        print('Program interrupted by user...')
        break
    except Exception as e:
        print("Unable to retrieve or parse page",url)
        print("Error",e)
        fail = fail + 1
        if fail > 2 : break
        continue
    api_json = json.loads(api_data)
    print(url,len(api_json))


    if api_json["status"] != 'ok' or api_json["status"] == None or len(api_json) == 0:
        print(api_json["status"])
        print("Bad Satatus or empty JSON from API")
        fail = fail + 1
        if fail > 1 : break
        continue

    status = api_json["status"]
    try:
        offset = api_json["offset"]
    except:
        print(api_json)
        print("Could not find offset data in the API response")
        fail = fail + 1
        if fail > 1 : break
        continue
    try:
        count_api = api_json["count"]
    except:
        print(api_json)
        print("Could not find count data in the API response")
        fail = fail + 1
        if fail > 1 : break
        continue

    print("API call Status:", status, offset, count_api, end = ' ')

    try:
        record = api_json["records"][0]
    except:
        print(api_json)
        print("Could not find record data in the API response")
        fail = fail + 1
        if fail > 1 : break
        continue
    try:
        state = api_json["records"][0]["state_ut"]
    except:
        print(api_json)
        print("Could not find State data in the API response")
        fail = fail + 1
        if fail > 1 : break
        continue
    try:
        year = api_json["records"][0]["year"]
    except:
        print(api_json)
        print("Could not find year data in the API response")
        fail = fail + 1
        if fail > 1 : break
        continue
    count = count + 1
    # Reset the fail counter
    fail = 0
    print(state, year)
    recordstr = json.dumps(record)
    cur.execute('''INSERT OR IGNORE INTO School_With_Computers (id, state_ut, status,
     offset, count, record)
        VALUES ( ?, ?, ?, ?, ?, ? )''', ( start, state, status, offset, count_api, recordstr.encode()))
    if count % 5 == 0 : conn.commit()
    if count % 10 == 0 : time.sleep(2)

conn.commit()
cur.close()
