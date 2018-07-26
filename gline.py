import sqlite3
import time
import zlib

conn = sqlite3.connect(r'c:\users\biswajit\coursera\dta_analysis\database\index.sqlite')
cur = conn.cursor()

while True:
    year = input("Enter yer to for which you want to view chart")

    if len(year) < 1:
        print("Enter year to continue")
        continue
    try:
        year = int(year)
        break
    except:
        print("Enter numeric year")
        continue

cur.execute("""SELECT year_id FROM Year_Range
               WHERE year_range LIKE '%?%'""", (year,))

row = cur.fetchone()
if row == None:
    print("Year not found in datawarehouse")
    quit()

year_id = row[0]

cur.execute('SELECT id, guid,sender_id,subject_id,sent_at FROM Messages')
messages = dict()
for message_row in cur :
    messages[message_row[0]] = (message_row[1],message_row[2],message_row[3],message_row[4])

print("Loaded messages=",len(messages),"senders=",len(senders))

sendorgs = dict()
for (message_id, message) in list(messages.items()):
    sender = message[1]
    pieces = senders[sender].split("@")
    if len(pieces) != 2 : continue
    dns = pieces[1]
    sendorgs[dns] = sendorgs.get(dns,0) + 1

# pick the top schools
orgs = sorted(sendorgs, key=sendorgs.get, reverse=True)
orgs = orgs[:10]
print("Top 10 Oranizations")
print(orgs)

counts = dict()
months = list()
# cur.execute('SELECT id, guid,sender_id,subject_id,sent_at FROM Messages')
for (message_id, message) in list(messages.items()):
    sender = message[1]
    pieces = senders[sender].split("@")
    if len(pieces) != 2 : continue
    dns = pieces[1]
    if dns not in orgs : continue
    month = message[3][:7]
    if month not in months : months.append(month)
    key = (month, dns)
    counts[key] = counts.get(key,0) + 1

months.sort()
# print counts
# print months

fhand = open('gline.js','w')
fhand.write("gline = [ ['Year'")
for org in orgs:
    fhand.write(",'"+org+"'")
fhand.write("]")

for month in months:
    fhand.write(",\n['"+month+"'")
    for org in orgs:
        key = (month, org)
        val = counts.get(key,0)
        fhand.write(","+str(val))
    fhand.write("]");

fhand.write("\n];\n")
fhand.close()

print("Output written to gline.js")
print("Open gline.htm to visualize the data")
