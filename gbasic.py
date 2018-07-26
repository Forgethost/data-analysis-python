import sqlite3



conn = sqlite3.connect(r'c:\python\data_analysis\database\index.sqlite')
cursor = conn.cursor()


cursor.execute("""Select year_id, year_range from Year_Range
                 Order by year_range DESC Limit 1""")
try:
    year_row = cursor.fetchone()
    latest_year_id = year_row[0]
except:
    print("No year data found.. Nothing to retrive..exiting the program")
    quit()

cursor.execute("""SELECT id,
	 State.state_name,
	 Year_Range.year_range,
     primary_only ,
     primary_with_u_primary ,
     primary_with_u_primary_sec_hrsec ,
     u_primary_only ,
     u_primary_with_sec_hrsec ,
     primary_with_u_primary_sec ,
     u_primary_with_sec ,
     sec_only ,
     sec_with_hrsec ,
     hrsec_only ,
     all_schools
     From School_Computers 
	 join
	 State 
	 join
	 Year_Range
	 ON fk_state_id = state_id
	 AND fk_year_id = year_id
     Where fk_year_id = ?
     order by all_schools desc LIMIT 10""", (latest_year_id,) )
rank = 0
print("printing top 10 State by all schools computer percentage State for latest year")
for message_row in cursor :
    rank = rank + 1
    print("State:", message_row[1], "Year:", message_row[2], "Percentage:", message_row[13], "Rank:", rank)
