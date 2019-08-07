import psycopg2
import io
import csv
from urllib import request

#connect to our postgress server
conn = psycopg2.connect(dbname="postgres", user="postgres")
cursor = conn.cursor()
#create table to save data in
cursor.execute("""
CREATE TABLE weather(FID integer PRIMARY KEY,
YEAR integer,
MONTH integer,
DAY integer,
AD_TIME	text,
BTID integer,
NAME text,
LAT	text,
LONG text,
WIND_KTS integer,
PRESSURE integer,
CAT text,
BASIN text,
Shape_Leng text )
""")

#download the raw data
response = request.urlopen('https://dq-content.s3.amazonaws.com/251/storm_data.csv')
reader = csv.reader(io.TextIOWrapper(response))
#skip header
next(reader)
#save data into table
for line in reader:
    cursor.execute("INSERT INTO weather VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",line)

#close connection
conn.close()
