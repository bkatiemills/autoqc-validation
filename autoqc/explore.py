import sqlite3, pymongo
import numpy as np
import io

# sql
conn = sqlite3.connect("iquod.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# mongo
client = pymongo.MongoClient("mongodb://iquoddb/")
db = client["iquod"]
collection = db["wod"]

cur.execute("SELECT * FROM quota WHERE year==1991")
rows = cur.fetchall()

#column_names = [description[0] for description in cur.description]
#print(column_names)

total = 0
unique_match = 0
no_match = 0
too_many_matches = 0
for row in rows:
    if row['year'] != 1991:
        continue
    datenum = row['year']*10000+row['month']*100+row['day']
    window = 0.01
    polygon = {
        "type": "Polygon",
        "coordinates": [[
            [row['long'] - window, row['lat'] - window],
            [row['long'] - window, row['lat'] + window],
            [row['long'] + window, row['lat'] + window],
            [row['long'] + window, row['lat'] - window],
            [row['long'] - window, row['lat'] - window]  # close polygon
        ]]
    }

    query = {
        "geolocation": {
            "$geoWithin": {
                "$geometry": polygon
            }
        },
        "date": datenum
    }

    matches = list(collection.find(query))
    try:
        matches = [x for x in matches if abs(x['time']%1 - row['time']/24)<0.001]
    except:
        pass

    total += 1
    if len(matches) == 0:
        no_match += 1
        print(datenum, row['time'], row['long'], row['lat'], row['probe'])
    if len(matches) == 1:
        unique_match += 1
    if len(matches) > 1:
        too_many_matches += 1
        #print(datenum, row['time'], row['long'], row['lat'], row['probe'])
        #print(matches)

print(total, no_match, unique_match, too_many_matches)
