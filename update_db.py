# Sqlite3 imports
import psycopg2

# Local imports
from functions import (
    pull_modis, 
    process_live_data, 
    label_fires,
    haversine
)

# DS Logic imports
import pandas as pd
import feedparser


# Get MODIS Data and label it
dirty_df = pull_modis()
df = process_live_data(dirty_df)
labelled_df = label_fires(df)


# Credentials
dbname = 'polpmmvo'
user = 'polpmmvo'
password = '' # Don't commit this!
host = 'salt.db.elephantsql.com'

# Establish connection
pg_conn = psycopg2.connect(dbname=dbname, user=user,
                       password=password, host=host)

# Instantiate cursor
pg_curs = pg_conn.cursor()


# Send data to db
print('cleaning data')
dirty_rows = labelled_df.values

# Clean up rows
rows = []

for row in dirty_rows:
    rows.append(tuple(row))

print('adding data to DB')
# Loop over the array to write rows in the DB
for row in rows:
    insert = """
    INSERT INTO training
    (latitude, longitude, brightness, scan, track,
     satellite, confidence, version, bright_t31, frp,
     daynight, month, week, fire)
    VALUES 
    """ + str(row) + ';'
    
    pg_curs.execute(insert)

# Save and finish session
pg_curs.close()
pg_conn.commit()

print('all done!')