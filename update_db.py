# Sqlite3 imports
import psycopg2

# Local imports
from functions import (
    pull_modis, 
    process_live_data, 
    haversine
)

# DS Logic imports
import pandas as pd


# Get MODIS Data
dirty_df = pull_modis()
df = process_live_data(dirty_df)


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
dirty_rows = df.values

# Clean up rows
rows = []

for row in dirty_rows:
    rows.append(tuple(row))

# Loop over the array to write rows in the DB
for row in rows:
    insert = """
    INSERT INTO training
    (latitude, longitude, brightness, scan, track,
     satellite, confidence, version, bright_t31, frp,
     daynight, month, week)
    VALUES 
    """ + str(row) + ';'
    
    pg_curs.execute(insert)

# Save and finish session
pg_curs.close()
pg_conn.commit()