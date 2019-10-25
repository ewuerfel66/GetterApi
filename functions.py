# System Imports
import os
import requests
import json
import time
import datetime

# API Tokens 
nasa_lance_token = 'C751EA24-F34E-11E9-9D0F-ABF3207B60E0'
open_weather_token = ''

# DB Imports
# from .models import Modis, db

# DS Logic imports
import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
import feedparser


# Functions 

# MODIS Functions
def pull_modis():
    """
    Get latest modis data.
    """
    print("pulling modus")
    # time.sleep(1)

    url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/MODIS_C6_USA_contiguous_and_Hawaii_24h.csv"
    df = pd.read_csv(url, sep=",")
    print("got dataframe ", df.shape)
    return df

def process_live_data(original_df):
    """
    Pre processes live data to match pipeline expectations.
    """
    print("process_live_data!")
    df = original_df.copy()
    # process satellite labels
    df["satellite"] = df["satellite"].replace({"T": "Terra", "A": "Aqua"})

    # process time features
    df["acq_time"] = (df["acq_time"] // 100) * 60 + (df["acq_time"] % 100)
    df["timestamp"] = df.apply(
        lambda x: datetime.datetime.strptime(x["acq_date"], "%Y-%m-%d")
        + datetime.timedelta(minutes=x["acq_time"]),
        axis=1,
    )
    df["month"] = df["timestamp"].dt.month
    df["week"] = df["timestamp"].dt.weekofyear
    df.drop(columns=["acq_date", "acq_time", "timestamp"], inplace=True)

    return df
    
    
# prob need a function to check if  user input is within an already checked radius
# so as not to exceed request limit of Open weather data.
def haversine(lon1, lat1, lon2, lat2):
    """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """

    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 3956  # radius of earth in miles mean of  poles and equator radius
    return c * r


# Function to pull all fires
def fires_list():
    url = 'https://inciweb.nwcg.gov/feeds/rss/incidents/'
    fires = feedparser.parse(url)
    rss_fires = []
    for entry in fires.entries:
    # Return a dict for each fire with name and location
        fire_dict = {'name': entry.title, 'location': entry.where.coordinates}
        rss_fires.append(fire_dict)
    return rss_fires


def label_fires(df):
    print('labelling data')
    # Instantiate labels list
    labels = []
    
    # Get lats and lons from df
    lats = df['latitude'].tolist()
    lons = df['longitude'].tolist()
    
    # Pull confirmed fires
    fires = fires_list()
    locations = [entry['location'] for entry in fires]
    
    # loop data points
    for n in range(len(lats)):
        # loop fires
        for fire in locations:
            distance = haversine(lons[n], lats[n], fire[1], fire[0])
            label = 0
            if distance < 0.3:
                label = 1
                labels.append(label)
                break
            else:
                pass

        if label != 1:
            labels.append(label)
            
    # append labels to df
    labelled_df = df.copy()
    labelled_df['labels'] = labels
    
    return labelled_df


## Notes 

# open weather has a retangular box api call that may solve this problem 