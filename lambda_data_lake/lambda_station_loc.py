# Import necessary packages
import json
import pandas as pd
import numpy as np
import geopandas as gpd
import requests
import datetime
import psycopg2
import os

# Access details for database
ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']

# Data sources for shared mobility services and polygons of neigborhoods in Zurich
url_station_information = 'https://sharedmobility.ch/station_information.json'
url_provider = 'https://sharedmobility.ch/providers.json'
fp = "https://www.ogd.stadt-zuerich.ch/wfs/geoportal/Statistische_Quartiere?service=WFS&version=1.1.0&request=GetFeature&outputFormat=GeoJSON&typename=adm_statistische_quartiere_v"

# Lambda function
def lambda_handler(event, context):
    # Connect to database using access details defined above
    try:
        print("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    # Create database cursor
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)

    # Auto commit changes to database
    conn.set_session(autocommit=True)
    
    # Get data on stations
    r = requests.get(url_station_information)
    data = pd.DataFrame(r.json()['data']['stations'])

    # Get data on providers
    r = requests.get(url_provider)
    data_provider = pd.DataFrame(r.json()['data']['providers'])

    # Merge the data
    data = data.merge(data_provider, left_on='provider_id', right_on='provider_id')
    # Filter only area around Zurich
    data = data[((data['lat']>47.31460345103779) & (data['lat']<47.43742324709611)) & ((data['lon']>8.440539921109622)&(data['lon']<8.634235720318037))]
    # Filter only relevant vehicle types
    vehicle_type = ['E-Scooter', 'Bike', 'E-Bike', 'E-CargoBike']
    data = data[data['vehicle_type'].isin(vehicle_type)]
    # Change to geodata-format
    data = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data.lon,data.lat))

    # Load geodata from Zurich
    map_df = gpd.read_file(fp)
    # Change projection system  to match the one of stations data
    esg_epsg = 4326 # # EPSG code for ESG
    map_df = map_df.to_crs(epsg=esg_epsg)

    # Assign locations to neighborhoods
    data['qname'] = np.nan
    data['qnr'] = ''
    for idx in range(map_df.shape[0]):
        # For every address, find if they reside within a neighborhood
        pip = data.within(map_df.loc[idx, 'geometry'])
        if pip.sum() > 0: # we found where some of the addresses reside at map_df.loc[idx]
            data.loc[pip, 'qname']  = map_df.loc[idx, 'qname']
            data.loc[pip, 'qnr']  = map_df.loc[idx, 'qnr']

    # Drop rows conaining null values (not in Zurich)
    data.dropna(subset=['qname'], inplace=True)
    print(data.head(5))

    # Create table in database if it doesn't already exist
    cur.execute("CREATE TABLE IF NOT EXISTS public.station_loc (station_id varchar, name varchar, provider_id varchar, lat float8, lon float8, \
     qname varchar, qnr int);")

    # Load the data to the database
    try:
        for row in data.iterrows():
            cur.execute(f"INSERT INTO public.station_loc (station_id, name, provider_id, lat, lon, qname, qnr) \
            VALUES ('{row[1][0]}', '{row[1][1]}', '{row[1][4]}', {row[1][2]}, {row[1][3]}, '{row[1][23]}', '{row[1][24]}');")
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print (e)

    # Close database connection
    cur.close()
    conn.close()
    
    return print("Execution successful")
