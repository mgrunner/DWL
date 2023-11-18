import json
import pandas as pd
import numpy as np
import geopandas as gpd
import requests
import datetime
import psycopg2
import os

ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']

url_free_float = 'https://sharedmobility.ch/free_bike_status.json'
url_provider = 'https://sharedmobility.ch/providers.json'
fp = "https://www.ogd.stadt-zuerich.ch/wfs/geoportal/Statistische_Quartiere?service=WFS&version=1.1.0&request=GetFeature&outputFormat=GeoJSON&typename=adm_statistische_quartiere_v"


def lambda_handler(event, context):

    try:
        print("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)

    # Auto commit 
    conn.set_session(autocommit=True)
    
    # Get data on vehicles
    r = requests.get(url_free_float)
    # When was it last updated
    last_updated = r.json()['last_updated']
    # Convert to datetime and adjust to local time
    dt = datetime.datetime.fromtimestamp(last_updated) + datetime.timedelta(hours=1)
    data = pd.DataFrame(r.json()['data']['bikes'])
    # Add the timestamp as additional column
    data['last_updated'] = dt

    # Get data on provider
    r = requests.get(url_provider)
    data_provider = pd.DataFrame(r.json()['data']['providers'])

    # Merge the data
    data = data.merge(data_provider, left_on='provider_id', right_on='provider_id')
    # Filter only area around Zurich
    data = data[((data['lat']>47.31460345103779) & (data['lat']<47.43742324709611)) & ((data['lon']>8.440539921109622)&(data['lon']<8.634235720318037))]
    # Filter only relevant providers
    providers = ['bird-zurich-escooter', 'tier', 'voiscooters.com'] #'lime_zurich_ebike', 'lime_zurich_escooter', 'lime_opfikon_ebike', 'lime_opfikon_escooter'
    data = data[data['provider_id'].isin(providers)]
    # Change to geodata-format
    data = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data.lon,data.lat))

    # Load geodata from Zurich
    map_df = gpd.read_file(fp)
    # Change projection system
    esg_epsg = 4326 # # EPSG code for ESG
    map_df = map_df.to_crs(epsg=esg_epsg)

    # Assign locations to Quartiere
    data['qname'] = np.nan
    data['qnr'] = ''
    for idx in range(map_df.shape[0]):
        #For every address, find if they reside within a province
        pip = data.within(map_df.loc[idx, 'geometry'])
        if pip.sum() > 0: #we found where some of the addresses reside at map_df.loc[idx]
            data.loc[pip, 'qname']  = map_df.loc[idx, 'qname']
            data.loc[pip, 'qnr']  = map_df.loc[idx, 'qnr']

    # Drop rows conaining null values (not in Zurich)
    data.dropna(subset=['qname'], inplace=True)
    print(data.head(5))

    cur.execute("CREATE TABLE IF NOT EXISTS public.free_floating_systems_loc (vehicle_id varchar, provider_id varchar, lat float8, lon float8, \
        is_disabled boolean, is_reserved boolean, last_updated timestamp, qname varchar, qnr int);")

    try:
        for row in data.iterrows():
            #print(row[1])
            cur.execute(f"INSERT INTO public.free_floating_systems_loc (vehicle_id, provider_id, lat, lon, is_disabled, is_reserved, last_updated, qname, qnr) \
                          VALUES ('{row[1][0]}', '{row[1][5]}', {row[1][1]}, {row[1][2]}, {row[1][3]}, {row[1][4]}, '{row[1][7]}', '{row[1][21]}', '{row[1][22]}');")
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print (e)

    cur.close()
    conn.close()
    
    return print("Execution successful")