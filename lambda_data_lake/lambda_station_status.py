# Import necessary packages
import json
import pandas as pd
import requests
import datetime
import psycopg2
import os

# Access details for database
ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']

# Data sources for shared mobility services
url_station_information = 'https://sharedmobility.ch/station_information.json'
url_station_status = 'https://sharedmobility.ch/station_status.json'
url_provider = 'https://sharedmobility.ch/providers.json' 

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
    r = requests.get(url_station_status)
    # When was it last updated
    last_updated = r.json()['last_updated']
    # Convert to datetime and adjust to local time
    dt = datetime.datetime.fromtimestamp(last_updated) + datetime.timedelta(hours=1)
    # Get data on stations
    data = pd.DataFrame(r.json()['data']['stations'])
    # Add the timestamp as additional column to stations data
    data['last_updated'] = dt

    # Get information on stations
    r = requests.get(url_station_information)
    data_station = pd.DataFrame(r.json()['data']['stations'])
    
    # Get data on providers
    r = requests.get(url_provider)
    data_provider = pd.DataFrame(r.json()['data']['providers'])

    # Merge the data
    data = data.merge(data_provider, left_on='provider_id', right_on='provider_id').merge(data_station, left_on='station_id', right_on='station_id')
    # Filter only area around Zurich
    data = data[((data['lat']>47.31460345103779) & (data['lat']<47.43742324709611)) & ((data['lon']>8.440539921109622)&(data['lon']<8.634235720318037))]
    # Filter only relevant vehicle types
    vehicle_type = ['Bike', 'E-Bike', 'E-CargoBike']
    data = data[data['vehicle_type'].isin(vehicle_type)]
    print(data.head(5))

    # Create table in database if it doesn't already exist
    cur.execute("CREATE TABLE IF NOT EXISTS public.station_status (station_id varchar, is_installed boolean, is_renting boolean, is_returning boolean, number_vehicles_available int, last_updated timestamp);")

    # Load the data to the database
    try:
        for row in data.iterrows():
            #print(row[1])
            cur.execute(f"INSERT INTO public.station_status (station_id, is_installed, is_renting, is_returning, number_vehicles_available, last_updated) \
                          VALUES ('{row[1][0]}', {row[1][1]}, {row[1][2]}, {row[1][3]}, {row[1][5]}, '{row[1][8]}');")
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print (e)

    # Close database connection
    cur.close()
    conn.close()
    
    return print("Execution successful")
