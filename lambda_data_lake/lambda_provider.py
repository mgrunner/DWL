import json
import pandas as pd
import requests
import datetime
import psycopg2
import os


ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']

url_provider = 'https://sharedmobility.ch/providers.json'

def lambda_handler(event, context):
    try:
        print("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except psycopg2.Error as e:
        print("Error: Could not make a connection to the Postgres database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the Database")
        print(e)

    # Auto commit
    conn.set_session(autocommit=True)

    # Get data on providers
    r = requests.get(url_provider)
    data_provider = pd.DataFrame(r.json()['data']['providers'])

    print(data_provider.head(5))

    cur.execute("CREATE TABLE IF NOT EXISTS public.providers (provider_id varchar, name varchar, vehicle_type varchar, operator varchar);")

    try:
        for row in data_provider.iterrows():
            cur.execute(f"INSERT INTO public.providers (provider_id, name, vehicle_type,operator) \
                          VALUES ('{row[1]['provider_id']}', '{row[1]['name']}', '{row[1]['vehicle_type']}','{row[1]['operator']}');")
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print(e)

    cur.close()
    conn.close()

    return "Execution successful"
