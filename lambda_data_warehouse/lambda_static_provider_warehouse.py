import json
import psycopg2
import os
import pandas as pd
import requests
import urllib.request

ENDPOINT1 = os.environ['ENDPOINT1']
DB_NAME1 = os.environ['DB_NAME1']
USERNAME1 = os.environ['USERNAME1']
PASSWORD1 = os.environ['PASSWORD1']

ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']


# SQL query to fetch data from the source table
sql_provider = """
SELECT provider_id, name AS provider_name, vehicle_type 
FROM public.providers p 
WHERE vehicle_type IN ('E-Scooter', 'Bike', 'E-Bike', 'E-CargoBike')
GROUP BY 1,2,3
"""



def lambda_handler(event, context):
    try:
        print("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to connect to the database')
        }

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the Database")
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to create a database cursor')
        }

    provider = pd.read_sql(sql_provider, conn)
    print(provider)
    
 

    # Connect to the data warehouse
    try:
        print("host={} dbname={} user={} password={}".format(ENDPOINT1, DB_NAME1, USERNAME1, PASSWORD1))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT1, DB_NAME1, USERNAME1, PASSWORD1))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to connect to the data warehouse')
        }

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the Database")
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to create a database cursor')
        }

    # Auto commit 
    conn.set_session(autocommit=True)

    # Load data into the provider table
    cur.execute("CREATE TABLE IF NOT EXISTS public.provider (provider_id varchar, name varchar, vehicle_type varchar);")
  
    try:
        for row in provider.itertuples(index=False):
            cur.execute(f"INSERT INTO public.provider (provider_id, name, vehicle_type) \
                        VALUES ('{row[0]}', '{row[1]}', '{row[2]}');")
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to insert rows into the provider table')
        }
    finally:
        cur.close()
        conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Execution successful')
    }
