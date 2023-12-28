import json
import urllib.request
import psycopg2
import os

ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']

fp = "https://www.ogd.stadt-zuerich.ch/wfs/geoportal/Statistische_Quartiere?service=WFS&version=1.1.0&request=GetFeature&outputFormat=GeoJSON&typename=adm_statistische_quartiere_v"

def lambda_handler(event, context):
    try:
        print("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))

    except psycopg2.Error as e:
        print("Error: Could not make a connection to the Postgres database")
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

    with urllib.request.urlopen(fp) as response:
        data = response.read()
    json_data = json.loads(data)

    # Create the table if it doesn't exist
    cur.execute("CREATE TABLE IF NOT EXISTS public.location (qname varchar, qnr int, kname varchar, knr int, polygon geometry(Geometry, 4326));")

    # Insert data into the table
    for feature in json_data['features']:
        properties = feature['properties']
        qname = properties.get('qname')
        qnr = properties.get('qnr')
        knr = properties.get('knr')
        kname = properties.get('kname')
        geometry = json.dumps(feature['geometry'])
        cur.execute(f"INSERT INTO public.location (qname, qnr, kname, knr, polygon) VALUES (%s, %s, %s, %s, ST_GeomFromGeoJSON(%s));", (qname, qnr, kname, knr, geometry))

    # Commit changes and close the connection
    conn.commit() 
    cur.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Execution successful')
    }
