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

url_provider = 'https://api.openweathermap.org/data/2.5/weather?q=zurich&units=metric&appid=a77012e3a39b82c2e993182644693cca'

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
    weather_data = r.json()  # Store the entire JSON response
    
    temperature = weather_data['main']['temp']
    weather_id = weather_data['weather'][0]['id']
    lat = weather_data['coord']['lat']
    lon = weather_data['coord']['lon']
    temp_feels_like = weather_data['main']['feels_like']
    # When was it last updated
    last_updated = weather_data['dt']
    # Convert to datetime and adjust to local time
    dt = datetime.datetime.fromtimestamp(last_updated) +datetime.timedelta(hours=1)
    # Add the timestamp as an additional column
    time_request = dt

    cur.execute("CREATE TABLE IF NOT EXISTS public.weather (id SERIAL PRIMARY KEY, temperature varchar,weather_id varchar, lat varchar, lon varchar, temp_fees_like varchar, time_request timestamp );")



    try:
        cur.execute("INSERT INTO public.weather (temperature, weather_id, lat, lon, temp_fees_like, time_request) \
                     VALUES (%s, %s, %s, %s, %s, %s);", (temperature, weather_id, lat, lon, temp_feels_like, time_request))
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print(e)
    cur.close()
    conn.close()

    return "Execution successful"
