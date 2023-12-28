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
    weather_info_data = r.json()  # Store the entire JSON response
    
    weather_id = weather_info_data['weather'][0]['id']
    description = weather_info_data['weather'][0]['description']
    main = weather_info_data['weather'][0]['main']


    cur.execute("CREATE TABLE IF NOT EXISTS public.weather_info ( weather_id varchar, description varchar, main varchar );")



    try:
        cur.execute("INSERT INTO public.weather_info (weather_id, description,main) \
                     VALUES (%s, %s, %s);", (weather_id, description,main))
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print(e)
    cur.close()
    conn.close()

    return "Execution successful"