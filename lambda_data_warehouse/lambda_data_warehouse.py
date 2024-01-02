# Import libraries
import pandas as pd
import datetime
import psycopg2
import os

# Endpoints to connect to different databases
	# datalake01
ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']
	# cycleonmg
ENDPOINT2 = os.environ['ENDPOINT2']
DB_NAME2 = os.environ['DB_NAME2']
USERNAME2 = os.environ['USERNAME2']
PASSWORD2 = os.environ['PASSWORD2']
	# cycleondw
ENDPOINT3 = os.environ['ENDPOINT3']
DB_NAME3 = os.environ['DB_NAME3']
USERNAME3 = os.environ['USERNAME3']
PASSWORD3 = os.environ['PASSWORD3']

# SQL-queries to get data from data lake
	# Get weather data from last day, creating new variable "nearest_hour" to join with availability data
sql_weather = """
select id as weather_id, description, main, temperature, temp_fees_like as temp_feels_like , date_trunc('hour', time_request  + interval '30 minute') nearest_hour
from public.weather w 
left join (
	select weather_id, description, main 
	from public.weather_info wi 
	group by 1,2,3
) info_weather on info_weather.weather_id = w.weather_id 
where date_trunc('hour', time_request  + interval '30 minute')::date = current_date -1 -- filter last day
"""
	# Get data on providers
sql_providers = """
select provider_id, "name" as provider_name, vehicle_type 
from public.providers p 
where vehicle_type in ('E-Scooter', 'Bike', 'E-Bike', 'E-CargoBike')
group by 1,2,3
"""
	# Get data on stations
sql_stations = """ 
select station_id, "name" as station_name, qnr as location_id, s.provider_id, provider_name, vehicle_type
from station_loc s 
left join (select provider_id, "name" as provider_name, vehicle_type 
from public.providers p 
group by 1,2,3) p on s.provider_id = p.provider_id 
where vehicle_type in ('E-Scooter', 'Bike', 'E-Bike', 'E-CargoBike') -- filter relevant vehicle types
"""
	# Get data for station-based systems from last day, creating new variable "nearest_hour" to join with weather data
sql_station_based = """ 
select date_trunc('hour', last_updated + interval '30 minute') nearest_hour, station_id, sum(number_vehicles_available) as no_of_vehicles
from public.station_status ss 
where date_trunc('hour', last_updated + interval '30 minute')::date = current_date -1 -- filter only last day
and is_renting = true -- filter only stations that are renting
group by 1,2
"""
	# Get data for free-floating systems from last day, creating new variable "nearest_hour" to join with weather data
sql_free_float = """ 
select provider_id, date_trunc('hour', last_updated + interval '30 minute') nearest_hour, qnr as location_id, count(*) as no_of_vehicles
from free_floating_systems_loc ffs 
where date_trunc('hour', last_updated + interval '30 minute')::date = current_date -1 -- filter last day
and is_disabled = false -- filter only vehicles that are not disabled
group by 1, 2, 3 -- group by provider, hour and neigborhood
"""

# Lambda functino
def lambda_handler(event, context):
    ###############
    # Getting the data for the last day
    ###############

    # Connect to first data lake
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

    # Load data from first data lake
    weather = pd.read_sql(sql_weather, conn)
    providers = pd.read_sql(sql_providers, conn)
    stations = pd.read_sql(sql_stations, conn)

    # Delete duplicates
    stations = stations[~stations.duplicated()]

    # Close db-connection
    cur.close()
    conn.close()

    # Connect to second data lake
    try:
        print("host={} dbname={} user={} password={}".format(ENDPOINT2, DB_NAME2, USERNAME2, PASSWORD2))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT2, DB_NAME2, USERNAME2, PASSWORD2))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
	    
    # Create database cursor
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)

    # Load data from second data lake
    station_based = pd.read_sql(sql_station_based, conn)
    free_float = pd.read_sql(sql_free_float, conn)

    # close db-connection
    cur.close()
    conn.close()

    ###############
    # Preparing the data
    ###############
    
    # Joing data sources for free floating systems
    join_free_float = free_float.merge(providers, on='provider_id', how = 'left').merge(weather, on = 'nearest_hour')
	
    # Preparing data for station-based systems (Goal: get number of vehicles by timestamp, provider and neighborhood similiar to free-floating systems)
    join_stations_based = station_based.merge(stations, on = 'station_id')
    station_based_grouped = join_stations_based.groupby(['nearest_hour', 'provider_id', 'location_id'], as_index= False).sum()[['nearest_hour', 'provider_id', 'location_id', 'no_of_vehicles']]
    join_stations_based = station_based_grouped.merge(providers, on='provider_id').merge(weather, on = 'nearest_hour')

    # Preparing the fact table
    # List of columns for fact table
    column_names_loc = ['provider_id', 'nearest_hour', 'weather_id', 'location_id', 'no_of_vehicles']
    # Select relevant colums for free-floating and station-based systems
    fact_free_float = join_free_float[column_names_loc]
    fact_station_based  = join_stations_based[column_names_loc]
    # Join data on free-floating and station-based systems
    fact = pd.concat([fact_free_float, fact_station_based])

    # Create dimension weather table
    column_names = ['weather_id', 'description', 'main', 'temperature', 'temp_feels_like']
    weather_dim = weather[column_names]

    # Create dimension time table
    time = weather[['nearest_hour']]
    weekdays = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
    time['weekday'] = time['nearest_hour'].apply(lambda x: x.weekday())
    time['weekday'] = time['weekday'].apply(lambda x: weekdays[x])
    time['week'] = time['nearest_hour'].apply(lambda x: x.isocalendar()[1])
    time['hour'] = time['nearest_hour'].dt.hour
    time['day'] = time['nearest_hour'].dt.strftime('%d')
    time['month'] = time['nearest_hour'].dt.strftime('%m')
    time['year'] = time['nearest_hour'].dt.strftime('%y')

    ###############
    # Load fact and dimension tables to data warehouse
    ###############
    # Connect to data warehouse
    try:
        print("host={} dbname={} user={} password={}".format(ENDPOINT3, DB_NAME3, USERNAME3, PASSWORD3))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT3, DB_NAME3, USERNAME3, PASSWORD3))
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

    # Create fact table in data warehouse if it doesn't already exist
    cur.execute("CREATE TABLE IF NOT EXISTS public.fact (provider_id varchar, nearest_hour timestamp, weather_id int, \
        location_id int, no_of_vehicles int);")

    # Load data of fact table
    try:
        for row in fact.iterrows():
            #print(row[1])
            cur.execute(f"INSERT INTO public.fact (provider_id, nearest_hour, weather_id, location_id, no_of_vehicles) \
                          VALUES ('{row[1][0]}', '{row[1][1]}', {row[1][2]}, {row[1][3]}, {row[1][4]});")
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print (e)
    
    # Create weather table in data warehouse if it doesn't already exist and load data
    cur.execute("CREATE TABLE IF NOT EXISTS public.weather (weather_id int, description varchar, main varchar, \
        temperature float8, temp_feels_like float8);")

    try:
        for row in weather.iterrows():
            #print(row[1])
            cur.execute(f"INSERT INTO public.weather (weather_id, description, main, temperature, temp_feels_like) \
                          VALUES ({row[1][0]}, '{row[1][1]}', '{row[1][2]}', {row[1][3]}, {row[1][4]});")
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print (e)

    # Create time table in data warehouse if it doesn't already exist and load data
    cur.execute("CREATE TABLE IF NOT EXISTS public.time (nearest_hour timestamp, weekday varchar, week int, \
        hour int, day int, month int, year int);")

    try:
        for row in time.iterrows():
            #print(row[1])
            cur.execute(f"INSERT INTO public.time (nearest_hour, weekday, week, hour, day, month, year) \
                          VALUES ('{row[1][0]}', '{row[1][1]}', {row[1][2]}, {row[1][3]}, {row[1][4]}, {row[1][5]}, {row[1][6]});")
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print (e)

    # Close database connection
    cur.close()
    conn.close()
    
    return print("Execution successful")
