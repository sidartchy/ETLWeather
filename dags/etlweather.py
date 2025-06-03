from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta
import requests
import json

# Latitude and Longitude for the location 
LATITUDE = '27.7103'
LONGITUDE = '85.3222' # Kathmandu, Nepal

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),  # Start from yesterday
}

with DAG(
    dag_id='etl_weather_data',
    default_args=default_args,
    schedule  = '@daily',
    catchup=False
    ) as dag:

    #task to fetch weather data from Open Meteo API
    @task()
    def extract_weather_data():
        '''
        Fetch weather data from Open Meteo API
        '''
        #Use HttpHook to make the API call
        http_hook = HttpHook(method='GET', http_conn_id=API_CONN_ID)

        ## Define the API endpoint and parameters

        ## https://api.open-meteo.com
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        # params = {
        #     'latitude': LATITUDE,
        #     'longitude': LONGITUDE,
        #     'hourly': 'temperature_2m, precipitation_sum, windspeed_10m'
        # }

        ## make the api call
        response = http_hook.run(endpoint)

        if response.status_code ==200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")

    #task to transform
    @task()
    def transform_weather_data(weather_data):
        '''
        Transform the weather data :)'''
        
        #this may include cleaning, filtering, or aggregating the data 
        # I'm only getting the current_weather data here:
        current_weather = weather_data['current_weather']
        transformed_data = {
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'weathercode': current_weather['weathercode'],
            'time': current_weather['time']
        }
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        '''
        Load the transformed data into any db:
        Here, I'l be using postgres
        '''
        ## postgres hook to connect to the db
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # create table if not exists
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS weather_data (
                       time TIMESTAMP DEFAULT CURRENT_TIMESTAMP PRIMARY KEY,
                       windspeed FLOAT,
                       temperature FLOAT,
                       weathercode INT
                       );
                       """
                       )
        # insert the transformed data in table
        cursor.execute("""
                       INSERT INTO weather_data (time, windspeed, temperature, weathercode)
                       VALUES (%s, %s, %s, %s)""",
                       (transformed_data['time'],
                        transformed_data['windspeed'],
                        transformed_data['temperature'],
                        transformed_data['weathercode']
                        ))
        
        # commit the changes 
        conn.commit()
        cursor.close()


    # DAG workflow - ETL pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)




        


        

