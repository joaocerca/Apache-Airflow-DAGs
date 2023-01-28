
# imports important for Airflow
import pendulum

from pandas import DataFrame, json_normalize
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator


# Import Modules for code
import json
# import requests
import os


@dag(
    schedule_interval=None,                             # interval how often the dag will run (can be cron expression as string)
    start_date=pendulum.datetime(2023, 1, 10, tz="UTC"), # from what point on the dag will run (will only be scheduled after this date)
    catchup=False,                                      # no catchup needed, because we are running an api that returns now values
    tags=['iQair'],                      # tag the DAQ so it's easy to find in AirflowUI
)


def AirQuality():

         
    @task.python
    def extract_data():

        import requests

        iQpass = Variable.get("iQair_api_key")
        time_now = pendulum.now().strftime('%Y%m%d_T%H-%M-%S')

        # for city, details in cities.items():
        payload = {'key':iQpass, 'country':'Poland', 'state':'Pomerania', 'city':'Gdynia'}
        # payload = {'key':iQpass, 'country':city, 'state':details['state'], 'city':details['city']}                

        r = requests.get("http://api.airvisual.com/v2/city", params=payload)    
        r_string = r.json()

        with open(f'/opt/airflow/completed/api_request-{payload["city"]}-{time_now}.json', "w") as newfile:
            json.dump(r_string, newfile)            

        # print(f'The type is {type(weather_data)}')
        return r_string


    @task.python
    def transform_data(weather_quality_data):
        import pandas as pd
             
        # json.dumps transforms a python object (dict, string, etc into json string)
        air_quality_df = pd.json_normalize(weather_quality_data)

        air_quality_df.rename(columns={
            'data.city':'city',
            'data.country':'country',
            'data.location.coordinates':'location_coord',
            'data.current.pollution.ts':'cur_pol_ts',
            'data.current.pollution.aqius':'cur_pol_aquis',
            'data.current.weather.ts':'cur_weather_ts',
            'data.current.weather.tp':'cur_weather_temp'}, inplace=True)

        final_df = air_quality_df.filter(['city', 'country', 'location_coord', 'cur_pol_ts', 'cur_pol_aquis', 'cur_weather_ts', 'cur_weather_temp'])

        final_df_json = final_df.to_json(orient='records')

        final_df_dict = final_df.to_dict(orient='records')

        print(type(final_df_json))

        return final_df_dict

        # with open("/opt/airflow/completed/test.txt", "w") as newfile:
        #     newfile.write(air_quality_df_txt)        

        # print(air_quality_df)

    @task
    def insert_db_records(air_data):
        import psycopg2

        try:
            connection = psycopg2.connect(user="airflow",
                                        password="airflow",
                                        host="postgres",
                                        port="5432",
                                        database="AirData")

            cursor = connection.cursor()  

            print(type(air_data))

            sql_query = """INSERT INTO air_report (city, country, location_coord, cur_pol_ts, cur_pol_aquis, cur_weather_ts, cur_weather_temp) VALUES (%s, %s, %s, %s, %s, %s, %s);"""
            records_to_insert = (air_data[0]['city'], air_data[0]['country'], 
                                    air_data[0]['location_coord'], air_data[0]['cur_pol_ts'], 
                                    air_data[0]['cur_pol_aquis'], air_data[0]['cur_weather_ts'], 
                                    air_data[0]['cur_weather_temp'])

            cursor.execute(sql_query, records_to_insert)
            connection.commit()
            print(cursor.rowcount, " Record(s) inserted successfully into the table")
           

        except(Exception, psycopg2.Error) as error:
            
            print("Failed to insert record", error)

            if connection:
                cursor.close()
                connection.close()
                print("Connection to the DB is closed")
            
            raise Exception(error)

        finally:

            # Close db connection
            if connection:
                cursor.close()
                connection.close()
                print("Connection to the DB is closed")

    @task()
    def query_results(air_data):
        print(air_data)
 
    # Define the main flow
    
    air_data = extract_data()
    insert_db_records(transform_data(air_data))
    # weather_summary = transform(weather_data)
    # load(weather_summary)
    query_results(air_data)



# Invocate the DAG
lde_weather_dag = AirQuality()