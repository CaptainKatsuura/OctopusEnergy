import airflow
import logging
import json
import requests
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from requests import Response 
from airflow.models import Variable
from sqlalchemy import types
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator



    

@dag(start_date=datetime(2025,1,1)
    , schedule_interval='@hourly'
    , catchup=False)
def octopus_consumption():
    # TASK - GET MAX INTERVAL START DATE
    sql_command_1 = 'SELECT max(interval_start) as interval_start FROM public.octopus_electricity;'
    get_latest_octopus_electric_consumption = SQLExecuteQueryOperator(
        task_id = 'get_latest_octopus_electric_consumption',
        sql = sql_command_1,
        conn_id = 'OctopusEnergy_PG',
        autocommit = True)

    @task()
    def extract_electric_consumption(ti, **kwargs):
        max_interval_start = ti.xcom_pull(task_ids='get_latest_octopus_electric_consumption')[0][0]
        max_interval_start = datetime(2024,1,1) if max_interval_start is None else max_interval_start
        max_interval_start = max_interval_start.strftime("%Y-%m-%dT%H:%M:%S")
        print(max_interval_start)
        mpan = '1200062428490'
        serial = '19L2432124'
        url = f'https://api.octopus.energy/v1/electricity-meter-points/{mpan}/meters/{serial}/consumption/'
        api_key = Variable.get('octupus_api_key')
        print('api_key')
        logging.info(api_key)
        params={'page_size':'25000','period_from':max_interval_start}
        responses = []
        while True:
            request = requests.get(url, params=params, auth = (api_key, '')).json()
            responses.extend(request['results'])
            if request['next']:
                url = request['next']
            else:
                break
        print(responses)
        df = pd.DataFrame(responses)
        df[['mpan','serial']] = [mpan,serial]
        postgres_hook = PostgresHook(postgres_conn_id="OctopusEnergy_PG")
        dtype_dic = {'consumption': types.DECIMAL(10,2), 'interval_start': types.TIMESTAMP(timezone=False),'interval_end': types.TIMESTAMP(timezone=False),'mpan': types.VARCHAR(50), 'serial': types.VARCHAR(50)}
        df.to_sql('octopus_electricity_staging', postgres_hook.get_sqlalchemy_engine(), if_exists='replace', chunksize=1000, dtype=dtype_dic, index=False)


    sql_command_2 = 'CALL public.merge_electric_consumption_staging()'
    sp_merge_electric_consumption_staging = SQLExecuteQueryOperator(
        task_id = 'sp_merge_electric_consumption_staging',
        sql = sql_command_2,
        conn_id = 'OctopusEnergy_PG',
        autocommit = True)
        
    

    #pull historic weather
    
    sql_command_3 = 'SELECT max(time_start) as time_start FROM public.historic_weather where temperature_2m is not null;'
    get_latest_historic_weather = SQLExecuteQueryOperator(
        task_id = 'get_latest_historic_weather',
        sql = sql_command_3,
        conn_id = 'OctopusEnergy_PG',
        autocommit = True)
    

    @task()
    def pull_historic_weather_data(ti, **kwargs):
        max_time_start = ti.xcom_pull(task_ids='get_latest_historic_weather')[0][0]
        max_time_start = datetime(2024,1,1) if max_time_start is None else max_time_start
        max_time_start = max_time_start.strftime("%Y-%m-%d")
        print(max_time_start)
        end_date = datetime.now().date().strftime("%Y-%m-%d")
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": 51.54,
            "longitude": 0.0807,
            "start_date": max_time_start, #use max historic weather
            "end_date": end_date, # current date
            "hourly": ["temperature_2m","precipitation"]
        }
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        # Process hourly data. The order of variables needs to be the same as requested.
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_rainfall = hourly.Variables(1).ValuesAsNumpy()
        hourly_data = {"time_start": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        )}

        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data['rainfall'] = hourly_rainfall
        hourly_data = pd.DataFrame(data = hourly_data)
        postgres_hook = PostgresHook(postgres_conn_id="OctopusEnergy_PG")
        dtype_dic = {'time_start': types.TIMESTAMP(timezone=False), 'temperature_2m': types.DECIMAL(10,5),'precipitation': types.DECIMAL(10,5)}
        hourly_data.to_sql('historic_weather_staging', postgres_hook.get_sqlalchemy_engine(), if_exists='replace', chunksize=1000, dtype=dtype_dic, index=False)
    
    sql_command_4 = 'CALL public.merge_historic_weather_staging()'
    sp_merge_historic_weather_staging = SQLExecuteQueryOperator(
        task_id = 'sp_merge_historic_weather_staging',
        sql = sql_command_4,
        conn_id = 'OctopusEnergy_PG',
        autocommit = True)
    
    #pull forecast weather
    
    @task()
    def pull_forecast_weather_data(ti, **kwargs):
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)
        # Make sure all required weather variables are listed here
        # The order of variables in hourly or daily is important to assign them correctly below
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 51.54,
            "longitude": 0.0807,
	        "past_days": 2,
            "hourly": ["temperature_2m", "precipitation"]
        }
        responses = openmeteo.weather_api(url, params=params)

        # Process first location. Add a for-loop for multiple locations or weather models
        response = responses[0]

        # Process hourly data. The order of variables needs to be the same as requested.
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()

        hourly_data = {"date": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        )}

        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["precipitation"] = hourly_precipitation

        hourly_data = pd.DataFrame(data = hourly_data)
        postgres_hook = PostgresHook(postgres_conn_id="OctopusEnergy_PG")
        dtype_dic = {'time_start': types.TIMESTAMP(timezone=False), 'temperature_2m': types.DECIMAL(10,5),'precipitation': types.DECIMAL(10,5)}
        hourly_data.to_sql('forecasted_weather_staging', postgres_hook.get_sqlalchemy_engine(), if_exists='replace', chunksize=1000, dtype=dtype_dic, index=False)

    sql_command_5 = 'CALL public.merge_forecasted_weather_staging()'
    sp_merge_forecasted_weather_staging = SQLExecuteQueryOperator(
        task_id = 'merge_forecasted_weather_staging',
        sql = sql_command_5,
        conn_id = 'OctopusEnergy_PG',
        autocommit = True)
        

    # run FE -- select historical data
        # define date range
    training_start_date, training_end_date = '2024-01-01','2024-12-31'
        # run fe script
        
    # train sarimax model
        # read FE data
        # forward selection
            # write model metadata to sql 1 model many features
            # write train/test result to sql
            # store model as file locally/store model params
            # iterate through all features
        
    # live predict all models

    get_latest_octopus_electric_consumption >> extract_electric_consumption() >> sp_merge_electric_consumption_staging
    get_latest_historic_weather >> pull_historic_weather_data() >> sp_merge_historic_weather_staging
    pull_forecast_weather_data() >> sp_merge_forecasted_weather_staging


octopus_consumption()