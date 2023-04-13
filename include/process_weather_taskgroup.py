import json
import logging
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

EXTRACT_WEATHER_DATA_TASK_ID="extract_data"  
PROCESS_WEATHER_DATA_TASK_ID="process_data"  

def process_weather_func(ti, group_id):
    info = ti.xcom_pull(f"{group_id}.{EXTRACT_WEATHER_DATA_TASK_ID}")
    timestamp = info["dt"]
    temp = info["temp"]
    humidity = info["humidity"]
    cloudiness = info["clouds"]
    windSpeed = info["wind_speed"]
    logging.info(f"Time: {timestamp}. Temp: {temp}. Humidity: {humidity}. Cloudiness: {cloudiness}. WindSpeed: {windSpeed}.")
    return timestamp, temp, humidity, cloudiness, windSpeed


def process_weather_taskgroup(dag, group_id, endpoint, endpoint_data, response_filter):
    with TaskGroup(group_id=group_id, dag=dag) as process_weather:
        extract_data = SimpleHttpOperator(
            task_id = EXTRACT_WEATHER_DATA_TASK_ID,
            http_conn_id = "weather_api_con",
            endpoint = endpoint,
            data = endpoint_data,
            method = "GET",
            response_filter = response_filter,
            log_response = True
        )

        process_data = PythonOperator(
            task_id = PROCESS_WEATHER_DATA_TASK_ID,
            python_callable = process_weather_func,
            op_kwargs= {"group_id": group_id}
        )

        extract_data >> process_data
        
    return process_weather