from datetime import date
import logging
import json
from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator

BRANCH_EXTRACT_WEATHER_DATA_TASK_ID = "branch_extract_data"
EXTRACT_CURRENT_WEATHER_DATA_TASK_ID = "extract_current_data"  
EXTRACT_HISTORY_WEATHER_DATA_TASK_ID = "extract_history_data"  
PROCESS_WEATHER_DATA_TASK_ID = "process_data"  
INJECT_WEATHER_DATA_TASK_ID = "inject_data"  

def branch_extract_data(ti, current_full_id):
    logging.info(f"Execution Date: {ti.execution_date}")
    if ti.execution_date.date() == date.today():
        return f"{current_full_id}.{EXTRACT_CURRENT_WEATHER_DATA_TASK_ID}"
    else:    
        return f"{current_full_id}.{EXTRACT_HISTORY_WEATHER_DATA_TASK_ID}"

def extract_data(task_id, endpoint, endpoint_data, response_filter):
        return SimpleHttpOperator(
            task_id = task_id,
            http_conn_id = "weather_api_con",
            endpoint = endpoint,
            data = endpoint_data,
            method = "GET",
            response_filter = response_filter,
            log_response = True
        )

def process_data(ti, current_full_id):
    info = ti.xcom_pull(task_ids = [f"{current_full_id}.{EXTRACT_CURRENT_WEATHER_DATA_TASK_ID}", f"{current_full_id}.{EXTRACT_HISTORY_WEATHER_DATA_TASK_ID}"])[0]
    timestamp = info["dt"]
    temp = info["temp"]
    humidity = info["humidity"]
    cloudiness = info["clouds"]
    windSpeed = info["wind_speed"]
    logging.info(f"Time: {timestamp}. Temp: {temp}. Humidity: {humidity}. Cloudiness: {cloudiness}. WindSpeed: {windSpeed}.")
    return timestamp, temp, humidity, cloudiness, windSpeed

def process_city_weather_taskgroup(dag, parent_full_id, group_id, city):
    with TaskGroup(group_id = group_id, dag = dag) as process_weather:
        current_full_id = f"{parent_full_id}.{group_id}"
        
        branch_extract_data_task = BranchPythonOperator(
            task_id = BRANCH_EXTRACT_WEATHER_DATA_TASK_ID,
            python_callable = branch_extract_data,
            op_kwargs = { 'current_full_id': current_full_id },
            provide_context = True
        )

        extract_current_data_task = extract_data(EXTRACT_CURRENT_WEATHER_DATA_TASK_ID, "data/3.0/onecall",
                                                 {"appId": Variable.get("WEATHER_API_KEY"), "lat": city['lat'], "lon": city['lon']},
                                                 lambda x: json.loads(x.text)["current"])
        
        extract_history_data_task = extract_data(EXTRACT_HISTORY_WEATHER_DATA_TASK_ID, "data/3.0/onecall/timemachine",
                                                 {"appId": Variable.get("WEATHER_API_KEY"), "lat": city['lat'], "lon": city['lon'], "dt": "{{ execution_date.timestamp() | int }}"},
                                                 lambda x: json.loads(x.text)["data"][0])

        process_data_task = PythonOperator(
            task_id = PROCESS_WEATHER_DATA_TASK_ID,
            python_callable = process_data,
            op_kwargs = { 'current_full_id': current_full_id },
            trigger_rule = 'none_failed'
        )

        inject_data = PostgresOperator(
            task_id = INJECT_WEATHER_DATA_TASK_ID,
            postgres_conn_id = "measurements_db",
            sql = f"""
                INSERT INTO measurements (execution_time, temperature, humidity, cloudiness, windSpeed, city) VALUES
                (to_timestamp({{{{ti.xcom_pull(task_ids='{current_full_id}.{PROCESS_WEATHER_DATA_TASK_ID}')[0]}}}}),
                {{{{ti.xcom_pull(task_ids='{current_full_id}.{PROCESS_WEATHER_DATA_TASK_ID}')[1]}}}},
                {{{{ti.xcom_pull(task_ids='{current_full_id}.{PROCESS_WEATHER_DATA_TASK_ID}')[2]}}}},
                {{{{ti.xcom_pull(task_ids='{current_full_id}.{PROCESS_WEATHER_DATA_TASK_ID}')[3]}}}},
                {{{{ti.xcom_pull(task_ids='{current_full_id}.{PROCESS_WEATHER_DATA_TASK_ID}')[4]}}}},
                '{city["name"]}');
            """
        )

        branch_extract_data_task >> [extract_current_data_task, extract_history_data_task] >> process_data_task >> inject_data
        
    return process_weather