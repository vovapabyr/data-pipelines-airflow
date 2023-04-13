from datetime import date
import json
import logging
from airflow.models import Variable
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator 
from airflow.utils.dates import days_ago
from include.process_weather_taskgroup import process_weather_taskgroup, EXTRACT_WEATHER_DATA_TASK_ID

PROCESS_CURRENT_WEATHER_GROUP_ID="process_current_weather"
PROCESS_HISTORY_WEATHER_GROUP_ID="process_history_weather"

def branch_process_weather_func(ti):
    logging.info(f"Execution Date: {ti.execution_date}")
    if ti.execution_date.date() == date.today():
        return f"{PROCESS_CURRENT_WEATHER_GROUP_ID}.{EXTRACT_WEATHER_DATA_TASK_ID}"
    else:
        ti.xcom_push(key='execution_date_timestamp', value=int(ti.execution_date.timestamp()))        
        return f"{PROCESS_HISTORY_WEATHER_GROUP_ID}.{EXTRACT_WEATHER_DATA_TASK_ID}"

with DAG(dag_id = "weather", schedule = "@daily", start_date = days_ago(2)) as dag:
    create_table_postgres_task = PostgresOperator(
        task_id = "create_table_postgres",
        postgres_conn_id = "measurements_db",
        sql = r"""
            CREATE TABLE IF NOT EXISTS measurements (
                execution_time TIMESTAMP NOT NULL,
                temperature FLOAT);
        """
    )

    run_migrations_postgres_task = PostgresOperator(
        task_id = "run_migrations_postgres",
        postgres_conn_id = "measurements_db",
        sql = r"""
            ALTER TABLE measurements
            ADD COLUMN IF NOT EXISTS humidity FLOAT,
            ADD COLUMN IF NOT EXISTS cloudiness FLOAT,
            ADD COLUMN IF NOT EXISTS windSpeed FLOAT;
        """
    )

    branch_process_weather = BranchPythonOperator(
        task_id='branch_process_weather',
        python_callable=branch_process_weather_func,
        provide_context=True
    )

    process_current_weather_group = process_weather_taskgroup(dag,
                                                              PROCESS_CURRENT_WEATHER_GROUP_ID,
                                                              "data/3.0/onecall",
                                                              {"appId": Variable.get("WEATHER_API_KEY"), "lat": 49.842957, "lon": 24.031111},
                                                              lambda x: json.loads(x.text)["current"])
    
    process_history_weather_group = process_weather_taskgroup(dag,
                                                              PROCESS_HISTORY_WEATHER_GROUP_ID,
                                                              "data/3.0/onecall/timemachine", 
                                                              {"appId": Variable.get("WEATHER_API_KEY"), "lat": 49.842957, "lon": 24.031111, "dt": "{{ ti.xcom_pull(task_ids='branch_process_weather', key='execution_date_timestamp') }}"},
                                                              lambda x: json.loads(x.text)["data"][0])
    inject_data = PostgresOperator(
        task_id = "inject_data",
        postgres_conn_id = "measurements_db",
        sql = r"""
            INSERT INTO measurements (execution_time, temperature, humidity, cloudiness, windSpeed) VALUES
            (to_timestamp({{ti.xcom_pull(task_ids=['process_current_weather.process_data', 'process_history_weather.process_data'])[0][0]}}),
            {{ti.xcom_pull(task_ids=['process_current_weather.process_data', 'process_history_weather.process_data'])[0][1]}},
            {{ti.xcom_pull(task_ids=['process_current_weather.process_data', 'process_history_weather.process_data'])[0][2]}},
            {{ti.xcom_pull(task_ids=['process_current_weather.process_data', 'process_history_weather.process_data'])[0][3]}},
            {{ti.xcom_pull(task_ids=['process_current_weather.process_data', 'process_history_weather.process_data'])[0][4]}});
        """,
        trigger_rule = 'none_failed'
    )

    create_table_postgres_task >> run_migrations_postgres_task >> branch_process_weather >> [process_current_weather_group, process_history_weather_group] >> inject_data