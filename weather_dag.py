import json
import logging
from airflow.models import Variable
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator 
from airflow.utils.dates import days_ago

def _process_weather(ti):
    info = ti.xcom_pull("extract_data")
    timestamp = info["dt"]
    temp = info["main"]["temp"]
    humidity = info["main"]["humidity"]
    cloudiness = info["clouds"]["all"]
    windSpeed = info["wind"]["speed"]
    logging.info(f"Time: {timestamp}. Temp: {temp}. Humidity: {humidity}. Cloudiness: {cloudiness}. WindSpeed: {windSpeed}.")
    return timestamp, temp, humidity, cloudiness, windSpeed

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

    extract_data = SimpleHttpOperator(
        task_id = "extract_data",
        http_conn_id = "weather_api_con",
        endpoint = "data/2.5/weather",
        data = {"appId": Variable.get("WEATHER_API_KEY"), "q": "Lviv"},
        method = "GET",
        response_filter = lambda x: json.loads(x.text),
        log_response = True,
        trigger_rule = 'none_failed'
    )

    process_data = PythonOperator(
        task_id = "process_data",
        python_callable = _process_weather
    )
    
    inject_data = PostgresOperator(
        task_id = "inject_data",
        postgres_conn_id = "measurements_db",
        sql = r"""
            INSERT INTO measurements (execution_time, temperature, humidity, cloudiness, windSpeed) VALUES
            (to_timestamp({{ti.xcom_pull(task_ids='process_data')[0]}}),
            {{ti.xcom_pull(task_ids='process_data')[1]}},
            {{ti.xcom_pull(task_ids='process_data')[2]}},
            {{ti.xcom_pull(task_ids='process_data')[3]}},
            {{ti.xcom_pull(task_ids='process_data')[4]}});
        """,
        trigger_rule = 'none_failed'
    )

    create_table_postgres_task >> run_migrations_postgres_task >> extract_data >> process_data >> inject_data