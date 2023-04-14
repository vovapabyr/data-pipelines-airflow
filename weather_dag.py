from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from include.process_cities_weather_taskgroup import process_cities_weather_taskgroup

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
            ADD COLUMN IF NOT EXISTS windSpeed FLOAT,
            ADD COLUMN IF NOT EXISTS city VARCHAR(50);
        """
    )

    create_table_postgres_task >> run_migrations_postgres_task >> process_cities_weather_taskgroup(dag, Variable.get("CITIES", deserialize_json=True))