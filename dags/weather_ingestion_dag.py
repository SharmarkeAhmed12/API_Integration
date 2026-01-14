"""
Weather ingestion DAG (BashOperator)

This DAG runs the `Api_Ingest.py --once` CLI inside the Airflow container.
Using `BashOperator` keeps the DAG import lightweight and avoids Python
import path issues.
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# DAG configuration
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "weather_ingestion_daily",
    default_args=default_args,
    description="Ingest weather data from OpenWeather API → Kafka",
    schedule_interval="0 0 * * *",  # daily at midnight; change as needed
    catchup=False,
    tags=["weather", "ingestion"],
)

# Run the script inside the container; Script lives at /home/airflow/app/Api_Ingest.py
ingest_task = BashOperator(
    task_id="ingest_weather",
    bash_command="python /home/airflow/app/Api_Ingest.py --once",
    dag=dag,
)

