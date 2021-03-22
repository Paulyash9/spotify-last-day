from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from etl_process import run_spotify_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0, 0, 0, 0, 0),
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='Get songs from Spotify, listened last 24 hours',
    schedule_interval='@daily'
)


etl_process = PythonOperator(
    task_id='from_spotify_into_db',
    python_callable=run_spotify_etl,
    dag=dag
)


etl_process
