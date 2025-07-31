from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingestion import fetch_market_data
from quality_checks import data_quality

default_args = {
    'owner': 'leena',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'market_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

ingest_task = PythonOperator(
    task_id='fetch_market_data',
    python_callable=fetch_market_data.insert_data_to_db,
    dag=dag,
)

quality_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality.main,
    dag=dag,
)

ingest_task >> quality_task
