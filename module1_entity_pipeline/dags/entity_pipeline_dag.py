from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

default_args = {
    'owner': 'lekshmikanth',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 1)
}

def run_ingestion():
    result = subprocess.run(
        [sys.executable, '/opt/airflow/src/ingest_entities.py'],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Ingestion failed: {result.stderr}")

def run_clustering():
    result = subprocess.run(
        [sys.executable, '/opt/airflow/src/cluster_entities.py'],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Clustering failed: {result.stderr}")

def run_alerts():
    result = subprocess.run(
        [sys.executable, '/opt/airflow/src/generate_alerts.py'],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Alert generation failed: {result.stderr}")

def run_report():
    result = subprocess.run(
        [sys.executable, '/opt/airflow/src/generate_report.py'],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Report generation failed: {result.stderr}")

with DAG(
    dag_id='entity_content_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Daily entity data ingestion and pattern analysis pipeline'
) as dag:

    ingest = PythonOperator(task_id='ingest_entity_data', python_callable=run_ingestion)
    cluster = PythonOperator(task_id='run_clustering', python_callable=run_clustering)
    alerts = PythonOperator(task_id='generate_alerts', python_callable=run_alerts)
    report = PythonOperator(task_id='generate_report', python_callable=run_report)

    ingest >> cluster >> alerts >> report
