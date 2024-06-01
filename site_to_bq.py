from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery
import requests
import json
from datetime import datetime

# Replace with your BigQuery dataset and table name
BQ_DATASET = 'your_dataset'
BQ_TABLE = 'your_table'
PROJECT_ID = 'your_project_id'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 31),
    'retries': 1,
}

dag = DAG(
    'download_to_bigquery',
    default_args=default_args,
    description='Download data from a website and load it into BigQuery',
    schedule_interval='@daily',
)

def download_data():
    url = 'https://example.com/data'  # Replace with your data source URL
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()  # Assuming the data is in JSON format
        with open('/tmp/data.json', 'w') as f:
            json.dump(data, f)
    else:
        response.raise_for_status()

def upload_to_bigquery():
    client = bigquery.Client()
    dataset_ref = client.dataset(BQ_DATASET)
    table_ref = dataset_ref.table(BQ_TABLE)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
    )

    with open('/tmp/data.json', 'rb') as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            job_config=job_config,
        )

    job.result()  # Waits for the job to complete.

download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_bigquery',
    python_callable=upload_to_bigquery,
    dag=dag,
)

download_task >> upload_task
