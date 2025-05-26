from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl import extract_csv, extract_api, extract_sql, transform_and_merge, upload_to_s3

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 24),
    'retries': 5
}

with DAG(
    dag_id='supplier_data_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_extract_csv = PythonOperator(
        task_id='extract_csv_data',
        python_callable=extract_csv
    )

    task_extract_api = PythonOperator(
        task_id='extract_api_data',
        python_callable=extract_api
    )

    task_extract_sql = PythonOperator(
        task_id='extract_sql_data',
        python_callable=extract_sql
    )

    task_transform = PythonOperator(
        task_id='transform_and_merge',
        python_callable=transform_and_merge
    )

    task_upload_s3 = PythonOperator(
        task_id='upload_merged_to_s3',
        python_callable=upload_to_s3
    )

    [task_extract_csv, task_extract_api, task_extract_sql] >> task_transform >> task_upload_s3
