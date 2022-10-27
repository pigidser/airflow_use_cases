'''
DAG for ingesting yellow taxi trip data to GCP.
Data interval: 2019, 2020 years.
'''
import os
# import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from pyarrow.csv import read_csv, ParseOptions, ConvertOptions, ReadOptions
from pyarrow.parquet import write_table


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Get interval start for the current DAG and get the previous period
EXECUTION_YEAR_MONTH_TEMPLATE = "{{ data_interval_start.subtract(months=1).strftime(\'%Y-%m\') }}"
EXECUTION_YEAR_TEMPLATE = "{{ data_interval_start.subtract(months=1).strftime(\'%Y\') }}"

DATASET_FILE = f"yellow_tripdata_{EXECUTION_YEAR_MONTH_TEMPLATE}.parquet"
URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{DATASET_FILE}"

PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

LOCAL_FILE = f"{PATH_TO_LOCAL_HOME}/{DATASET_FILE}"
GCP_OBJECT_NAME = f"raw/yellow_tripdata/{EXECUTION_YEAR_TEMPLATE}/{DATASET_FILE}"


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)

    blob.upload_from_filename(local_file)


# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="yellow_taxi_data",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 2, 1),
    end_date=datetime(2019, 4, 5),
    # end_date=datetime(2021, 1, 5),
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        },
    catchup=True,
    max_active_runs=3,
    tags=['de-zoomcamp'],
) as yellow_taxi_data:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        # Run wget in quite mode and save file
        bash_command=f"wget -q {URL} -O {LOCAL_FILE}"
    )

    # test_task = BashOperator(
    #     task_id="test_task",
    #     bash_command=f"ls -l"
    # )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": GCP_OBJECT_NAME,
            "local_file": LOCAL_FILE,
        },
    )


    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id="bigquery_external_table_task",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table",
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
    #         },
    #     },
    # )

    rm_task = BashOperator(
        task_id="rm_task",
        bash_command=f"rm {LOCAL_FILE}"
    )

    download_dataset_task >> local_to_gcs_task >> rm_task
    # download_dataset_task >> local_to_gcs_task >> bigquery_external_table_task