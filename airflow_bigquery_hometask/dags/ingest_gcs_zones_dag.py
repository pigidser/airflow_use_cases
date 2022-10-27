'''
DAG for ingesting zones file to GCP.
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
EXECUTION_MONTH_TEMPLATE = "{{ data_interval_start.subtract(months=1).strftime(\'%Y-%m\') }}"

DATASET_FILE = f"taxi+_zone_lookup.csv"
URL = f"https://d37ci6vzurychx.cloudfront.net/misc/{DATASET_FILE}"

PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "retries": 1,
# }

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="ingest_gcs_zones_dag",
    schedule_interval="@once",
    start_date=days_ago(1),
    # end_date=datetime(2021, 1, 5),
    # default_args=default_args,
    # catchup=True,
    max_active_runs=3,
    tags=['de-zoomcamp'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        # Run wget in quite mode and save file
        bash_command=f"wget -q {URL} -O {PATH_TO_LOCAL_HOME}/{DATASET_FILE}"
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{DATASET_FILE}",
            "local_file": f"{PATH_TO_LOCAL_HOME}/{DATASET_FILE}",
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

    download_dataset_task >> local_to_gcs_task
    # download_dataset_task >> local_to_gcs_task >> bigquery_external_table_task