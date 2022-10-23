from lib2to3.pytree import convert
from operator import index
import os
from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")

EXECUTION_MONTH_TEMPLATE = "{{ execution_date.strftime(\'%Y-%m\') }}"

URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
URL = os.path.join(URL_PREFIX, f'yellow_tripdata_{EXECUTION_MONTH_TEMPLATE}.parquet')
OUTPUT_FILE = os.path.join(AIRFLOW_HOME, f'output_{EXECUTION_MONTH_TEMPLATE}.parquet')
TABLE_NAME = f'yellow_taxi_{EXECUTION_MONTH_TEMPLATE}'

# def convert_parquet_to_csv(input_filename, output_filename):
#     """
#     Reads parquet and save to csv with pandas (from here https://stackoverflow.com/a/48714539)
#     """
#     # import pandas as pd
#     import pyarrow.parquet as pq

#     data = pq.read_table(input_filename).to_pandas()[:10000]
#     data.to_csv(output_filename, index=False)


local_workflow = DAG(
    "LocalIngestionDAG",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2022, 5, 1)
)

with local_workflow:

    download_task = BashOperator(
        task_id="download",
        bash_command=f'echo {URL} && curl -sS {URL} > {OUTPUT_FILE}'
    )

    # convert_task = PythonOperator(
    #     task_id="convert",
    #     python_callable=convert_parquet_to_csv,
    #     op_kwargs=dict(
    #         input_filename=OUTPUT_FILE,
    #         output_filename=f'{os.path.splitext(OUTPUT_FILE)[0]}.csv'
    #     )
    # )

    test_task = BashOperator(
        task_id="test",
        bash_command=f'ls -l {AIRFLOW_HOME}'
    )

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME,
            data_file=OUTPUT_FILE
        )
    )

    download_task >> test_task >> ingest_task
    # download_task >> convert_task >> test_task >> ingest_task
