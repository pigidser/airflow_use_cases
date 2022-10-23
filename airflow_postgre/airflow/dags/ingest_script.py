"""
Download parquet format data and upload it to PostgreSQL database

Usage 
python ingest_data_my.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table=yellow_taxi_data2 \
    --url=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet

"""

import os
from sqlalchemy import create_engine
from pyarrow.dataset import dataset
import pandas as pd

from time import time

def ingest_callable(user, password, host, port, db, table_name, data_file):

    print(f"Ingest data to {table_name} table")

    # Create an engine to connect to postgresql
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()

    print(f"Connected!")

    # Drop table
    query = f"""DROP TABLE IF EXISTS "{table_name}";"""
    engine.execute(query)

    def transform(df):
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


    # Read the file by chunks
    ds = dataset(data_file, format="parquet")

    print("Start ingesting of the data...")

    batches = ds.to_batches()

    for batch in batches:

        t_start = time()

        df = batch.to_pandas()
        transform(df)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        print(f"Inserted {df.shape[0]} rows, took {(t_end - t_start):.2f}")
            
    print("Ingesting finished successfuly")