from airflow.sdk import dag, task
from src import ingestion, load, transformation
from datetime import timedelta
import pandas as pd
import json
@dag
def etl():

    @task(
            retries=3,                              
            retry_delay=timedelta(seconds=10),       
            retry_exponential_backoff=True,        
            max_retry_delay=timedelta(seconds=30)
    )
    def extract():
        data = ingestion.fetch_trending_videos(50)
        # ingestion.save_to_json(data)
        return data
    

    @task(
            retries=3,                              
            retry_delay=timedelta(seconds=10),       
            retry_exponential_backoff=True,        
            max_retry_delay=timedelta(seconds=30)
            # Wil have to change these in the future depending on the data size
    )
    def transform(data: dict) -> pd.DataFrame:
        transformed_data = transformation.transform_raw_data(data)
        # transformation.save_as_parquet(data)
        return transformed_data
        



    @task(
            retries=3,                              
            retry_delay=timedelta(seconds=5),       
            retry_exponential_backoff=True,        
            max_retry_delay=timedelta(seconds=15)
    )
    def load_to_Postgres(data: pd.DataFrame):
        # data = load.get_parquet_data()

        load.save_to_Postgres(data)



    raw_data = extract() 
    clean_data = transform(raw_data) 
    load_to_Postgres(clean_data)


etl()