from airflow.sdk import dag, task
from src import ingestion, load, transformation
from datetime import datetime, timedelta

@dag
def etl():

    @task(
            retries=3,                              
            retry_delay=timedelta(seconds=10),       
            retry_exponential_backoff=True,        
            max_retry_delay=timedelta(seconds=30)
    )
    def extract():
        data = ingestion.fetch_trending_videos(10)
        ingestion.save_to_json(data)
    

    @task(
            retries=3,                              
            retry_delay=timedelta(seconds=10),       
            retry_exponential_backoff=True,        
            max_retry_delay=timedelta(seconds=30)
            # Wil have to change these in the future depending on the data size
    )
    def transform():
        data = transformation.transform_raw_data()
        transformation.save_as_parquet(data)

    @task(
            retries=3,                              
            retry_delay=timedelta(seconds=10),       
            retry_exponential_backoff=True,        
            max_retry_delay=timedelta(seconds=30)
    )
    def load_in_SQLite():
        data = load.get_parquet_data()

        load.save_to_SQLite(data)

    @task(
            retries=3,                              
            retry_delay=timedelta(seconds=5),       
            retry_exponential_backoff=True,        
            max_retry_delay=timedelta(seconds=15)
    )
    def data_check():
        load.SQLite_check()


    extract() >> transform() >> load_in_SQLite() >> data_check()


etl()