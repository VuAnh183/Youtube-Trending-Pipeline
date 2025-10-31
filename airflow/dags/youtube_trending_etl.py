from airflow.sdk import dag, task
from src import ingestion, load, transformation

@dag
def etl():

    @task
    def extract():
        ingestion.fetch_trending_videos(10)
    
    @task
    def transform():
        data = transformation.transform_raw_data()
        transformation.save_as_parquets(data)

    @task
    def load_in_SQLite():
        data = load.get_parquet_data()

        load.save_to_SQLite(data)

    @task
    def data_check():
        load.SQLite_check()


    extract() >> transform() >> load_in_SQLite() >> data_check()


etl()