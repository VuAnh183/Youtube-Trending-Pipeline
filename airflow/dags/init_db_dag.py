from airflow.sdk import dag, task
from src.init_db import init_postgres_table

@dag
def database():
    
    @task()
    def init_postgres():
        init_postgres_table()

    init_postgres()

database()