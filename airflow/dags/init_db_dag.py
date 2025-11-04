from airflow.sdk import dag, task
from src import init_db

@dag
def database():
    
    @task()
    def init_postgres():
        init_db.init_postgres_table()

    init_postgres()

database()