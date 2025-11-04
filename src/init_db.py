from sqlalchemy import create_engine, text
import logging
from dotenv import load_dotenv
import os


load_dotenv(override=False)
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")

def init_postgres_table():
    # connection string: postgresql+psycopg2://user:password@host:port/dbname
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE}(
        video_id TEXT PRIMARY KEY,
        publish_at TIMESTAMP,
        channel_id TEXT,
        title TEXT, 
        channel_title TEXT,
        category INT,
        views BIGINT,
        likes BIGINT,
        comments BIGINT
    )
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_query))
        logging.info(f"Table {POSTGRES_TABLE} is ready!")