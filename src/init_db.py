from sqlalchemy import create_engine, text
import logging
from dotenv import load_dotenv
import os


load_dotenv(override=False)
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")

def init_postgres_table():
    # connection string: postgresql+psycopg2://user:password@host:port/dbname
    engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB_NAME}")

    create_table_query = """
    CREATE TABLE IF NOT EXISTS trending_videos(
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
        logging.info("Table 'trending_videos' is ready!")