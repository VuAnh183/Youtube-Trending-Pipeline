import os
import pandas as pd 
from sqlalchemy import create_engine, text
from pathlib import Path
from dotenv import load_dotenv
import logging

load_dotenv()
DB_FILE = Path(os.getenv("DB_FILE"))
PROCESSED_DIR = Path(os.getenv("PROCESSED_DATA_DIR"))

processed_file_path = PROCESSED_DIR / "test.parquet"

def get_parquet_data():
    try:    
        processed_file_path.parent.mkdir(parents=True, exist_ok=True)

        if processed_file_path.exists():
            df = pd.read_parquet(processed_file_path)
            return df
        else:
            return f"File does not exist: {processed_file_path}"
        
    except Exception as e:
        logging.error(f"Load processed data failed: {processed_file_path}")
        raise RuntimeError(f"Loading failed: {e}") from e   

def save_to_SQLite(df: pd.DataFrame):
    try:
        engine = create_engine(f"sqlite:///{DB_FILE}")

        df.to_sql(
        name="youtube_trending_videos",     
        con=engine,         
        if_exists="replace", 
        index=False         
        )

    except Exception as e:
        logging.error(f"Save to parquet failed:  {e}")
        raise RuntimeError(f"Loading failed: {e}") from e 

def SQLite_check():
    try:
        engine = create_engine(f"sqlite:///{DB_FILE}")

        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM youtube_trending_videos"))
            count = result.scalar()
            print("Rows in table:", count)
    except Exception as e:
        raise RuntimeError(f"Health check failed: {e}") from e


# if __name__ == "__main__":
#     data = get_parquet_data()

#     save_to_SQLite(data)
#     SQLite_check()


