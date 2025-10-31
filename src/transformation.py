import pandas as pd
import json
import os
from dotenv import load_dotenv
from pathlib import Path
import logging


load_dotenv()
RAW_DIR = Path(os.getenv("RAW_DATA_DIR")) 
PROCESSED_DIR = Path(os.getenv("PROCESSED_DATA_DIR"))

file_path_load = RAW_DIR / "test.json"
file_path_save = PROCESSED_DIR / "test.parquet"


def transform_raw_data() -> pd.DataFrame:

    try:
        with open(file_path_load, 'r') as file:
            data = json.load(file)
    except Exception as e:
        logging.error(f"Load raw data failed: {file_path_load}")
        raise RuntimeError(f"Transformation failed: {e}") from e

    filtered_df = pd.json_normalize(data['items'])

    columns_to_keep = [
    'id',
    'snippet.publishedAt',
    'snippet.channelId',
    'snippet.title',
    'snippet.channelTitle',
    'snippet.categoryId',
    'statistics.viewCount',
    'statistics.likeCount',
    'statistics.commentCount'
    ]
    
    clean_df = filtered_df[columns_to_keep].copy()

    clean_df.rename(columns={
    'id': 'video_id',
    'snippet.title': 'title',
    'snippet.publishedAt': 'published_at',
    'snippet.channelId': 'channel_id',
    'snippet.channelTitle': 'channel_title',
    'snippet.categoryId': 'category_id',
    'statistics.viewCount': 'views',
    'statistics.likeCount': 'likes',
    'statistics.commentCount': 'comments'
    }, inplace=True)

    clean_df['views'] = clean_df['views'].astype(int)
    clean_df['likes'] = clean_df['likes'].astype(int)
    clean_df['comments'] = clean_df['comments'].astype(int)
    clean_df['published_at'] = pd.to_datetime(clean_df['published_at'])

    return clean_df

def save_as_parquet(df: pd.DataFrame) -> Path:
    
    try:    
        file_path_save.parent.mkdir(parents=True, exist_ok=True)

        if file_path_save.exists():
            print(f"File existed! Overwriting existing file: {file_path_save}")
        df.to_parquet(file_path_save, engine="pyarrow")

        logging.info(f"Data extracted to {file_path_save}")
        return file_path_save
    
    except Exception as e:
        logging.error(f"Save as parquet failed {file_path_save}")
        raise RuntimeError(f"Transformation failed: {e}") from e


# if __name__ == '__main__':
#     data = transform_raw_data()

#     save_as_parquet(data)
    


