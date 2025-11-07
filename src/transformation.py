import pandas as pd
import json
import os
from dotenv import load_dotenv
from pathlib import Path
import logging
import json

load_dotenv(override=False)
RAW_DIR = Path(os.getenv("RAW_DATA_DIR")) 
PROCESSED_DIR = Path(os.getenv("PROCESSED_DATA_DIR"))

file_path_load = RAW_DIR / "test.json"
file_path_save = PROCESSED_DIR / "test.parquet"

def transform_raw_data(data: dict) -> pd.DataFrame:

    # try:
    #     with open(file_path_load, 'r') as file:
    #         data = json.load(file)
    # except Exception as e:
    #     logging.error(f"Load raw data failed: {file_path_load}")
    #     raise RuntimeError(f"Transformation failed: {e}") from e

    filtered_df = pd.json_normalize(data['items'])
    for col in filtered_df.columns:
        dtype = filtered_df[col].dtype
        
        if pd.api.types.is_numeric_dtype(dtype):
            filtered_df[col] = filtered_df[col].fillna(0)
        elif pd.api.types.is_string_dtype(dtype):
            filtered_df[col] = filtered_df[col].fillna(pd.NA)
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            filtered_df[col] = filtered_df[col].fillna(pd.NaT)
        elif pd.api.types.is_bool_dtype(dtype):
            filtered_df[col] = filtered_df[col].fillna(False)
        else:
            # For any other object type, just fill with None
            filtered_df[col] = filtered_df[col].fillna(None)


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

    clean_df['views'] = clean_df['views'].astype("Int64")
    clean_df['likes'] = clean_df['likes'].astype("Int64")
    clean_df['comments'] = clean_df['comments'].astype("Int64")
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
#     data = ingestion.fetch_trending_videos(50)
#     df = transform_raw_data(data)
#     print(df)

#     # save_as_parquet(data)
    


