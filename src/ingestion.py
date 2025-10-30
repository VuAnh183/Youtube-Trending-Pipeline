import os
import requests
import json
from dotenv import load_dotenv
from pathlib import Path
from googleapiclient.discovery import build

load_dotenv()
API_KEY = os.getenv("API_KEY")
REGION = os.getenv("REGIONS")
RAW_DIR = Path(os.getenv("RAW_DATA_DIR")) 

file_path = RAW_DIR / "test.json"


def fetch_trending_videos(maxResults: int):
    url = (
        f"https://www.googleapis.com/youtube/v3/videos?"
        f"part=snippet,statistics&chart=mostPopular&regionCode={REGION}&"
        f"maxResults={maxResults}&key={API_KEY}"
    )

    response =requests.get(url)

    data = response.json()
    
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
if __name__ == "__main__":
    fetch_trending_videos(10)