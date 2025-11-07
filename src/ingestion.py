import os
import requests
import json
from dotenv import load_dotenv
from pathlib import Path
import logging

load_dotenv(override=False)
API_KEY = os.getenv("API_KEY")
REGION = os.getenv("REGIONS")
RAW_DIR = Path(os.getenv("RAW_DATA_DIR")) 

file_path = RAW_DIR / "test.json"


def fetch_trending_videos(maxResults: int, maxRetries: int = 5):
    print("Fetching...")

    for attempt in range(1, maxRetries + 1):
        try:
            url = (
                f"https://www.googleapis.com/youtube/v3/videos?"
                f"part=snippet,statistics&chart=mostPopular&regionCode={REGION}&"
                f"maxResults={maxResults}&key={API_KEY}"
            )

            response =requests.get(url)

            print(f"Successfully fetch top {maxResults} trending youtube videos from region {REGION}")
            return response.json()

        except requests.RequestException as e:
            print(f"Attempt {attempt} failed: {e}")

    
def save_to_json(data: dict) -> Path:
    print("Saving data to json...")

    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)

        if file_path.exists():
            print(f"File existed! Overwriting existing file {file_path}")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        logging.info(f"Data extracted to {file_path}")
        return file_path

    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise RuntimeError(f"Extraction failed: {e}") from e


if __name__ == "__main__":
    data = fetch_trending_videos(50)
    # print(data)
    save_to_json(data)