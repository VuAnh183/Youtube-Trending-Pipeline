import string
import requests
import json
from googleapiclient.discovery import build
from config import API_KEY, REGIONS

def fetch_trending_videos(region_code: string, maxResults: int):
    url = (
        f"https://www.googleapis.com/youtube/v3/videos?"
        f"part=snippet,statistics&chart=mostPopular&regionCode={region_code}&"
        f"maxResults={maxResults}&key={API_KEY}"
    )

    response =requests.get(url)

    data = response.json()

    print(json.dumps(data,indent = 2))

if __name__ == "__main__":
    fetch_trending_videos("FR", 3)