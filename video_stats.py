import requests
import json
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path='.env')
API_KEY = os.getenv('API_KEY')
CHANNEL_HANDLE = "mrBeast"

def get_playlist_id(url):
    try:
        # url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        url = f"{url}{CHANNEL_HANDLE}&key={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        # print(json.dumps(data, indent=4))
        channel_items = data['items'][0]
        channel_playlistid = channel_items['contentDetails']['relatedPlaylists']['uploads']
        # print(channel_playlistid)
        return channel_playlistid
    except requests.exceptions.RequestException as e:
        print(f"Error fetching playlist ID: {e}")
        return None
    
if __name__ == "__main__":
    print(get_playlist_id('https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle='))
