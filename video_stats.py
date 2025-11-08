import requests
import json
# import os
# from dotenv import load_dotenv
import datetime
from datetime import date
from airflow.decorators import task
from airflow.models import Variable

# load_dotenv(dotenv_path='.env')
API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
maxResults = 10

@task
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



# 'UUX6OQ3DkcsbYNE6H8uQQuVA'
@task
def get_videolistid(playlistid):
    video_ids = []
    pageToken = None
    base_url = f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistid}&key={API_KEY}'
    try:
        # url = base_url
        # for url in [base_url]:
        #     if pageToken:
        #         url += f'&pageToken={pageToken}'
        #     response = requests.get(url)
        #     response.raise_for_status()
        #     data = response.json()
        #     for item in data['items']:
        #         video_ids.append(item['contentDetails']['videoId'])
        #     pageToken = data.get('nextPageToken')
        #     if not pageToken:
        #         break
        while True:
            url = base_url
            if pageToken:
                url += f'&pageToken={pageToken}'
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data['items']:
                video_ids.append(item['contentDetails']['videoId'])
            pageToken = data.get('nextPageToken')
            if not pageToken:
                break
        return video_ids
    except requests.exceptions.RequestException as e:
        print(f"Error fetching playlist items: {e}")

@task
def extract_video_data(video_ids):
    extracted_data = []

    def batch_list(video_id_lst, batch_size):
        if not isinstance(video_id_lst, (list, tuple)):
            print("❌ Expected list of video IDs, got:", type(video_id_lst))
            return []

        for video_id in range(0, len(video_id_lst), batch_size):
            yield video_id_lst[video_id : video_id + batch_size]
            # print(f'video_id: {video_id} to {batch_size}')
            # print("Processing batch:", video_id_lst[video_id : video_id + batch_size])

    try:
        for batch in batch_list(video_ids, maxResults):
            video_ids_str = ",".join(batch)
            # print("Fetching data for video IDs:", video_ids_str)
            url = (
                f"https://youtube.googleapis.com/youtube/v3/videos?"
                f"part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"
            )

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):
                video_id = item["id"]
                snippet = item["snippet"]
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]

                video_data = {
                    "video_id": video_id,
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get("viewCount"),
                    "likeCount": statistics.get("likeCount"),
                    "commentCount": statistics.get("commentCount"),
                }
                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e
@task  
def save_to_json(extracted_data):
    file_path = f"./Data/YT_data_{date.today()}.json"

    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    playlistid = get_playlist_id(
        "https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle="
    )
    video_id_lst = get_videolistid(playlistid)
    print("✅ video_ids count:", len(video_id_lst))
    video_data = extract_video_data(video_id_lst)
    save_to_json(video_data)
