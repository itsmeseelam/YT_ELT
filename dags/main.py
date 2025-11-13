from airflow import DAG
import pendulum
from datetime import datetime, timedelta
# from airflow.decorators import task
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

from datawarehouse.dwh import staging_table, core_table

local_tz = pendulum.timezone("America/New_York")

default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule="*/5 * * * *",
    catchup=False,
) as dag:

    playlist_task = get_playlist_id()
    video_ids_task = get_video_ids(playlist_task)
    video_data_task = extract_video_data(video_ids_task)
    save_task = save_to_json(video_data_task)

    # Set dependencies
    playlist_task >> video_ids_task >> video_data_task >> save_task


with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process json file and insert into the staging and core tables",
    schedule="*/2 * * * *",
    catchup=False,
) as dag:

    # Define tasks  
    update_staging = staging_table()
    update_core = core_table()

    # Define dependencies
    update_staging >> update_core
