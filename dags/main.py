from airflow.models import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import (
    get_playlist_id,
    get_video_ids,
    extract_video_data,
    save_to_json,
)

from datawarehouse.data_warehouse import staging_table, core_table


# define local timezone
local_tz = pendulum.timezone("Europe/Bucharest")


# default args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    #'retries' :1,
    #'retry-delay':timedelta(minutes =5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    #'end_date:datetime(2030,12,31, tzinfo=local_tz),
}


with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file to produce data",
    schedule="0 14 * * *",
    catchup=False,
) as dag:

    # define task
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    # define dependecies
    playlist_id >> video_ids >> extract_data >> save_to_json_task


with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging & core schemas",
    schedule="0 15 * * *",
    catchup=False,
) as dag:

    # define task
    update_staging = staging_table()
    update_core = core_table()

    # define dependecies
    update_staging >> update_core
