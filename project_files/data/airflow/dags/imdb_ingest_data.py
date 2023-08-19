import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from imdb_download_from_source_operator import ImdbDownloadFromSourceOperator

URLS_IMDB = {
   'title_crew': 'https://datasets.imdbws.com/title.crew.tsv.gz',
   'title_episode': 'https://datasets.imdbws.com/title.episode.tsv.gz',
   'title_principals': 'https://datasets.imdbws.com/title.principals.tsv.gz',
   'title_ratings': 'https://datasets.imdbws.com/title.ratings.tsv.gz'
}

# DAG
imdb_ingest_data_dag =  DAG(dag_id="imdb_ingest_data_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)

# TASKS

# title_crew
download_title_crew = ImdbDownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_IMDB['title_crew'])}", url=URLS_IMDB['title_crew'],dag=imdb_ingest_data_dag
)

# title_episode
download_title_episode = ImdbDownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_IMDB['title_episode'])}", url=URLS_IMDB['title_episode'],dag=imdb_ingest_data_dag
)

# title_principals
download_title_principals = ImdbDownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_IMDB['title_principals'])}", url=URLS_IMDB['title_principals'],dag=imdb_ingest_data_dag
)

# title_ratings
download_title_ratings = ImdbDownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_IMDB['title_ratings'])}", url=URLS_IMDB['title_ratings'],dag=imdb_ingest_data_dag
)

download_title_crew 
download_title_episode
download_title_principals
download_title_ratings
