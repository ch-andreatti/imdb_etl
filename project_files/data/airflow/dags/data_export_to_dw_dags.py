import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from bi_pg_operator import BIPgOperator

URLS_IMDB = {
   'title_crew': 'title.crew.csv',
   'title_episode': 'title.episode.csv',
   'title_principals': 'title.principals.csv',
   'title_ratings': 'title.ratings.csv'
}

data_export_to_dw =  DAG(dag_id=f"data_export_to_dw_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)

# TASKS

# title_crew
data_export_to_dw_title_crew = BIPgOperator(
   task_id=f"download_{URLS_IMDB['title_crew']}", url=URLS_IMDB['title_crew'], tablename='title_crew', dag=data_export_to_dw
)

# title_episode
data_export_to_dw_title_episode = BIPgOperator(
   task_id=f"download_{URLS_IMDB['title_episode']}", url=URLS_IMDB['title_episode'], tablename='title_episode', dag=data_export_to_dw
)

# title_principals
data_export_to_dw_title_principals = BIPgOperator(
   task_id=f"download_{URLS_IMDB['title_principals']}", url=URLS_IMDB['title_principals'], tablename='title_principals', dag=data_export_to_dw
)

# title_ratings
data_export_to_dw_title_ratings = BIPgOperator(
   task_id=f"download_{URLS_IMDB['title_ratings']}", url=URLS_IMDB['title_ratings'], tablename='title_ratings', dag=data_export_to_dw
)

data_export_to_dw_title_crew
data_export_to_dw_title_episode
data_export_to_dw_title_principals
data_export_to_dw_title_ratings
