import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from ds_convert_to_parquet_operator import ConvertToParquetOperator

URLS_IMDB = {
   'title_crew': 'title.crew.csv',
   'title_episode': 'title.episode.csv',
   'title_principals': 'title.principals.csv',
   'title_ratings': 'title.ratings.csv'
}

ds_convert_to_parquet_dag =  DAG(dag_id=f"ds_convert_to_parquet_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)

# TASKS

# title_crew
ds_convert_to_parquet_title_crew = ConvertToParquetOperator(
   task_id=f"download_{URLS_IMDB['title_crew']}", url=URLS_IMDB['title_crew'],dag=ds_convert_to_parquet_dag
)

# title_episode
ds_convert_to_parquet_title_episode = ConvertToParquetOperator(
   task_id=f"download_{URLS_IMDB['title_episode']}", url=URLS_IMDB['title_episode'],dag=ds_convert_to_parquet_dag
)

# title_principals
ds_convert_to_parquet_title_principals = ConvertToParquetOperator(
   task_id=f"download_{URLS_IMDB['title_principals']}", url=URLS_IMDB['title_principals'],dag=ds_convert_to_parquet_dag
)

# title_ratings
ds_convert_to_parquet_title_ratings = ConvertToParquetOperator(
   task_id=f"download_{URLS_IMDB['title_ratings']}", url=URLS_IMDB['title_ratings'],dag=ds_convert_to_parquet_dag
)

ds_convert_to_parquet_title_crew
ds_convert_to_parquet_title_episode
ds_convert_to_parquet_title_principals
ds_convert_to_parquet_title_ratings
