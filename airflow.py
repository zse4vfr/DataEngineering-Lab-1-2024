import os
import datetime
import pandas as pd
from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.decorators import task

# Определение пути к данным
def get_input_dir():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'input')

def get_output_dir():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'output')

# Определение DAG
with DAG(
        dag_id="lr1_dag",
        schedule_interval=None,
        dagrun_timeout=datetime.timedelta(minutes=45),
        start_date=datetime.datetime(2023, 1, 1),
        catchup=False,
) as dag:

    @task(task_id='read')
    def read():
        input_dir = get_input_dir()
        df = pd.concat([pd.read_csv(os.path.join(input_dir, f'chunk{i}.csv')) for i in range(26)], axis=0)
        df.to_csv('tmp.csv', index=False)

    @task(task_id='drop')
    def drop():
        df = pd.read_csv('tmp.csv')
        df.dropna(subset=['designation', 'region_1'], inplace=True)
        df.to_csv('tmp.csv', index=False)

    @task(task_id='fill_with_zero')
    def fill_with_zero():
        df = pd.read_csv('tmp.csv')
        df.fillna({'price': 0.0}, inplace=True)
        df.to_csv('tmp.csv', index=False)

    @task(task_id='save_csv')
    def save_csv():
        output_dir = get_output_dir()
        df = pd.read_csv('tmp.csv')
        df.to_csv(os.path.join(output_dir, 'output.csv'), index=False)

    @task(task_id='save_elastic_search')
    def save_elastic_search():
        df = pd.read_csv('tmp.csv')
        elasticsearch = Elasticsearch("http://elasticsearch-kibana:9200")
        #elasticsearch = Elasticsearch("http://localhost:15601/app/home#")
        for _, row in df.iterrows():
            elasticsearch.index(index='lr1_dag_save', body=row.to_json())

    # Определение зависимостей задач
    task1 = read()
    task2 = drop()
    task3 = fill_with_zero()
    task4_1 = save_elastic_search()
    task4_2 = save_csv()

    task1 >> task2 >> task3 >> [task4_1, task4_2]