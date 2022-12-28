from datetime import timedelta
import airflow
import json
import pandas as pd
import numpy as np
import re


from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.contrib.sensors.file_sensor import FileSensor

DEFAULT_ARGS = {
    'owner': 'GROUP7',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
DATA_FOLDER = '/tmp/data'

DUMMY_CONSTANTS = {
    'gender': 'M',
    'citations_count': 1,
    'h_index': 1
}

def prepare_year(data_folder):
    insert_statement = 'INSERT INTO year_of_publication (year)\n VALUES \n'
    year = 2023
    for i in range(200):
        year = year - 1
        if i < 199:
            insert_statement += f'({year}),\n'
        else:
            insert_statement += f'({year}) \nON CONFLICT DO NOTHING; \n'
    with open(f'{data_folder}/insert_year.sql', 'w') as f:
        f.write(insert_statement)

def normalize_name(name):
    name = name.replace(".", ". ")
    name = name.replace("-", " -")
    name = name.replace("'", "")
    name_components = re.split(r"\s+", name.strip())

    def mapper(name):
        if re.fullmatch(r"-*\w", name):
            return (name + ".").title()
        return name.title()
        
    name = " ".join(list(map(mapper, name_components))).replace(" -", "-")
    parts = name.split()
    first_name = " ".join(parts[:-1])
    last_name = parts[len(parts)-1]
    return [first_name, last_name, name]

def read_data(data_folder):
    with open(f'{data_folder}/dataframe.json', 'r') as f:
        lines = f.readlines()
    
    N = 100
    papers = pd.DataFrame([json.loads(x) for x in lines])
    papers = papers.head(N)
    ##papers = papers.drop('abstract', axis=1)
    return papers

def prepare_author(data_folder):
    papers = read_data(data_folder)
    authors = {}
    insert_statement = 'INSERT INTO authors (id, first_name, last_name, gender, citations_count, h_index)\n VALUES \n'
    size = len(papers.index)
    for index, row in papers.iterrows():
        for author in row["authors_parsed"]:
            first_name = author[1].replace(",", "")
            last_name = author[0].replace(",", "")

        if first_name.strip() != "":
            names = normalize_name(first_name + " " + last_name)
            first_name = names[0]
            last_name = names[1]
            name = names[2]
            gender = DUMMY_CONSTANTS['gender']
            citations_count = DUMMY_CONSTANTS['citations_count']
            h_index = DUMMY_CONSTANTS['h_index']
            ##name, gender = full_name_and_gender(name)
            
            if not (name in authors):
                authors[name] = {
                    "google_scholar_id": None,
                    "gender": gender,
                    "university": None,
                    "role": None
                }
                insert_statement += f'{index, first_name, last_name, gender, citations_count, h_index}'
                if index + 1 == size:
                    insert_statement += '\n ON CONFLICT DO NOTHING;\n'
                else:
                    insert_statement += ',\n'
    with open(f'{data_folder}/insert.sql', 'w') as f:
        f.write(insert_statement)

def prepare_data(data_folder):
    prepare_author(data_folder)
    prepare_year(data_folder)


project = DAG(
    dag_id='project',  # name of dag
    schedule_interval='* * * * *',  # execute every minute
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,  # in case execution has been paused, should it execute everything in between
    # the PostgresOperator will look for files in this folder
    template_searchpath=DATA_FOLDER,
    default_args=DEFAULT_ARGS,  # args assigned to all operators
)

first_task = PostgresOperator(
    task_id='create_tables',
    dag=project,
    postgres_conn_id='airflow_pg',
    sql='schema.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

second_task = PythonOperator(
    task_id='prepare_author_insert',
    dag=project,
    trigger_rule='none_failed',
    python_callable=prepare_data,
    op_kwargs={
        'data_folder': DATA_FOLDER,
    },
)

third_task = PostgresOperator(
    task_id='insert_authors',
    dag=project,
    postgres_conn_id='airflow_pg',
    sql='insert.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

fourth_task = PostgresOperator(
    task_id='insert_years',
    dag=project,
    postgres_conn_id='airflow_pg',
    sql='insert_year.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

first_task >> second_task >> third_task >> fourth_task