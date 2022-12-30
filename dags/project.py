import re
import os
import json
import time
import random
import pickle
import airflow
import numpy as np
import pandas as pd
import gender_guesser.detector as gender

from semanticscholar import SemanticScholar
from datetime import timedelta
from urllib.request import urlopen

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

GENDER_MAPPING = {
    'male': "M",
    'female': "F",
}

MOCK_API = False
MOCK_FOLDER = "API_mock_data"
serpapi_key = "76a33e61246c7dc521e7c31b6abb087ef2f0a206e51476258b07c36ea5f900dd"

genderDetector = gender.Detector()

def get_JSON(URL):
    response = urlopen(URL)
    decoded = response.read().decode("utf-8")
    return json.loads(decoded)

def normalize_title(title):
    return " ".join(title.strip().title().split()).replace(".", "")
    
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

def clear_title(title):
    if title is not None:
        title = title.replace("\n", "")
        title = title.replace("'", "")
    return title if title else 'NULL'

def parse_first_name(normalized_name):
    return " ".join(normalized_name.split()[:-1])

def parse_last_name(normalized_name):
    return normalized_name.split()[-1]

def is_full_name(normalized_name):
    return (len(normalized_name) > 1) and not re.search(r"\.", normalized_name)

def query_gender_API(first_name):
    local_mock = True
    if local_mock:
        return {
            "name": first_name, 
            "gender": random.choice(["male", "female"]), 
            "samples":  random.randint(10, 10000), 
            "accuracy": random.randint(1, 100), 
            "duration": str(random.randint(10, 50)) + "ms"
        }
    else:
        key = "WX923RkrwWYQE4UGSt4GHEFk7EZYgwUJ5adt"
        url = "https://gender-api.com/get?key=" + key + "&name=" + first_name
        return get_JSON(url)

sch = SemanticScholar()

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

def read_data(data_folder):
    with open(f'{data_folder}/dataframe.json', 'r') as f:
        lines = f.readlines()
    
    N = 100
    papers = pd.DataFrame([json.loads(x) for x in lines])
    papers = papers.head(N)
    ##papers = papers.drop('abstract', axis=1)
    return papers

def prepare_data(data_folder):
    papers = read_data(data_folder)
    authors = {}
    insert_authors = 'INSERT INTO authors (id, first_name, last_name, gender)\n VALUES \n'
    insert_papers = 'INSERT INTO papers (id, title, scientific_domain_id, doi, comments, report_no, license)\n VALUES \n'
    insert_authors_to_papers = 'INSERT INTO author_to_paper (author_id, paper_id)\n VALUES \n'
    insert_scientific_domain = 'INSERT INTO scientific_domain (id, code)\n VALUES \n'
    index = 1
    for i, row in papers.iterrows():
        for author in row["authors_parsed"]:
            first_name = author[1].replace(",", "")
            last_name = author[0].replace(",", "")

            if first_name.strip() != "":
                names = normalize_name(first_name + " " + last_name)
                first_name = names[0]
                last_name = names[1]
                name = names[2]
                genderResult = genderDetector.get_gender(first_name)
                gender = 'X'
                if genderResult == 'female' or genderResult == 'male':
                    gender = GENDER_MAPPING[genderResult]
                ##name, gender = full_name_and_gender(name)
                
                if not (name in authors):
                    authors[name] = {
                        "google_scholar_id": None,
                        "gender": gender,
                        "university": None,
                        "role": None
                    }
                    insert_authors += f'{index, first_name, last_name, gender},\n'
                    insert_authors_to_papers += f'{index, i},\n'
                    index += 1

        title = clear_title(row.title)
        doi = row.doi if row.doi else 'NULL'
        comments = clear_title(row.comments)
        report_no = row["report-no"] if row["report-no"] else 'NULL'
        license = row.license if row.license else "NULL"
        insert_papers += f'{i, title, i, doi, comments, report_no, license},\n'
        scientific_domain = row.categories
        insert_scientific_domain += f'{i, scientific_domain,},\n'
    
    insert_authors = insert_authors[:-2]
    insert_authors += '\n ON CONFLICT DO NOTHING;\n'
    with open(f'{data_folder}/insert_authors.sql', 'w') as f:
        f.write(insert_authors)

    insert_papers = insert_papers[:-2]
    insert_papers += '\n ON CONFLICT DO NOTHING;\n'
    with open(f'{data_folder}/insert_papers.sql', 'w') as f:
        f.write(insert_papers)
    
    insert_authors_to_papers = insert_authors_to_papers[:-2]
    insert_authors_to_papers += '\n ON CONFLICT DO NOTHING;\n'
    with open(f'{data_folder}/insert_authors_to_papers.sql', 'w') as f:
        f.write(insert_authors_to_papers)

    insert_scientific_domain = insert_scientific_domain[:-2]
    insert_scientific_domain += '\n ON CONFLICT DO NOTHING;\n'
    with open(f'{data_folder}/insert_scientific_domain.sql', 'w') as f:
        f.write(insert_scientific_domain)

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
    task_id='prepare_data',
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
    sql='insert_authors.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

fourth_task = PostgresOperator(
    task_id='insert_scientific_domain',
    dag=project,
    postgres_conn_id='airflow_pg',
    sql='insert_scientific_domain.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

fifth_task = PostgresOperator(
    task_id='insert_papers',
    dag=project,
    postgres_conn_id='airflow_pg',
    sql='insert_papers.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

sixth_task = PostgresOperator(
    task_id='insert_authors_to_papers',
    dag=project,
    postgres_conn_id='airflow_pg',
    sql='insert_authors_to_papers.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

first_task >> second_task >> third_task >> fourth_task >> fifth_task >> sixth_task