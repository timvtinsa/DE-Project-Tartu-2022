import requests
import csv
from datetime import datetime, timedelta
import airflow

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.contrib.sensors.file_sensor import FileSensor

DEFAULT_ARGS = {
    'owner': 'Jaak',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
API_URL = 'https://randomuser.me/api'
DATA_FOLDER = '/tmp/data'


def get_random_user(url, output_folder):
    users = []
    for i in range(5):
        r = requests.get(url)
        r_json = r.json()
        results = r_json['results']
        firstName = results[0]['name']['first']
        lastName = results[0]['name']['last']
        age = results[0]['dob']['age']
        users.append([firstName, lastName, age])
    with open(f'{output_folder}/users.csv', 'w') as csv_file:
        for i in range(5):
            csv_file.write(f'{users[i][0]}, {users[i][1]}, {users[i][2]} \n')


def prepare_insert(output_folder):
    insertStatements = []
    with open(f'{output_folder}/users.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            insertStatements.append(
                'INSERT INTO users (first_name, last_name, age)\n'
                f'VALUES {row[0], row[1].strip(), int(row[2])};\n'
            )

    with open(f'{output_folder}/insert.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS users (\n'
            'first_name VARCHAR(255),\n'
            'last_name VARCHAR(255),\n'
            'age INT);\n'
        )
        for statement in insertStatements:
            f.write(statement)


crm_generator = DAG(
    dag_id='crm_generator',  # name of dag
    schedule_interval='* * * * *',  # execute every minute
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,  # in case execution has been paused, should it execute everything in between
    # the PostgresOperator will look for files in this folder
    template_searchpath=DATA_FOLDER,
    default_args=DEFAULT_ARGS,  # args assigned to all operators
)

first_task = PythonOperator(
    task_id='get_random_user',
    dag=crm_generator,
    trigger_rule='none_failed',
    python_callable=get_random_user,
    op_kwargs={
        'output_folder': DATA_FOLDER,
        'url': API_URL,
    },
)


second_task = PythonOperator(
    task_id='prepare_insert_stmt',
    dag=crm_generator,
    trigger_rule='none_failed',
    python_callable=prepare_insert,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

first_task >> second_task
