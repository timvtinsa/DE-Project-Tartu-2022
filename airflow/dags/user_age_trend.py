from datetime import timedelta
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
DATA_FOLDER = '/tmp/data'


def get_average_age():
    hook = PostgresHook(postgres_conn_id="airflow_pg")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select age from public.users")
    data = cursor.fetchall()
    current_total = 0
    prev_total = 0
    for age in data:
        current_total += age[0]
    current_avg = current_total/len(data)
    without_last_5 = data[:-5]
    for age in without_last_5:
        prev_total += age[0]
    prev_avg = prev_total/len(without_last_5)

    if current_avg > prev_avg:
        return 'increase'
    return 'decrease'


user_age_trend = DAG(
    dag_id='user_age_trend',  # name of dag
    schedule_interval='* * * * *',  # execute every minute
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,  # in case execution has been paused, should it execute everything in between
    # the PostgresOperator will look for files in this folder
    template_searchpath=DATA_FOLDER,
    default_args=DEFAULT_ARGS,  # args assigned to all operators
)

sensor = FileSensor(
    task_id="sensor_file",
    dag=user_age_trend,
    fs_conn_id="fs_connection",
    filepath='/tmp/data/insert.sql',
)

third_task = PostgresOperator(
    task_id='insert_to_db',
    dag=user_age_trend,
    postgres_conn_id='airflow_pg',
    sql='insert.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

branch = BranchPythonOperator(
    task_id='check_age',
    dag=user_age_trend,
    trigger_rule='none_failed',
    python_callable=get_average_age
)

decrease = BashOperator(
    task_id='decrease',
    dag=user_age_trend,
    trigger_rule='none_failed',
    bash_command="echo 'Age decreased'"
)

increase = BashOperator(
    task_id='increase',
    dag=user_age_trend,
    trigger_rule='none_failed',
    bash_command="echo 'Age increased'"
)


sensor >> third_task >> branch
branch >> increase
branch >> decrease
