# import Library
import datetime as dt
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


def transform_csv():
    data = pd.read_csv(
        'https://gist.githubusercontent.com/sdcilsy/1f49732a3835dea3b288809d2ce68aaa/raw/990c12cad3d22e006aac1b79cc1683ebd1d7820a/nba.csv')
    data.dropna(inplace=True)
    print(data.head(10))


def transform_json():
    data_pns = pd.read_csv(
        'https://gist.githubusercontent.com/sdcilsy/8a7fd3edaf4a80b56f4fd869b7c1a2e5/raw/280d3ace43b734cf9aaec1a78d7e381d26743da0/revisi-datapegawaipnsguru-2019.csv')
    data_pns.dropna(inplace=True)
    print(data_pns.head(10))


# Bikin DAG
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 9, 10),
    'concurrency': 1,
    'retries': 0,
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retry_delay': timedelta(minutes=5),
}
with DAG('getdata_dag', catchup=False, default_args=default_args, schedule_interval='*/5 * * * *',
         tags=['sekolah_big_data']) as dag:
    # Bikin Task di dalam DAG
    opr_1 = BashOperator(task_id='start', bash_command='echo "Start"')
    opr_2 = PythonOperator(task_id='transform_csv', python_callable=transform_csv)
    opr_3 = PythonOperator(task_id='transform_json', python_callable=transform_json)
    opr_4 = BashOperator(task_id='end', bash_command='End')

# Bikin alur
opr_1 >> [opr_2, opr_3] >> opr_4
