import datetime as dt
import json
from datetime import timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import mysql.connector as sql


def baca_data(**kwargs):
    ti = kwargs['ti']
    print('Memulai membaca data...')
    conn = sql.connect(host='34.101.66.65',
                       database='dezyre_db',
                       user='root',
                       password='root123')
    # mysql = MySqlHook(mysql_conn_id='mysql_default')
    # conn = mysql.get_conn()
    data = pd.read_sql('SELECT * FROM employees', conn).to_json()
    ti.xcom_push('data', data)


def lihat_data(**kwargs):
    ti = kwargs['ti']
    print('Menampilkan data...')
    data = ti.xcom_pull(task_ids='baca', key='data')
    print(pd.read_json(data))


default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 8, 1),
    'concurrency': 1,
    'retries': 1,
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
}
with DAG('simple_mysql', catchup=False, default_args=default_args, schedule_interval='@daily') as dag:
    baca = PythonOperator(task_id='baca', python_callable=baca_data)
    lihat = PythonOperator(task_id='lihat', python_callable=lihat_data)

baca >> lihat