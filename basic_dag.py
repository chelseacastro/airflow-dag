# import Library
import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# Bikin DAG
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 9, 7),
    'concurrency': 1,
    'retries': 0,
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retry_delay': timedelta(minutes=5),
}
with DAG('basic_dag', catchup=False, default_args=default_args, schedule_interval='*/5 * * * *') as dag:
    # Bikin Task di dalam DAG
    opr_1 = BashOperator(task_id='Operator_1', bash_command='echo "Operator 1"')
    opr_2 = BashOperator(task_id='Operator_2', bash_command='echo "Operator 2"')
    opr_3 = BashOperator(task_id='Operator_3', bash_command='echo "Operator 3"')
    opr_4 = BashOperator(task_id='Operator_4', bash_command='echo "Operator 4"')

# Bikin alur
opr_1 >> opr_2 >> opr_3 >> opr_4