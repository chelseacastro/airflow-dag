# Import Library
import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


def operator1(**kwargs):
    count = 2
    ti = kwargs['ti']
    count = count ** 2
    print('Initial Value: ', count)

    ti.xcom_push('count1', count)


def operator2(**kwargs):
    ti = kwargs['ti']
    count = ti.xcom_pull(task_ids='Task_1', key='count1')
    # print('Current Value: ', count)

    results = []
    for x in range(count):
        results.append(x)

    print('Current List: ', results)
    ti.xcom_push('count2', results)


def operator3(**kwargs):
    ti = kwargs['ti']
    count = ti.xcom_pull(task_ids='Task_2', key='count2')

    final_result = []
    for x in range(len(count)):
        x = x + 5
        final_result.append(x)

    print('Final List: ', final_result)


default_args = {
    'owner': 'chelsea',
    'start_date': dt.datetime(2022, 9, 10),
    'concurrency': 1,
    'retries': 0,
}
with DAG('latihandag1_chelsea', catchup=False, default_args=default_args, schedule_interval='* 5 */7 * *') as dag:
    # Task DAG
    opr_1 = PythonOperator(task_id='Task_1', python_callable=operator1)
    opr_2 = PythonOperator(task_id='Task_2', python_callable=operator2)
    opr_3 = PythonOperator(task_id='Task_3', python_callable=operator3)

# Work Flow
opr_1 >> opr_2 >> opr_3
