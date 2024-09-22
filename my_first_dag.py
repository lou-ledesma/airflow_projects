from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 14),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("my_first_dag", default_args=default_args, schedule_interval=timedelta(days = 1))


def CreateDirectory():
    directory = os.getcwd()
    os.mkdir(directory, "test_dir")
    print(directory)

def PrintDone():
    print("Done!")


task_1 = PythonOperator(task_id = "mk_dir", python_callable = CreateDirectory, dag=dag)

task_2 = PythonOperator(task_id = "completion_confirmation", python_callable= PrintDone, dag = dag)

task_2.set_upstream(task_1)