"""
Простой тестовый DAG
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "me",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

dag = DAG(
    "test_dag",  # это имя должно быть уникальным
    default_args=default_args,
    description="Мой первый DAG",
    schedule_interval="@daily",
    catchup=False,
)

hello = BashOperator(
    task_id="hello",
    bash_command='echo "Hello, Airflow!"',
    dag=dag,
)
