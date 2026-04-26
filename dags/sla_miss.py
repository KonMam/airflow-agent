import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def slow_task():
    time.sleep(10)
    print("Slow task completed after 10 seconds")


with DAG(
    dag_id="sla_miss",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"sla": timedelta(seconds=2)},
    tags=["test", "sla"],
) as dag:
    PythonOperator(
        task_id="slow_op", python_callable=slow_task, sla=timedelta(seconds=2)
    )
