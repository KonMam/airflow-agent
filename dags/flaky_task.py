import random
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def flaky_operation():
    if random.random() < 0.5:
        raise ConnectionError(
            "Flaky failure: intermittent network timeout (50% chance)"
        )
    print("Flaky task succeeded this time")


with DAG(
    dag_id="flaky_task",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "flaky"],
) as dag:
    PythonOperator(task_id="flaky_op", python_callable=flaky_operation)
