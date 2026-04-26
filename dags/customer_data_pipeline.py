"""Daily customer data ETL: extracts from source, validates schema, loads to warehouse."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def extract_customers(**context):
    import csv
    import os

    # Simulate reading from a shared data mount that exists in prod but not locally
    source_path = os.getenv("CUSTOMER_DATA_PATH", "/data/exports/customers_daily.csv")
    if not os.path.exists(source_path):
        raise FileNotFoundError(
            f"[Errno 2] No such file or directory: '{source_path}'\n"
            f"Expected daily export from CRM at {source_path}. "
            f"Check if the upstream CRM export job completed successfully."
        )
    with open(source_path) as f:
        rows = list(csv.DictReader(f))
    context["ti"].xcom_push(key="row_count", value=len(rows))
    return rows


def validate_schema(**context):
    rows = context["ti"].xcom_pull(task_ids="extract")
    required_columns = {
        "customer_id",
        "email",
        "segment",
        "lifetime_value",
        "last_seen_at",
    }
    if rows:
        actual_columns = set(rows[0].keys())
        missing = required_columns - actual_columns
        if missing:
            raise KeyError(
                f"Schema validation failed: missing columns {sorted(missing)}. "
                f"Got columns: {sorted(actual_columns)}. "
                f"The CRM export schema may have changed — check with the data platform team."
            )


def load_to_warehouse(**context):
    rows = context["ti"].xcom_pull(task_ids="extract")
    print(f"Loading {len(rows)} customer records to warehouse.")


with DAG(
    dag_id="customer_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["etl", "customers", "daily"],
) as dag:
    t1 = PythonOperator(task_id="extract", python_callable=extract_customers)
    t2 = PythonOperator(task_id="validate_schema", python_callable=validate_schema)
    t3 = PythonOperator(task_id="load_to_warehouse", python_callable=load_to_warehouse)
    t1 >> t2 >> t3
