"""Nightly revenue aggregation: computes daily/weekly/monthly rollups."""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def compute_daily_revenue(**context):
    # Simulate fetching from a DB — returns empty result set outside business hours
    rows = []  # Empty: no transactions recorded yet for this partition
    context["ti"].xcom_push(key="daily_rows", value=rows)
    print(f"Fetched {len(rows)} transaction rows for {context['ds']}")


def compute_weekly_rollup(**context):
    daily_rows = context["ti"].xcom_pull(
        task_ids="compute_daily_revenue", key="daily_rows"
    )
    total_revenue = sum(r.get("amount", 0) for r in daily_rows)
    context["ti"].xcom_push(key="weekly_revenue", value=total_revenue)


def compute_arpu(**context):
    weekly_revenue = context["ti"].xcom_pull(
        task_ids="compute_weekly_rollup", key="weekly_revenue"
    )
    # Fetch active user count from a separate system
    import os

    active_users = int(os.getenv("ACTIVE_USER_COUNT", "0"))
    if active_users == 0:
        raise ZeroDivisionError(
            "Cannot compute ARPU: active_user_count is 0. "
            "The user metrics service may be unavailable or the segment query returned no results. "
            f"weekly_revenue={weekly_revenue}, partition_date={context['ds']}"
        )
    arpu = weekly_revenue / active_users
    print(f"ARPU: {arpu:.2f}")


def publish_to_dashboard(**context):
    print("Publishing metrics to Metabase dashboard.")


with DAG(
    dag_id="nightly_aggregation",
    start_date=datetime(2024, 1, 1),
    schedule="0 1 * * *",
    catchup=False,
    default_args={"retries": 0},
    tags=["analytics", "revenue", "nightly"],
) as dag:
    t1 = PythonOperator(
        task_id="compute_daily_revenue", python_callable=compute_daily_revenue
    )
    t2 = PythonOperator(
        task_id="compute_weekly_rollup", python_callable=compute_weekly_rollup
    )
    t3 = PythonOperator(task_id="compute_arpu", python_callable=compute_arpu)
    t4 = PythonOperator(
        task_id="publish_to_dashboard", python_callable=publish_to_dashboard
    )
    t1 >> t2 >> t3 >> t4
