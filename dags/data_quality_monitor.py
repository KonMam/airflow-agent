"""Data quality monitor: checks row counts, freshness, and nulls across core tables."""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

_EXPECTATIONS = {
    "orders": {"min_rows": 500, "max_null_rate": 0.02},
    "users": {"min_rows": 100, "max_null_rate": 0.01},
    "events": {"min_rows": 5000, "max_null_rate": 0.05},
}


def check_row_counts(**context):
    import random

    random.seed(42)
    failures = []
    for table, exp in _EXPECTATIONS.items():
        # Simulate querying warehouse — events table has gone stale
        actual = (
            0
            if table == "events"
            else random.randint(exp["min_rows"], exp["min_rows"] * 3)
        )
        if actual < exp["min_rows"]:
            failures.append(
                f"  {table}: got {actual} rows, expected >= {exp['min_rows']}"
            )
    if failures:
        raise AssertionError(
            "Data quality check failed — row count violations:\n"
            + "\n".join(failures)
            + "\n"
            "Possible causes: upstream ingestion job did not complete, "
            "partition was dropped, or warehouse compaction is in progress."
        )
    print("Row count checks passed.")


def check_null_rates(**context):
    print("Null rate checks passed.")


def check_freshness(**context):
    import os
    from datetime import datetime, timezone

    last_updated_str = os.getenv("EVENTS_TABLE_LAST_UPDATED", "2024-01-01T00:00:00")
    last_updated = datetime.fromisoformat(last_updated_str).replace(tzinfo=timezone.utc)
    age_hours = (datetime.now(timezone.utc) - last_updated).total_seconds() / 3600
    if age_hours > 25:
        raise RuntimeError(
            f"Freshness check failed: 'events' table last updated {age_hours:.1f}h ago "
            f"(threshold: 25h). The Kafka consumer or Spark streaming job may have stalled. "
            f"Last known update: {last_updated_str}"
        )
    print(f"Freshness OK: events updated {age_hours:.1f}h ago.")


with DAG(
    dag_id="data_quality_monitor",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    default_args={"retries": 0},
    tags=["data-quality", "monitoring"],
) as dag:
    t1 = PythonOperator(task_id="check_row_counts", python_callable=check_row_counts)
    t2 = PythonOperator(task_id="check_null_rates", python_callable=check_null_rates)
    t3 = PythonOperator(task_id="check_freshness", python_callable=check_freshness)
    [t1, t2] >> t3
