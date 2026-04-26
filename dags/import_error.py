"""Segment export pipeline — broken due to missing internal library."""

from datetime import datetime

# This import would work in prod where the internal package is installed,
# but fails in environments where the package isn't available
from acme_utils.segment import SegmentClient  # noqa: F401
from airflow import DAG
from airflow.operators.python import PythonOperator


def export_segments():
    pass


with DAG(
    dag_id="segment_export_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["segments", "export"],
) as dag:
    PythonOperator(task_id="export", python_callable=export_segments)
