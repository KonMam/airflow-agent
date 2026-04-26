"""Syncs subscription data from the billing API into the data warehouse."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def fetch_subscriptions(**context):
    import json
    import os
    import urllib.request

    api_base = os.getenv("BILLING_API_URL", "http://billing-service.internal:8443")
    api_key = os.getenv("BILLING_API_KEY", "")

    try:
        req = urllib.request.Request(
            f"{api_base}/v2/subscriptions?since={context['ds']}&limit=1000",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except OSError as e:
        raise ConnectionError(
            f"Failed to reach billing API at {api_base}: {e}. "
            f"The service may be down or the VPN tunnel to the billing cluster is not established. "
            f"Check the billing-service health endpoint and Datadog for recent error spikes."
        ) from e

    context["ti"].xcom_push(key="subscriptions", value=data.get("items", []))


def upsert_to_warehouse(**context):
    subs = context["ti"].xcom_pull(task_ids="fetch_subscriptions", key="subscriptions")
    print(f"Upserting {len(subs)} subscription records.")


def reconcile_mrr(**context):
    print("Reconciling MRR with Stripe totals.")


with DAG(
    dag_id="api_data_sync",
    start_date=datetime(2024, 1, 1),
    schedule="0 */6 * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=3)},
    tags=["billing", "sync", "subscriptions"],
) as dag:
    t1 = PythonOperator(
        task_id="fetch_subscriptions", python_callable=fetch_subscriptions
    )
    t2 = PythonOperator(
        task_id="upsert_to_warehouse", python_callable=upsert_to_warehouse
    )
    t3 = PythonOperator(task_id="reconcile_mrr", python_callable=reconcile_mrr)
    t1 >> t2 >> t3
