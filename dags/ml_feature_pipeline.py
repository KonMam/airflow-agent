"""ML feature pipeline: computes and publishes features for the churn prediction model."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def fetch_raw_events(**context):
    # Simulate event data — some sessions have null engagement scores
    import random

    random.seed(int(context["ds_nodash"]))
    events = [
        {
            "user_id": f"u{i}",
            "session_duration": random.randint(10, 600),
            "page_views": random.randint(1, 50),
            "engagement_score": random.choice([None, None, random.uniform(0, 1)]),
        }
        for i in range(200)
    ]
    context["ti"].xcom_push(key="events", value=events)
    print(f"Fetched {len(events)} raw events.")


def compute_engagement_features(**context):
    events = context["ti"].xcom_pull(task_ids="fetch_raw_events", key="events")
    null_count = sum(1 for e in events if e["engagement_score"] is None)
    null_rate = null_count / len(events)

    if null_rate > 0.4:
        raise ValueError(
            f"Feature quality check failed: engagement_score null rate is {null_rate:.1%} "
            f"({null_count}/{len(events)} records). Threshold is 40%. "
            f"The upstream event tracking service may have had an outage — "
            f"check event-collector logs for {context['ds']}."
        )

    features = [
        {
            "user_id": e["user_id"],
            "avg_session": e["session_duration"],
            "engagement": e["engagement_score"] or 0.0,
        }
        for e in events
    ]
    context["ti"].xcom_push(key="features", value=features)


def publish_features(**context):
    features = context["ti"].xcom_pull(
        task_ids="compute_engagement_features", key="features"
    )
    print(f"Published {len(features)} feature vectors to feature store.")


with DAG(
    dag_id="ml_feature_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 4 * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["ml", "features", "churn"],
) as dag:
    t1 = PythonOperator(task_id="fetch_raw_events", python_callable=fetch_raw_events)
    t2 = PythonOperator(
        task_id="compute_engagement_features",
        python_callable=compute_engagement_features,
    )
    t3 = PythonOperator(task_id="publish_features", python_callable=publish_features)
    t1 >> t2 >> t3
