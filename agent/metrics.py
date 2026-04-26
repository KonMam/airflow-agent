"""Periodic Airflow metrics collection — snapshots system health and per-DAG run stats."""

import logging
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import db.store as db
from agent.tools import airflow_client as client

_log = logging.getLogger(__name__)


def collect_metrics() -> None:
    _log.info("Collecting Airflow metrics snapshot.")
    try:
        _collect()
    except Exception as e:
        _log.error("Metrics collection failed: %s", e)


def _collect() -> None:
    health = client.get_health()
    scheduler_status = health.get("scheduler", {}).get("status", "unknown")

    dags = client.list_dags()
    total_dags = len(dags)
    active_dags = sum(1 for d in dags if not d.get("is_paused"))
    paused_dags = total_dags - active_dags
    import_errors = len(client.get_import_errors())

    now = datetime.now(timezone.utc)
    since_24h = (now - timedelta(hours=24)).isoformat()
    since_7d = (now - timedelta(days=7)).isoformat()

    runs_24h = client.list_all_dag_runs_since(since_24h)
    runs_7d = client.list_all_dag_runs_since(since_7d)

    db.insert_airflow_snapshot(
        scheduler_status=scheduler_status,
        total_dags=total_dags,
        active_dags=active_dags,
        paused_dags=paused_dags,
        import_errors=import_errors,
        runs_24h=len(runs_24h),
        runs_24h_success=sum(1 for r in runs_24h if r.get("state") == "success"),
        runs_24h_failed=sum(1 for r in runs_24h if r.get("state") == "failed"),
    )

    by_dag: dict[str, list[dict]] = defaultdict(list)
    for run in runs_7d:
        by_dag[run["dag_id"]].append(run)

    for dag_id, dag_runs in by_dag.items():
        total = len(dag_runs)
        success = sum(1 for r in dag_runs if r.get("state") == "success")
        failed = sum(1 for r in dag_runs if r.get("state") == "failed")

        durations = []
        for r in dag_runs:
            start, end = r.get("start_date"), r.get("end_date")
            if start and end:
                try:
                    s = datetime.fromisoformat(start.replace("Z", "+00:00"))
                    e = datetime.fromisoformat(end.replace("Z", "+00:00"))
                    durations.append((e - s).total_seconds())
                except Exception:
                    pass

        sorted_runs = sorted(
            dag_runs, key=lambda r: r.get("start_date", ""), reverse=True
        )
        last = sorted_runs[0] if sorted_runs else None

        db.upsert_dag_stats(
            dag_id=dag_id,
            runs_7d=total,
            success_7d=success,
            failed_7d=failed,
            success_rate=round(success / total, 3) if total else 0.0,
            avg_duration_seconds=round(statistics.mean(durations), 1)
            if durations
            else None,
            last_run_at=client.run_date_field(last) if last else None,
            last_run_state=last.get("state") if last else None,
        )

    _log.info(
        "Metrics snapshot saved — %d DAGs, %d active, %d 24h runs (%d success / %d failed).",
        total_dags,
        active_dags,
        len(runs_24h),
        sum(1 for r in runs_24h if r.get("state") == "success"),
        sum(1 for r in runs_24h if r.get("state") == "failed"),
    )
