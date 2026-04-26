"""Polling loop: detect new failures, invoke investigation graph, handle approvals."""

import logging
import sqlite3
import time

# Configure logging before any imports that may call basicConfig (e.g. LiteLLM)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
_root = logging.getLogger()
_root.addHandler(_handler)
_root.setLevel(logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

import httpx  # noqa: E402
from langgraph.types import Command  # noqa: E402

import db.store as db  # noqa: E402
from agent.graph import get_graph  # noqa: E402
from agent.tools import airflow_client as client  # noqa: E402
from config import config  # noqa: E402
from db.store import InvestigationStatus  # noqa: E402

log = logging.getLogger(__name__)

SEEN_DB = ".seen_runs.db"
MAX_INVESTIGATION_RETRIES = 3


def _init_seen(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS seen_runs (run_id TEXT PRIMARY KEY)")
    conn.commit()


def _is_seen(conn: sqlite3.Connection, run_id: str) -> bool:
    return (
        conn.execute("SELECT 1 FROM seen_runs WHERE run_id=?", (run_id,)).fetchone()
        is not None
    )


def _mark_seen(conn: sqlite3.Connection, run_id: str) -> None:
    conn.execute("INSERT OR IGNORE INTO seen_runs VALUES (?)", (run_id,))
    conn.commit()


def _initial_state(dag_id: str, run_id: str) -> dict:
    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "airflow_version": config.airflow_api_version,
        "triggered_by": "poller",
        "similar_past_investigations": [],
        "investigation_notes": "",
        "task_instances": [],
        "diagnosis": "",
        "recommended_action": "",
        "error_summary": "",
        "primary_failing_task": "",
        "report": "",
        "action_decision": "",
        "llm_model_used": "",
    }


def resume_incomplete(graph, seen_conn: sqlite3.Connection) -> None:
    """On startup, re-investigate interrupted or pending-retry runs from DB."""
    candidates = (
        db.get_investigations_by_status(InvestigationStatus.IN_PROGRESS)
        + db.get_pending_retry_investigations()
    )
    if not candidates:
        return
    log.info("Found %d incomplete investigation(s) to resume.", len(candidates))
    for i, inv in enumerate(candidates):
        if i > 0:
            time.sleep(5)
        run_id, dag_id = inv["run_id"], inv["dag_id"]
        if inv.get("retry_count", 0) >= MAX_INVESTIGATION_RETRIES:
            log.warning(
                "Run %s/%s exhausted retries — marking failed_permanent.",
                dag_id,
                run_id,
            )
            db.mark_investigation_failed_permanent(run_id)
            _mark_seen(seen_conn, run_id)
            continue
        thread_config = {"configurable": {"thread_id": f"{run_id}__resume"}}
        db.upsert_investigation(
            run_id=run_id,
            dag_id=dag_id,
            task_id=inv.get("task_id", ""),
            diagnosis="",
            recommended_action="",
            error_summary="",
            airflow_version=inv.get("airflow_version") or config.airflow_api_version,
            status=InvestigationStatus.IN_PROGRESS,
        )
        state = _initial_state(dag_id, run_id)
        if run_id.startswith("import_error__"):
            state["triggered_by"] = "import_error"
        try:
            graph.invoke(state, config=thread_config)
            _mark_seen(seen_conn, run_id)
        except (
            Exception
        ) as e:  # LangGraph can raise anything; keep broad to prevent poller crash
            log.error("Resume failed for %s/%s: %s", dag_id, run_id, e)
            db.record_investigation_failure(run_id, dag_id, str(e))


def check_import_errors(graph, seen_conn: sqlite3.Connection) -> None:
    """Import errors never produce DAG runs — detect and investigate them separately."""
    try:
        errors = client.get_import_errors()
    except (httpx.HTTPError, httpx.ConnectError, httpx.TimeoutException) as e:
        log.error("Failed to fetch import errors: %s", e)
        return

    for err in errors:
        synthetic_id = f"import_error__{err.get('filename', 'unknown')}"
        if _is_seen(seen_conn, synthetic_id):
            continue
        filename = err.get("filename", "unknown")
        dag_id = (
            filename.split("/")[-1].replace(".py", "") if filename else "unknown_dag"
        )

        existing = db.get_investigation(synthetic_id)
        if existing and existing.get("retry_count", 0) >= MAX_INVESTIGATION_RETRIES:
            log.warning(
                "Import error %s exhausted retries — marking failed_permanent.",
                filename,
            )
            db.mark_investigation_failed_permanent(synthetic_id)
            _mark_seen(seen_conn, synthetic_id)
            continue

        log.info("New import error in: %s", filename)
        db.upsert_investigation(
            run_id=synthetic_id,
            dag_id=dag_id,
            task_id="",
            diagnosis="",
            recommended_action="",
            error_summary="",
            airflow_version=config.airflow_api_version,
            status=InvestigationStatus.IN_PROGRESS,
        )
        state = _initial_state(dag_id, synthetic_id)
        state["triggered_by"] = "import_error"
        thread_config = {"configurable": {"thread_id": synthetic_id}}
        try:
            graph.invoke(state, config=thread_config)
            _mark_seen(seen_conn, synthetic_id)
        except (
            Exception
        ) as e:  # LangGraph can raise anything; keep broad to prevent poller crash
            log.error("Investigation failed for import error %s: %s", filename, e)
            db.record_investigation_failure(synthetic_id, dag_id, str(e))


def run_investigations(graph, seen_conn: sqlite3.Connection) -> None:
    check_import_errors(graph, seen_conn)

    try:
        runs = client.list_all_failed_dag_runs(limit=20)
    except (httpx.HTTPError, httpx.ConnectError, httpx.TimeoutException) as e:
        log.error("Failed to fetch failed runs: %s", e)
        return

    new_runs = [r for r in runs if not _is_seen(seen_conn, r["dag_run_id"])]
    if not new_runs:
        log.info("No new failures.")
        return

    for i, run in enumerate(new_runs):
        if i > 0:
            time.sleep(5)
        dag_id = run["dag_id"]
        run_id = run["dag_run_id"]

        existing = db.get_investigation(run_id)
        if existing and existing.get("retry_count", 0) >= MAX_INVESTIGATION_RETRIES:
            log.warning(
                "Run %s/%s exhausted %d retries — marking failed_permanent.",
                dag_id,
                run_id,
                MAX_INVESTIGATION_RETRIES,
            )
            db.mark_investigation_failed_permanent(run_id)
            _mark_seen(seen_conn, run_id)
            continue

        log.info("New failure: DAG=%s run=%s", dag_id, run_id)
        db.upsert_investigation(
            run_id=run_id,
            dag_id=dag_id,
            task_id="",
            diagnosis="",
            recommended_action="",
            error_summary="",
            airflow_version=config.airflow_api_version,
            status=InvestigationStatus.IN_PROGRESS,
        )
        thread_config = {"configurable": {"thread_id": run_id}}
        try:
            graph.invoke(_initial_state(dag_id, run_id), config=thread_config)
            state = graph.get_state(thread_config)
            if state.next and "execute_action" in state.next:
                log.info(
                    "Investigation for %s/%s is pending human approval.", dag_id, run_id
                )
            else:
                log.info(
                    "Investigation for %s/%s completed (no action required).",
                    dag_id,
                    run_id,
                )
            _mark_seen(seen_conn, run_id)
        except Exception as e:  # LangGraph can raise anything; intentionally NOT marked seen — will retry next cycle
            log.error("Investigation failed for %s/%s: %s", dag_id, run_id, e)
            db.record_investigation_failure(run_id, dag_id, str(e))


_RETRY_TIMEOUT_HOURS = 24


def check_retry_outcomes(graph, seen_conn: sqlite3.Connection) -> None:
    retrying = db.get_retry_in_progress()
    if not retrying:
        return

    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(hours=_RETRY_TIMEOUT_HOURS)

    for inv in retrying:
        dag_id, run_id, task_id = inv["dag_id"], inv["run_id"], inv["task_id"]
        timed_out = datetime.fromisoformat(inv["timestamp"]) < cutoff
        try:
            instances = client.get_task_instances(dag_id, run_id)
            task = next((t for t in instances if t["task_id"] == task_id), None)
            if not task:
                if timed_out:
                    log.warning(
                        "Retry timed out (task not found) for %s/%s task=%s — marking stale.",
                        dag_id,
                        run_id,
                        task_id,
                    )
                    db.upsert_investigation(
                        run_id=run_id,
                        dag_id=dag_id,
                        task_id=task_id,
                        diagnosis=inv["diagnosis"],
                        recommended_action=inv["recommended_action"],
                        error_summary=inv["error_summary"],
                        airflow_version=inv["airflow_version"]
                        or config.airflow_api_version,
                        status=InvestigationStatus.STALE,
                        llm_model=inv.get("llm_model", ""),
                    )
                continue

            state = task.get("state")
            if state == "success":
                log.info("Retry succeeded for %s/%s task=%s", dag_id, run_id, task_id)
                db.upsert_investigation(
                    run_id=run_id,
                    dag_id=dag_id,
                    task_id=task_id,
                    diagnosis=inv["diagnosis"],
                    recommended_action=inv["recommended_action"],
                    error_summary=inv["error_summary"],
                    airflow_version=inv["airflow_version"]
                    or config.airflow_api_version,
                    status=InvestigationStatus.RESOLVED,
                    llm_model=inv.get("llm_model", ""),
                )
            elif state == "failed":
                log.info(
                    "Retry failed for %s/%s task=%s — re-investigating.",
                    dag_id,
                    run_id,
                    task_id,
                )
                reinvestigate_id = f"{run_id}__reinvestigate"
                thread_config = {"configurable": {"thread_id": reinvestigate_id}}
                db.upsert_investigation(
                    run_id=run_id,
                    dag_id=dag_id,
                    task_id=task_id,
                    diagnosis="",
                    recommended_action="",
                    error_summary="",
                    airflow_version=inv["airflow_version"]
                    or config.airflow_api_version,
                    status=InvestigationStatus.IN_PROGRESS,
                    llm_model="",
                )
                graph.invoke(_initial_state(dag_id, run_id), config=thread_config)
            elif timed_out:
                log.warning(
                    "Retry timed out for %s/%s task=%s after %dh — marking stale.",
                    dag_id,
                    run_id,
                    task_id,
                    _RETRY_TIMEOUT_HOURS,
                )
                db.upsert_investigation(
                    run_id=run_id,
                    dag_id=dag_id,
                    task_id=task_id,
                    diagnosis=inv["diagnosis"],
                    recommended_action=inv["recommended_action"],
                    error_summary=inv["error_summary"],
                    airflow_version=inv["airflow_version"]
                    or config.airflow_api_version,
                    status=InvestigationStatus.STALE,
                    llm_model=inv.get("llm_model", ""),
                )
        except (httpx.HTTPError, httpx.ConnectError, httpx.TimeoutException) as e:
            log.error("Failed to check retry outcome for %s/%s: %s", dag_id, run_id, e)
            if timed_out:
                log.warning(
                    "Marking %s/%s stale after API error and %dh timeout.",
                    dag_id,
                    run_id,
                    _RETRY_TIMEOUT_HOURS,
                )
                db.upsert_investigation(
                    run_id=run_id,
                    dag_id=dag_id,
                    task_id=task_id,
                    diagnosis=inv["diagnosis"],
                    recommended_action=inv["recommended_action"],
                    error_summary=inv["error_summary"],
                    airflow_version=inv["airflow_version"]
                    or config.airflow_api_version,
                    status=InvestigationStatus.STALE,
                    llm_model=inv.get("llm_model", ""),
                )


def process_approvals(graph) -> None:
    resolved = db.get_resolved_approvals()
    if not resolved:
        return

    for approval in resolved:
        run_id = approval["run_id"]
        decision = approval["status"]  # "approved" | "rejected"
        log.info("Processing approval decision=%s for run=%s", decision, run_id)

        thread_config = {"configurable": {"thread_id": run_id}}
        try:
            graph.invoke(
                Command(resume=None, update={"action_decision": decision}),
                config=thread_config,
            )
            log.info("Resumed graph for %s with decision=%s", run_id, decision)
        except (
            Exception
        ) as e:  # LangGraph can raise anything; finally block always cleans up
            log.error("Failed to resume graph for %s: %s", run_id, e)
        finally:
            db.mark_approval_actioned(run_id)


def main() -> None:
    db.init_db()
    graph = get_graph()
    seen_conn = sqlite3.connect(SEEN_DB)
    _init_seen(seen_conn)

    if config.airflow_url.startswith("http://"):
        log.warning(
            "AIRFLOW_URL uses plain HTTP — credentials will be sent unencrypted. "
            "Set AIRFLOW_URL to https:// for production use."
        )

    log.info(
        "Poller starting — Airflow %s at %s, interval=%ds",
        config.airflow_api_version,
        config.airflow_url,
        config.poll_interval,
    )

    resume_incomplete(graph, seen_conn)
    check_retry_outcomes(graph, seen_conn)

    last_proactive = 0.0  # triggers immediately on first cycle
    last_metrics = 0.0  # triggers immediately on first cycle
    last_cleanup = 0.0  # triggers immediately on first cycle
    _CLEANUP_INTERVAL_S = 86400  # once per day

    try:
        while True:
            run_investigations(graph, seen_conn)
            process_approvals(graph)
            check_retry_outcomes(graph, seen_conn)

            if time.time() - last_metrics > 900:  # every 15 minutes
                from agent.metrics import collect_metrics

                try:
                    collect_metrics()
                    last_metrics = time.time()
                except Exception as e:
                    log.error("Metrics collection failed: %s", e)

            if time.time() - last_cleanup > _CLEANUP_INTERVAL_S:
                try:
                    deleted = db.cleanup_old_records(config.db_retention_days)
                    total = sum(deleted.values())
                    if total:
                        log.info(
                            "DB cleanup: removed %d old records (retention=%dd): %s",
                            total,
                            config.db_retention_days,
                            deleted,
                        )
                    last_cleanup = time.time()
                except Exception as e:
                    log.error("DB cleanup failed: %s", e)

            if time.time() - last_proactive > 3600:
                if _run_proactive(graph):
                    last_proactive = time.time()

            _retry_pending_from_db(graph, seen_conn)

            time.sleep(config.poll_interval)
    except KeyboardInterrupt:
        log.info("Poller stopped.")
    finally:
        seen_conn.close()


def _run_proactive(graph) -> bool:
    try:
        from agent.proactive.graph import proactive_graph

        dag_ids = [d["dag_id"] for d in client.list_dags()]
        proactive_graph.invoke(
            {
                "dag_ids": dag_ids,
                "metrics": {},
                "trend_analysis": "",
                "recommendations": [],
            }
        )
        log.info("Proactive analysis complete.")
        return True
    except Exception as e:
        log.error("Proactive analysis failed: %s", e)
        return False


def _retry_pending_from_db(graph, seen_conn: sqlite3.Connection) -> None:
    """Mid-session check: re-investigate pending_retry runs that may have slipped out of the Airflow API window."""
    pending = db.get_pending_retry_investigations()
    for inv in pending:
        run_id, dag_id = inv["run_id"], inv["dag_id"]
        if _is_seen(seen_conn, run_id):
            continue
        if inv.get("retry_count", 0) >= MAX_INVESTIGATION_RETRIES:
            log.warning(
                "Run %s/%s exhausted retries — marking failed_permanent.",
                dag_id,
                run_id,
            )
            db.mark_investigation_failed_permanent(run_id)
            _mark_seen(seen_conn, run_id)
            continue
        log.info(
            "Retrying pending_retry investigation for %s/%s (attempt %d).",
            dag_id,
            run_id,
            inv.get("retry_count", 0) + 1,
        )
        thread_config = {"configurable": {"thread_id": f"{run_id}__retry"}}
        db.upsert_investigation(
            run_id=run_id,
            dag_id=dag_id,
            task_id=inv.get("task_id", ""),
            diagnosis="",
            recommended_action="",
            error_summary="",
            airflow_version=inv.get("airflow_version") or config.airflow_api_version,
            status=InvestigationStatus.IN_PROGRESS,
        )
        state = _initial_state(dag_id, run_id)
        if run_id.startswith("import_error__"):
            state["triggered_by"] = "import_error"
        try:
            graph.invoke(state, config=thread_config)
            _mark_seen(seen_conn, run_id)
        except (
            Exception
        ) as e:  # LangGraph can raise anything; keep broad to prevent poller crash
            log.error("Retry investigation failed for %s/%s: %s", dag_id, run_id, e)
            db.record_investigation_failure(run_id, dag_id, str(e))


if __name__ == "__main__":
    main()
