"""Central SQLite store for investigations, approvals, run history, and recommendations."""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Generator

from config import config


class InvestigationStatus(StrEnum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    RETRY_IN_PROGRESS = "retry_in_progress"
    RESOLVED = "resolved"
    PENDING_RETRY = "pending_retry"
    FAILED_PERMANENT = "failed_permanent"
    STALE = "stale"


class ApprovalStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


_DB_PATH = config.db_path
_local = threading.local()


def _conn() -> sqlite3.Connection:
    if not hasattr(_local, "conn"):
        _local.conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
    return _local.conn


@contextmanager
def _tx() -> Generator[sqlite3.Connection, None, None]:
    conn = _conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db() -> None:
    with _tx() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS investigations (
                run_id TEXT PRIMARY KEY,
                dag_id TEXT NOT NULL,
                task_id TEXT,
                diagnosis TEXT,
                recommended_action TEXT,
                error_summary TEXT,
                airflow_version TEXT,
                llm_model TEXT,
                timestamp TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'completed',
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS pending_approvals (
                run_id TEXT PRIMARY KEY,
                dag_id TEXT NOT NULL,
                action TEXT NOT NULL,
                details TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                resolved_at TEXT
            );

            CREATE TABLE IF NOT EXISTS run_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dag_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                duration_seconds REAL,
                state TEXT,
                timestamp TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_run_history_dag_task
                ON run_history(dag_id, task_id);

            CREATE TABLE IF NOT EXISTS recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dag_id TEXT NOT NULL,
                content TEXT NOT NULL,
                generated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS anomalies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dag_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                actual_duration REAL,
                expected_p90 REAL,
                detected_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS airflow_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collected_at TEXT NOT NULL,
                scheduler_status TEXT,
                total_dags INTEGER,
                active_dags INTEGER,
                paused_dags INTEGER,
                import_errors INTEGER,
                runs_24h INTEGER,
                runs_24h_success INTEGER,
                runs_24h_failed INTEGER
            );

            CREATE TABLE IF NOT EXISTS dag_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dag_id TEXT NOT NULL,
                collected_at TEXT NOT NULL,
                runs_7d INTEGER,
                success_7d INTEGER,
                failed_7d INTEGER,
                success_rate REAL,
                avg_duration_seconds REAL,
                last_run_at TEXT,
                last_run_state TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_dag_stats_dag_collected
                ON dag_stats(dag_id, collected_at);
        """)
        # Migrations for columns added after initial schema
        for col, definition in [
            ("retry_count", "INTEGER NOT NULL DEFAULT 0"),
            ("last_error", "TEXT"),
            ("llm_model", "TEXT"),
        ]:
            try:
                conn.execute(
                    f"ALTER TABLE investigations ADD COLUMN {col} {definition}"
                )
            except sqlite3.OperationalError:
                pass  # column already exists


# ── investigations ────────────────────────────────────────────────────────────


def upsert_investigation(
    run_id: str,
    dag_id: str,
    task_id: str,
    diagnosis: str,
    recommended_action: str,
    error_summary: str,
    airflow_version: str,
    status: str = InvestigationStatus.COMPLETED,
    llm_model: str = "",
) -> None:
    with _tx() as conn:
        conn.execute(
            """INSERT INTO investigations
               (run_id, dag_id, task_id, diagnosis, recommended_action,
                error_summary, airflow_version, llm_model, timestamp, status)
               VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(run_id) DO UPDATE SET
                 task_id=excluded.task_id,
                 diagnosis=excluded.diagnosis,
                 recommended_action=excluded.recommended_action,
                 error_summary=excluded.error_summary,
                 llm_model=excluded.llm_model,
                 status=excluded.status""",
            (
                run_id,
                dag_id,
                task_id,
                diagnosis,
                recommended_action,
                error_summary,
                airflow_version,
                llm_model,
                _now(),
                status,
            ),
        )


def get_recent_investigations(limit: int = 50) -> list[dict]:
    rows = (
        _conn()
        .execute(
            "SELECT * FROM investigations ORDER BY timestamp DESC LIMIT ?", (limit,)
        )
        .fetchall()
    )
    return [dict(r) for r in rows]


def get_investigation(run_id: str) -> dict | None:
    row = (
        _conn()
        .execute("SELECT * FROM investigations WHERE run_id=?", (run_id,))
        .fetchone()
    )
    return dict(row) if row else None


def count_investigations_since(days: int = 7) -> int:
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    return (
        _conn()
        .execute("SELECT COUNT(*) FROM investigations WHERE timestamp > ?", (cutoff,))
        .fetchone()[0]
    )


def most_failing_dag(days: int = 7) -> str | None:
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    row = (
        _conn()
        .execute(
            """SELECT dag_id, COUNT(*) AS cnt FROM investigations
           WHERE timestamp > ? GROUP BY dag_id ORDER BY cnt DESC LIMIT 1""",
            (cutoff,),
        )
        .fetchone()
    )
    return row["dag_id"] if row else None


# ── pending_approvals ─────────────────────────────────────────────────────────


def create_pending_approval(
    run_id: str, dag_id: str, action: str, details: str
) -> None:
    with _tx() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO pending_approvals
               (run_id, dag_id, action, details, status, created_at)
               VALUES (?,?,?,?,?,?)""",
            (run_id, dag_id, action, details, ApprovalStatus.PENDING, _now()),
        )


def get_pending_approvals() -> list[dict]:
    rows = (
        _conn()
        .execute(
            "SELECT * FROM pending_approvals WHERE status=? ORDER BY created_at DESC",
            (ApprovalStatus.PENDING,),
        )
        .fetchall()
    )
    return [dict(r) for r in rows]


def resolve_approval(run_id: str, decision: str) -> None:
    """decision: 'approved' | 'rejected'"""
    with _tx() as conn:
        conn.execute(
            "UPDATE pending_approvals SET status=?, resolved_at=? WHERE run_id=?",
            (decision, _now(), run_id),
        )


def get_resolved_approvals() -> list[dict]:
    """Return approvals resolved since last poller check (not yet acted on)."""
    rows = (
        _conn()
        .execute(
            """SELECT * FROM pending_approvals
           WHERE status IN (?,?) AND resolved_at IS NOT NULL
           ORDER BY resolved_at ASC""",
            (ApprovalStatus.APPROVED, ApprovalStatus.REJECTED),
        )
        .fetchall()
    )
    return [dict(r) for r in rows]


def mark_approval_actioned(run_id: str) -> None:
    with _tx() as conn:
        conn.execute(
            "UPDATE pending_approvals SET resolved_at=NULL WHERE run_id=?", (run_id,)
        )


# ── run_history ───────────────────────────────────────────────────────────────


def insert_run_history(
    dag_id: str, task_id: str, run_id: str, duration_seconds: float | None, state: str
) -> None:
    with _tx() as conn:
        conn.execute(
            """INSERT INTO run_history (dag_id, task_id, run_id, duration_seconds, state, timestamp)
               VALUES (?,?,?,?,?,?)""",
            (dag_id, task_id, run_id, duration_seconds, state, _now()),
        )


def get_run_history(
    dag_id: str, task_id: str | None = None, limit: int = 50
) -> list[dict]:
    if task_id:
        rows = (
            _conn()
            .execute(
                """SELECT * FROM run_history WHERE dag_id=? AND task_id=?
               ORDER BY timestamp DESC LIMIT ?""",
                (dag_id, task_id, limit),
            )
            .fetchall()
        )
    else:
        rows = (
            _conn()
            .execute(
                "SELECT * FROM run_history WHERE dag_id=? ORDER BY timestamp DESC LIMIT ?",
                (dag_id, limit),
            )
            .fetchall()
        )
    return [dict(r) for r in rows]


def get_dag_failure_count(dag_id: str, days: int = 7) -> int:
    """Count of distinct DAG run failures (each investigation = one DAG run failure)."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    return (
        _conn()
        .execute(
            "SELECT COUNT(*) FROM investigations WHERE dag_id=? AND timestamp > ?",
            (dag_id, cutoff),
        )
        .fetchone()[0]
    )


def record_investigation_failure(run_id: str, dag_id: str, error: str) -> None:
    """Increment retry_count and set status=pending_retry. Does not overwrite a completed investigation."""
    with _tx() as conn:
        conn.execute(
            """INSERT INTO investigations
               (run_id, dag_id, task_id, diagnosis, recommended_action, error_summary,
                airflow_version, timestamp, status, retry_count, last_error)
               VALUES (?,?,'','','','','',?,?,1,?)
               ON CONFLICT(run_id) DO UPDATE SET
                 status=CASE WHEN status IN (?,?,?) THEN status
                             ELSE ? END,
                 retry_count=retry_count+1,
                 last_error=excluded.last_error,
                 timestamp=excluded.timestamp""",
            (
                run_id,
                dag_id,
                _now(),
                InvestigationStatus.PENDING_RETRY,
                error,
                InvestigationStatus.COMPLETED,
                InvestigationStatus.APPROVED,
                InvestigationStatus.REJECTED,
                InvestigationStatus.PENDING_RETRY,
            ),
        )


def get_pending_retry_investigations() -> list[dict]:
    rows = (
        _conn()
        .execute(
            "SELECT * FROM investigations WHERE status=? ORDER BY timestamp ASC",
            (InvestigationStatus.PENDING_RETRY,),
        )
        .fetchall()
    )
    return [dict(r) for r in rows]


def get_investigations_by_status(status: str) -> list[dict]:
    rows = (
        _conn()
        .execute(
            "SELECT * FROM investigations WHERE status=? ORDER BY timestamp ASC",
            (status,),
        )
        .fetchall()
    )
    return [dict(r) for r in rows]


def get_retry_in_progress() -> list[dict]:
    rows = (
        _conn()
        .execute(
            "SELECT * FROM investigations WHERE status=? ORDER BY timestamp ASC",
            (InvestigationStatus.RETRY_IN_PROGRESS,),
        )
        .fetchall()
    )
    return [dict(r) for r in rows]


def mark_investigation_failed_permanent(run_id: str) -> None:
    with _tx() as conn:
        conn.execute(
            "UPDATE investigations SET status=? WHERE run_id=?",
            (InvestigationStatus.FAILED_PERMANENT, run_id),
        )


def get_all_dag_ids() -> list[str]:
    rows = (
        _conn()
        .execute("SELECT DISTINCT dag_id FROM investigations ORDER BY dag_id")
        .fetchall()
    )
    return [r["dag_id"] for r in rows]


def get_dag_duration_history(dag_id: str, days: int = 14) -> list[dict]:
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    rows = (
        _conn()
        .execute(
            """SELECT task_id, AVG(duration_seconds) AS avg_duration, DATE(timestamp) AS day
           FROM run_history WHERE dag_id=? AND timestamp > ? AND duration_seconds IS NOT NULL
           GROUP BY task_id, DATE(timestamp) ORDER BY day""",
            (dag_id, cutoff),
        )
        .fetchall()
    )
    return [dict(r) for r in rows]


# ── anomalies ─────────────────────────────────────────────────────────────────


def insert_anomaly(
    dag_id: str, task_id: str, run_id: str, actual: float, expected_p90: float
) -> None:
    with _tx() as conn:
        conn.execute(
            """INSERT INTO anomalies (dag_id, task_id, run_id, actual_duration, expected_p90, detected_at)
               VALUES (?,?,?,?,?,?)""",
            (dag_id, task_id, run_id, actual, expected_p90, _now()),
        )


def get_recent_anomalies(days: int = 7) -> list[dict]:
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    rows = (
        _conn()
        .execute(
            "SELECT * FROM anomalies WHERE detected_at > ? ORDER BY detected_at DESC",
            (cutoff,),
        )
        .fetchall()
    )
    return [dict(r) for r in rows]


# ── recommendations ───────────────────────────────────────────────────────────


def upsert_recommendation(dag_id: str, content: str) -> None:
    with _tx() as conn:
        conn.execute(
            "INSERT INTO recommendations (dag_id, content, generated_at) VALUES (?,?,?)",
            (dag_id, content, _now()),
        )


def get_recommendations(dag_id: str | None = None) -> list[dict]:
    if dag_id:
        rows = (
            _conn()
            .execute(
                "SELECT * FROM recommendations WHERE dag_id=? ORDER BY generated_at DESC LIMIT 5",
                (dag_id,),
            )
            .fetchall()
        )
    else:
        rows = (
            _conn()
            .execute(
                "SELECT * FROM recommendations ORDER BY generated_at DESC LIMIT 20"
            )
            .fetchall()
        )
    return [dict(r) for r in rows]


# ── airflow_snapshots ─────────────────────────────────────────────────────────


def insert_airflow_snapshot(
    scheduler_status: str,
    total_dags: int,
    active_dags: int,
    paused_dags: int,
    import_errors: int,
    runs_24h: int,
    runs_24h_success: int,
    runs_24h_failed: int,
) -> None:
    with _tx() as conn:
        conn.execute(
            """INSERT INTO airflow_snapshots
               (collected_at, scheduler_status, total_dags, active_dags, paused_dags,
                import_errors, runs_24h, runs_24h_success, runs_24h_failed)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                _now(),
                scheduler_status,
                total_dags,
                active_dags,
                paused_dags,
                import_errors,
                runs_24h,
                runs_24h_success,
                runs_24h_failed,
            ),
        )


def get_latest_snapshot() -> dict | None:
    row = (
        _conn()
        .execute("SELECT * FROM airflow_snapshots ORDER BY collected_at DESC LIMIT 1")
        .fetchone()
    )
    return dict(row) if row else None


def get_snapshot_history(days: int = 7) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    rows = (
        _conn()
        .execute(
            "SELECT * FROM airflow_snapshots WHERE collected_at > ? ORDER BY collected_at ASC",
            (cutoff,),
        )
        .fetchall()
    )
    return [dict(r) for r in rows]


# ── dag_stats ─────────────────────────────────────────────────────────────────


def upsert_dag_stats(
    dag_id: str,
    runs_7d: int,
    success_7d: int,
    failed_7d: int,
    success_rate: float,
    avg_duration_seconds: float | None,
    last_run_at: str | None,
    last_run_state: str | None,
) -> None:
    with _tx() as conn:
        conn.execute(
            """INSERT INTO dag_stats
               (dag_id, collected_at, runs_7d, success_7d, failed_7d, success_rate,
                avg_duration_seconds, last_run_at, last_run_state)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                dag_id,
                _now(),
                runs_7d,
                success_7d,
                failed_7d,
                success_rate,
                avg_duration_seconds,
                last_run_at,
                last_run_state,
            ),
        )


def get_latest_dag_stats() -> list[dict]:
    """Most recent stats row per DAG."""
    rows = (
        _conn()
        .execute(
            """SELECT * FROM dag_stats WHERE collected_at = (
               SELECT MAX(collected_at) FROM dag_stats AS d2 WHERE d2.dag_id = dag_stats.dag_id
           ) ORDER BY failed_7d DESC, dag_id"""
        )
        .fetchall()
    )
    return [dict(r) for r in rows]


def get_dag_stats_history(dag_id: str, days: int = 7) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    rows = (
        _conn()
        .execute(
            "SELECT * FROM dag_stats WHERE dag_id=? AND collected_at > ? ORDER BY collected_at ASC",
            (dag_id, cutoff),
        )
        .fetchall()
    )
    return [dict(r) for r in rows]


def cleanup_old_records(retention_days: int) -> dict[str, int]:
    """Delete records older than retention_days. Returns counts per table. Skips pending_approvals."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
    tables = [
        ("investigations", "timestamp"),
        ("run_history", "timestamp"),
        ("anomalies", "detected_at"),
        ("airflow_snapshots", "collected_at"),
        ("dag_stats", "collected_at"),
    ]
    deleted: dict[str, int] = {}
    with _tx() as conn:
        for table, ts_col in tables:
            cursor = conn.execute(
                f"DELETE FROM {table} WHERE {ts_col} < ?",
                (cutoff,),  # noqa: S608
            )
            deleted[table] = cursor.rowcount
    return deleted


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
