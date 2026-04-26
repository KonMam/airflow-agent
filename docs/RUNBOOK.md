# Runbook

## Health Checks

**Airflow 2 (local):**
```bash
curl -s http://localhost:8080/health | python3 -m json.tool
# Expected: {"status": "healthy", "scheduler": {"status": "healthy", ...}}
```

**Airflow 3 (local):**
```bash
curl -s http://localhost:8081/health | python3 -m json.tool
```

**Agent poller:** No HTTP health endpoint. Verify it is running:
```bash
ps aux | grep poller.py
```

**Streamlit UI:** Visit `http://localhost:8501`. The dashboard auto-refreshes every 30 seconds and shows the count of incidents and pending approvals.

**SQLite database connectivity:**
```bash
sqlite3 .airflow_agent.db "SELECT COUNT(*) FROM investigations;"
```

**ChromaDB:**
```bash
python3 -c "import chromadb; c = chromadb.PersistentClient('.chroma'); print(c.get_collection('investigations').count())"
```

## Monitoring & Alerts

The agent does not emit external metrics or integrate with Prometheus, Datadog, OpenTelemetry, or Sentry. All observability is internal:

- **Airflow health snapshots** are collected every 15 minutes by `agent/metrics.py:collect_metrics()` and stored in the `airflow_snapshots` SQLite table. The **Metrics** page (`ui/pages/04_metrics.py`) shows the latest snapshot.
- **Anomalies** — task duration outliers detected via IQR-P90 heuristic — are written to the `anomalies` table and displayed as alerts on the **DAG Health** page (`ui/pages/03_dag_health.py`).
- **Proactive trend analysis** runs every 60 minutes and writes LLM-generated `recommendations` per DAG to the database.

No external alerting or on-call integration exists. Fill in manually if required.

## Logs

The poller writes structured log lines to stdout using Python's `logging` module with the format:
```
%(asctime)s %(levelname)s %(message)s
```

Example lines:
```
2026-04-26 12:00:00,123 INFO Poller starting — Airflow v1 at http://localhost:8080, interval=30s
2026-04-26 12:00:05,456 INFO New failure: DAG=customer_data_pipeline run=manual__2026-04-26T12:00:00+00:00
2026-04-26 12:00:30,789 INFO Investigation for customer_data_pipeline/manual__... completed (no action required).
```

**Log levels in use:** `INFO` for normal operation, `WARNING` for rate-limit switches and exhausted retries, `ERROR` for API failures and investigation errors. `httpx` and `httpcore` are silenced to `WARNING`.

**To redirect logs to a file:**
```bash
uv run python poller.py 2>&1 | tee poller.log
```

**Key fields to filter on:**
- `New failure:` — a new failed run has been detected
- `Investigation for ... completed` — successful investigation
- `Investigation failed for` — investigation raised an exception (will retry next cycle)
- `exhausted retries` — a run hit `MAX_INVESTIGATION_RETRIES=3` and is now `failed_permanent`
- `rate limited` — LLM rate limit encountered; watch for frequent occurrences
- `Proactive analysis complete.` — hourly proactive cycle completed
- `Metrics snapshot saved` — 15-minute metrics collection completed

## Common Incidents

### Symptom: Investigation status stuck at `in_progress`

**Likely cause:** The poller process crashed or was killed while a LangGraph investigation was mid-flight.

**Diagnosis steps:**
1. Check if the poller process is running: `ps aux | grep poller.py`
2. Query the database: `sqlite3 .airflow_agent.db "SELECT run_id, dag_id, timestamp FROM investigations WHERE status='in_progress';"`

**Resolution:** Restart the poller. On startup, `resume_incomplete()` automatically re-investigates all `in_progress` and `pending_retry` records.

---

### Symptom: Approvals page has no Approve/Reject buttons for an investigation

**Likely cause:** The investigation's `recommended_action` was not `retry`, so no approval record was created. Only `retry` actions require approval; others (`fix_dag_code`, `fix_infrastructure`, `escalate`, `monitor`) complete without interruption.

**Diagnosis steps:**
1. Check the investigation's `recommended_action` in the Incidents page or via: `sqlite3 .airflow_agent.db "SELECT run_id, recommended_action, status FROM investigations WHERE run_id='<run_id>';"`

**Resolution:** No action required — the investigation completed normally.

---

### Symptom: High LLM rate-limit errors in logs

**Likely cause:** The primary LLM provider's rate limit is being hit, especially during bursts of new failures.

**Diagnosis steps:**
1. Look for `rate limited` log lines. Note the wait time extracted.
2. Check if `LLM_FALLBACK_MODELS` is configured in `.env`.

**Resolution:**
- Configure `LLM_FALLBACK_MODELS` with a local Ollama model as a fallback: `LLM_FALLBACK_MODELS=ollama/qwen2.5:7b`
- Lower `LLM_FALLBACK_THRESHOLD_S` (default 60s) to switch to the fallback model faster.
- Increase `POLL_INTERVAL` to reduce investigation frequency.

---

### Symptom: Investigation produces empty diagnosis (`diagnosis=''`)

**Likely cause:** The LLM returned an empty response (rate limit mid-stream) or produced output that neither the JSON parser nor the regex fallback could parse.

**Diagnosis steps:**
1. Check the investigation's `last_error` field: `sqlite3 .airflow_agent.db "SELECT run_id, last_error, retry_count FROM investigations WHERE diagnosis='';"`
2. If `retry_count < 3`, the investigation is in `pending_retry` status and will be re-attempted automatically.

**Resolution:** If `retry_count >= 3` and status is `failed_permanent`, delete the `.seen_runs.db` entry to re-investigate: `sqlite3 .seen_runs.db "DELETE FROM seen_runs WHERE run_id='<run_id>';"`

---

### Symptom: DAG import error not detected

**Likely cause:** The synthetic ID for the import error (`import_error__<filename>`) is already in `.seen_runs.db`.

**Diagnosis steps:**
1. Check: `sqlite3 .seen_runs.db "SELECT * FROM seen_runs WHERE run_id LIKE 'import_error__%';"`

**Resolution:** Delete the entry from `seen_runs` or run `rm .seen_runs.db` (this resets all seen-run state).

---

### Symptom: Airflow API calls returning 401 Unauthorized

**Likely cause:** Credentials in `.env` are wrong, or `AIRFLOW_API_TOKEN` has expired.

**Diagnosis steps:**
```bash
curl -u airflow:airflow http://localhost:8080/api/v1/dags
```

**Resolution:** Update `AIRFLOW_USER` / `AIRFLOW_PASSWORD` or `AIRFLOW_API_TOKEN` in `.env` and restart the poller.

---

### Symptom: `execute_action` not retrying after approval

**Likely cause:** The LangGraph checkpoint for the run ID is no longer in `.langgraph.db`, or the graph state was deleted.

**Diagnosis steps:**
1. Check the approval record: `sqlite3 .airflow_agent.db "SELECT * FROM pending_approvals WHERE run_id='<run_id>';"`
2. Check if `resolved_at IS NOT NULL` (indicates the poller already tried to process it).

**Resolution:** If the checkpoint is gone, manually trigger the Airflow task retry via the Airflow UI.

---

### Symptom: ChromaDB errors on startup (`Could not connect` or dimension mismatch)

**Likely cause:** The ChromaDB collection was created with a different embedding model than the one currently configured, or the `.chroma/` directory is corrupt.

**Resolution:** Delete the ChromaDB directory and let it regenerate (existing vector memory will be lost):
```bash
rm -rf .chroma
```

## Scheduled Jobs

The poller is a single continuously running process with internal timers — there are no cron jobs.

| Task | Interval | Implementation | Manual trigger |
|------|----------|---------------|----------------|
| Poll for new failed DAG runs | Every `POLL_INTERVAL` seconds (default 30s) | `run_investigations()` called in main loop | n/a (continuous loop) |
| Process human approval decisions | Every `POLL_INTERVAL` seconds | `process_approvals()` called in main loop | n/a |
| Check retry outcomes | Every `POLL_INTERVAL` seconds | `check_retry_outcomes()` called in main loop | n/a |
| Collect Airflow metrics snapshot | Every 900 seconds (15 min) | `collect_metrics()` in main loop | `python -c "from agent.metrics import collect_metrics; collect_metrics()"` |
| Proactive trend analysis | Every 3600 seconds (60 min) | `_run_proactive()` in main loop | `python -c "from agent.proactive.graph import proactive_graph; ..."` |
| DB cleanup (old records) | Every 86400 seconds (24 h) | `db.cleanup_old_records()` | `python -c "import db.store; print(db.store.cleanup_old_records(90))"` |

**Retry timeout:** Investigations in `retry_in_progress` status that have not resolved within 24 hours are automatically moved to `stale` status by `check_retry_outcomes()`.

## Maintenance Tasks

**DB cleanup** (runs automatically once per day; can also be triggered manually):
```bash
python3 -c "import db.store; print(db.store.cleanup_old_records(90))"
```
Deletes records older than `DB_RETENTION_DAYS` (default 90) from `investigations`, `run_history`, `anomalies`, `airflow_snapshots`, and `dag_stats`. The `pending_approvals` table is intentionally excluded.

**Reset all state** (use to start fresh in development):
```bash
make clean
```

**Manually reinvestigate a specific run:**
```bash
sqlite3 .seen_runs.db "DELETE FROM seen_runs WHERE run_id='<run_id>';"
# Then restart or wait for the next poll cycle
```

**Inspect investigation history:**
```bash
sqlite3 .airflow_agent.db "SELECT dag_id, run_id, status, recommended_action, timestamp FROM investigations ORDER BY timestamp DESC LIMIT 20;"
```

**Remove Airflow Docker volumes** (destroys Postgres data in local stacks):
```bash
docker compose -f docker/airflow2/docker-compose.yml down -v
docker compose -f docker/airflow3/docker-compose.yml down -v
```

**Dependency audit:**
```bash
uv sync --upgrade   # upgrade all dependencies to latest compatible versions
uv run pytest tests/unit/ -v   # verify nothing broke
```

## Known Issues & Tech Debt

No `TODO`, `FIXME`, `HACK`, or `DEPRECATED` comments were found in the source files.

Architectural gaps noted from code review:

| Severity | Area | Description |
|----------|------|-------------|
| Medium | CI/CD | No CI pipeline exists. Tests must be run manually before merging. |
| Medium | Security | Credentials are stored in plaintext `.env` file. No secrets manager integration. |
| Medium | Observability | No external metrics export (Prometheus, Datadog, etc.). No alerting integration. |
| Low | Deployment | No Docker image for the agent itself. Production deployment process is undocumented. |
| Low | `db/store.py:200` | `count_investigations_since` uses `datetime.utcnow()` (deprecated in Python 3.12+) instead of `datetime.now(timezone.utc)`. |
| Low | `db/store.py:206` | `most_failing_dag` uses `datetime.utcnow()` (deprecated) instead of `datetime.now(timezone.utc)`. |
| Low | Airflow client | `list_failed_dag_runs` has an identical `params` block for both v1 and v2 — the comment suggests they should differ but they currently don't. |
| Low | LangGraph checkpoint | No TTL or cleanup for `.langgraph.db` checkpoints. Long-running deployments will accumulate stale thread state indefinitely. |

## Contacts & Escalation

| Role | Name | Contact |
|------|------|---------|
| Primary on-call | Fill in manually | — |
| Secondary on-call | Fill in manually | — |
| LLM provider support | Fill in manually (Groq/OpenAI/Anthropic) | — |
| Airflow infrastructure owner | Fill in manually | — |
