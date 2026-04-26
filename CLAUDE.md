# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
uv sync                        # install dependencies
uv run python poller.py        # start the polling agent (or: make poller)
uv run streamlit run ui/app.py # start the dashboard (or: make ui)
uv run pytest tests/unit/ -v   # unit tests only, no Docker (or: make test)
uv run pytest tests/ -v        # all tests incl. integration (or: make test-all)
uv run ruff check .            # lint (or: make lint)
uv run ruff format .           # format (or: make fmt)
uv run mypy agent/ db/ config.py poller.py  # type-check (matches CI)

# Airflow environments (run from project root)
make up2    # Airflow 2 on :8080  (AIRFLOW_API_VERSION=v1)
make up3    # Airflow 3 on :8081  (AIRFLOW_API_VERSION=v2)
make down2
make down3

make clean  # remove all persisted state (.airflow_agent.db, .langgraph.db, .seen_runs.db, .chroma/)
```

Reset only seen-runs state: `rm .seen_runs.db`

## Architecture

The system is a polling agent loop: `poller.py` detects new failed Airflow DAG runs and import errors, feeds each one into a LangGraph investigation graph, and runs periodic proactive monitoring.

### Agent graph flow (`agent/graph.py`)
```
recall_memory → investigate → diagnose → report → save_to_memory
                                                        ↓ (if recommended_action == "retry")
                                                  execute_action  ← interrupt_before (human approval)
```
- **recall_memory**: queries ChromaDB for top-3 similar past investigations to inject as context
- **investigate**: ReAct loop — LiteLLM calls tools iteratively until it produces a structured JSON diagnosis
- **diagnose**: parses JSON (with regex fallback) into structured `AgentState` fields
- **report**: formats the incident report and persists the investigation record to SQLite via `db.store`
- **save_to_memory**: upserts the `InvestigationRecord` into ChromaDB; routes to `execute_action` if `recommended_action == "retry"`, otherwise exits
- **execute_action**: LangGraph interrupts *before* this node — the graph suspends until the poller receives an `approved`/`rejected` decision from the `pending_approvals` SQLite table, then clears and retries the Airflow task

### Proactive monitoring (`agent/proactive/`)
A separate sub-graph run by the poller every 60 minutes:
```
collect_dag_metrics → analyze_trends → generate_recommendations
```
Reads recent failure counts and run durations from SQLite, sends them to the LLM for trend analysis, and upserts per-DAG recommendations back to SQLite.

### Poller (`poller.py`)
Each poll cycle calls:
- `run_investigations` — lists failed runs + import errors, invokes the graph for unseen IDs
- `process_approvals` — resumes interrupted graphs when approvals are resolved in the UI
- `check_retry_outcomes` — marks retry-in-progress runs as resolved or stale
- `collect_metrics` (every 15 min) — snapshots Airflow health and per-DAG stats into SQLite
- `proactive_graph` (every 60 min) — runs the proactive sub-graph

Import errors are converted to synthetic run IDs (`import_error__<filename>`) so they flow through the same investigation graph as real failures.

Tracks already-investigated run IDs in `.seen_runs.db` (SQLite). Inserts a 5-second gap between back-to-back investigations to avoid rate limit bursts. On startup, `resume_incomplete()` re-investigates any `in_progress` runs left over from a crashed session.

### LLM provider abstraction
All LLM calls go through `_llm_call()` in `agent/nodes.py`. The model is set via `LLM_MODEL` using LiteLLM's provider-prefix format (e.g. `groq/llama-3.3-70b-versatile`, `openai/gpt-4o`, `ollama/qwen2.5:7b`). Rate-limit handling and automatic fallback (`LLM_FALLBACK_MODELS`) are centralised here. If rate-limit wait exceeds `LLM_FALLBACK_THRESHOLD_S`, the agent switches to the next fallback model rather than waiting.

### Airflow API abstraction (`agent/tools/airflow_client.py`)
All endpoints are built using `config.api_prefix` (`/api/v1` or `/api/v2`). Key differences handled:
- `execution_date` (v1) vs `logical_date` (v2) — use `run_date_field(run)` helper
- DAG details endpoint: `/dags/{id}` (v1) vs `/dags/{id}/details` (v2)
- Cross-DAG failed run listing: POST `~/dagRuns/list` (v1) vs GET `/dagRuns?state=failed` (v2)

### State (`agent/state.py`)
`AgentState` is a `TypedDict` threaded through all LangGraph nodes. Key fields:
- `triggered_by` — `"poller"` | `"manual"` | `"proactive"`
- `similar_past_investigations` — filled by `recall_memory`
- `investigation_notes`, `task_instances` — filled by `investigate`
- `diagnosis`, `recommended_action`, `error_summary`, `primary_failing_task` — filled by `diagnose`
- `action_decision` — set by the poller on graph resume (`"approved"` | `"rejected"`)
- `llm_model_used` — model that produced the final diagnosis

### Database (`db/store.py`)
Central SQLite store at `.airflow_agent.db` (WAL mode for concurrent poller + UI access). Tables: `investigations`, `pending_approvals`, `run_history`, `anomalies`, `recommendations`, `airflow_snapshots`, `dag_stats`. LangGraph checkpoints are in a separate `.langgraph.db`.

### Memory (`agent/memory/`)
ChromaDB persisted at `.chroma/`. Each `InvestigationRecord` is embedded using either an API embedding model (`EMBED_MODEL` env var) or local `sentence-transformers/all-MiniLM-L6-v2` as fallback. Embeddings keyed on `dag_id + task_id + error_summary + diagnosis`. Run ID is the document ID to prevent duplicates.

### Config (`config.py`)
All settings loaded from `.env` via `python-dotenv`. Copy `.env.example` → `.env` to get started. Uses a plain class (not dataclass/pydantic) so `monkeypatch.setattr("config.config.xxx", ...)` works cleanly in tests.

## Test DAGs (`dags/`)
Eight DAGs for local testing — all have `schedule=None` and must be triggered manually from the Airflow UI:
- `customer_data_pipeline` — fails when `CUSTOMER_DATA_PATH` env var points to a missing file
- `api_data_sync` — simulates an API sync pipeline
- `ml_feature_pipeline` — simulates an ML feature engineering pipeline
- `nightly_aggregation` — simulates a nightly aggregation job
- `data_quality_monitor` — simulates a data quality check
- `flaky_task` — fails ~50% of the time
- `sla_miss` — sleeps 10s with a 2s SLA
- `import_error` — imports a non-existent package to break DAG parsing (never creates a run)

## Tests
Unit tests mock the LLM and Airflow API (`tests/unit/`). Integration tests spin up real Docker Airflow containers (`tests/integration/`); session fixtures in `tests/conftest.py` handle `docker compose up`, health-check polling, DAG triggering, and teardown. Integration tests are excluded from CI (require Docker with running Airflow).
