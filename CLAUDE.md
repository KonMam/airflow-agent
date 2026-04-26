# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
uv sync                        # install dependencies
uv run python poller.py        # start the polling agent
uv run pytest                  # run tests
uv run pytest tests/test_x.py  # run a single test file
uv run ruff check .            # lint
uv run ruff format .           # format

# Airflow environments (run from project root)
docker compose -f docker/airflow2/docker-compose.yml up -d   # Airflow 2 on :8080
docker compose -f docker/airflow3/docker-compose.yml up -d   # Airflow 3 on :8081
docker compose -f docker/airflow2/docker-compose.yml down
docker compose -f docker/airflow3/docker-compose.yml down
```

Reset seen-runs state: `rm .seen_runs.db`

## Architecture

The system is a polling agent loop: `poller.py` detects new failed Airflow DAG runs and feeds each one into a LangGraph investigation graph.

### Agent graph flow (`agent/graph.py`)
```
recall_memory → investigate → diagnose → report → save_to_memory
```
- **recall_memory**: queries ChromaDB for similar past investigations to inject as context
- **investigate**: ReAct loop — LiteLLM calls tools iteratively until it produces a structured diagnosis
- **diagnose**: parses structured fields (DIAGNOSIS, FAILING_TASK, ERROR_SUMMARY, RECOMMENDED_ACTION) from the LLM output via regex
- **report**: formats and prints the incident report
- **save_to_memory**: embeds and stores the investigation in ChromaDB for future recall

### LLM provider abstraction
All LLM calls go through `litellm.completion()` in `agent/nodes.py:_llm_call()`. The model is set via `LLM_MODEL` env var using LiteLLM's provider-prefix format (e.g. `groq/llama-3.3-70b-versatile`, `openai/gpt-4o`, `ollama/qwen2.5:7b`). The `_llm_call` helper handles rate limit backoff and Groq's occasional malformed tool call errors.

### Airflow API abstraction (`agent/tools/airflow_client.py`)
Airflow 2 uses `/api/v1`, Airflow 3 uses `/api/v2`. All endpoints are built using `config.api_prefix`. Key differences handled:
- `execution_date` (v1) vs `logical_date` (v2) — use `run_date_field(run)` helper
- DAG details endpoint: `/dags/{id}` (v1) vs `/dags/{id}/details` (v2)
- Cross-DAG failed run listing: POST `~/dagRuns/list` (v1) vs GET `/dagRuns?state=failed` (v2)

### Memory (`agent/memory/`)
ChromaDB persisted at `.chroma/`. Each `InvestigationRecord` is embedded using either an API embedding model (`EMBED_MODEL` env var) or local `sentence-transformers/all-MiniLM-L6-v2` as fallback. Embeddings are keyed on `dag_id + task_id + error_summary + diagnosis`. Run ID is used as the document ID to prevent duplicates.

### State (`agent/state.py`)
`AgentState` is a `TypedDict` threaded through all LangGraph nodes. Fields are populated progressively: `recall_memory` fills `similar_past_investigations`, `investigate` fills `investigation_notes`, `diagnose` parses structured fields from notes.

### Poller (`poller.py`)
Tracks already-investigated run IDs in `.seen_runs.db` (SQLite). Calls `list_all_failed_dag_runs()` each interval and invokes the graph for any unseen failures. A 5-second gap is inserted between back-to-back investigations to avoid rate limit bursts.

### Config (`config.py`)
All settings loaded from `.env` via `python-dotenv`. Copy `.env.example` → `.env` to get started. `AIRFLOW_API_VERSION=v1` for Airflow 2, `v2` for Airflow 3.

## Test DAGs (`dags/`)
Five DAGs for local testing — all have `schedule=None` and must be triggered manually:
- `failing_task` — always raises ValueError
- `upstream_failure` — extract→transform→load chain where extract always fails
- `flaky_task` — 50% random failure
- `sla_miss` — sleeps 10s with a 2s SLA
- `import_error` — imports a non-existent package to break DAG parsing
