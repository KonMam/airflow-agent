# Airflow Agent

Autonomous monitoring agent for Apache Airflow. Detects failed DAG runs and import errors, investigates them using an LLM-powered ReAct loop, and surfaces diagnoses and recommended actions through a Streamlit dashboard. Human operators approve or reject automated retries before the agent acts.

**Features**

- Reactive investigations — polls for failed runs and DAG import errors, runs an LLM ReAct loop per failure, produces structured diagnoses
- Human-in-the-loop approvals — retry actions require explicit approval in the UI before execution
- Associative memory — past investigations are embedded in ChromaDB; similar past incidents are injected as context on each new failure
- Proactive monitoring — periodic trend analysis generates per-DAG recommendations (runs every 60 min)
- Anomaly detection — flags task duration outliers using an IQR-P90 heuristic (requires ≥5 historical data points)
- Airflow 2 and 3 support — REST API v1 and v2 handled transparently via a single `api_prefix` config

## Prerequisites

| Tool | Minimum Version | Notes |
|------|----------------|-------|
| Python | 3.11 | Required by `pyproject.toml` |
| uv | any current | Dependency and venv manager |
| Docker + Docker Compose | v2 (Compose V2 plugin) | Required for Airflow containers and integration tests |
| Ollama | any | Optional; only if using a local LLM |

An LLM API key is required unless running Ollama locally. Supported providers: Groq, OpenAI, Anthropic (and anything LiteLLM supports).

## Setup

```bash
uv sync
cp .env.example .env
# Edit .env — set LLM_MODEL and LLM_API_KEY at minimum
```

Set `LLM_MODEL` using LiteLLM's provider-prefix format:

```
LLM_MODEL=groq/llama-3.3-70b-versatile
LLM_MODEL=openai/gpt-4o
LLM_MODEL=anthropic/claude-sonnet-4-6
LLM_MODEL=ollama/qwen2.5:7b
```

### Local fallback with Ollama (recommended for offline / rate-limit resilience)

```bash
brew install ollama
ollama pull qwen2.5:7b   # ~4.7GB, good tool-calling support, fast on Apple Silicon
ollama serve             # runs on http://localhost:11434
```

Then in `.env`:
```
LLM_FALLBACK_MODELS=ollama/qwen2.5:7b
```

The agent falls back to the local model automatically when the primary is rate-limited or unavailable.

## Environment Variables

All variables are loaded from `.env` via `python-dotenv`. See [Development Guide](docs/DEVELOPMENT.md) for the full table.

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_MODEL` | `openai/gpt-4o` | Primary LLM in LiteLLM provider-prefix format |
| `LLM_API_KEY` | `""` | API key forwarded to LiteLLM |
| `LLM_FALLBACK_MODELS` | `""` | Comma-separated fallback models tried when primary is rate-limited |
| `AIRFLOW_URL` | `http://localhost:8080` | Airflow webserver base URL |
| `AIRFLOW_API_VERSION` | `v1` | `v1` for Airflow 2, `v2` for Airflow 3 |
| `AIRFLOW_USER` | `airflow` | Basic-auth username |
| `AIRFLOW_PASSWORD` | `airflow` | Basic-auth password |
| `POLL_INTERVAL` | `30` | Seconds between polling cycles |
| `POLL_DAG_IDS` | `""` | Comma-separated DAG IDs to restrict polling; empty = all |
| `DB_PATH` | `.airflow_agent.db` | Main SQLite database path |
| `DB_RETENTION_DAYS` | `90` | Days before old records are purged |

## How It Works

```
recall_memory → investigate → diagnose → report → save_to_memory
                                                        ↓ (if retry)
                                                  execute_action  ← human approval
```

1. The poller calls Airflow's REST API each cycle, collecting failed runs and import errors.
2. New run IDs (not in `.seen_runs.db`) are dispatched to the LangGraph investigation graph.
3. `recall_memory` fetches the top-3 most similar past incidents from ChromaDB.
4. `investigate` runs a ReAct loop — the LLM calls tools (`get_task_log`, `list_task_instances`, `get_dag_structure`, …) until it emits a structured JSON diagnosis.
5. `diagnose` parses the JSON into structured fields; `report` persists the record to SQLite.
6. `save_to_memory` upserts the record into ChromaDB. If `recommended_action == "retry"`, the graph **interrupts** at `execute_action` — the poller waits for human approval before clearing and retrying the task.

## Spin Up Airflow

```bash
make up2    # Airflow 2 on http://localhost:8080  (set AIRFLOW_API_VERSION=v1)
make up3    # Airflow 3 on http://localhost:8081  (set AIRFLOW_API_VERSION=v2)
```

Default credentials: `airflow` / `airflow`. The `dags/` directory is mounted into both containers. Trigger any DAG manually from the Airflow UI to generate runs for the agent to investigate.

## Run the Agent

Start the poller (detects failures, runs investigations, processes approvals):

```bash
make poller
```

Start the dashboard (incidents, approvals, DAG health, metrics):

```bash
make ui     # opens http://localhost:8501
```

Both processes are independent; run them in separate terminals.

## Human-in-the-Loop Approvals

When the agent recommends a `retry`, the run appears on the **Approvals** page. Click Approve or Reject — the poller picks up the decision and either executes the Airflow task retry or skips it.

## Tests

```bash
make test        # unit tests only (no Docker required)
make test-all    # unit + integration (requires Docker)
make test-int    # integration tests only
```

Run a single file or test:

```bash
uv run pytest tests/unit/test_nodes.py -v
uv run pytest tests/unit/test_nodes.py::test_diagnose_parses_all_fields -v
```

## Clean Up

Remove all persisted state (investigations, approvals, LangGraph checkpoints, vector memory, seen-run tracking):

```bash
make clean
```

Reset only the seen-runs tracker (forces re-investigation of all existing failures):

```bash
rm .seen_runs.db
```

## Tear Down Airflow

```bash
make down2
make down3
```

Remove volumes (Postgres data) as well:

```bash
docker compose -f docker/airflow2/docker-compose.yml down -v
docker compose -f docker/airflow3/docker-compose.yml down -v
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — system design, component map, data model, data flow, key design decisions
- [Development Guide](docs/DEVELOPMENT.md) — prerequisites, first-time setup, all environment variables, common tasks, debugging
- [Deployment](docs/DEPLOYMENT.md) — infrastructure, configuration management, rollback, database migrations
- [Runbook](docs/RUNBOOK.md) — health checks, common incidents, scheduled jobs, maintenance tasks, known issues
