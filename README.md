# Airflow Agent

Autonomous monitoring agent for Apache Airflow. Detects failed DAG runs, investigates them using an LLM-powered ReAct loop, and surfaces results in a Streamlit dashboard with human-in-the-loop approval for automated remediation actions.

## Setup

```bash
uv sync
cp .env.example .env
# Edit .env — set LLM_MODEL and LLM_API_KEY at minimum
```

LLM model is configured via `LLM_MODEL` using LiteLLM's provider-prefix format:

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
LLM_FALLBACK_MODELS=groq/llama-3.1-8b-instant,ollama/qwen2.5:7b
```

The agent will fall back to the local model automatically when cloud models are rate-limited or unavailable.

## Spin up Airflow

```bash
make up2    # Airflow 2 on http://localhost:8080
make up3    # Airflow 3 on http://localhost:8081
```

Default credentials: `airflow` / `airflow`. Set `AIRFLOW_API_VERSION=v1` for Airflow 2, `v2` for Airflow 3.

The `dags/` directory is mounted into both containers. Trigger any DAG manually from the Airflow UI to generate runs for the agent to investigate.

## Run the agent

Start the poller (detects failures and runs investigations):

```bash
make poller
```

Start the UI (dashboard, incidents, approvals, DAG health):

```bash
make ui     # opens http://localhost:8501
```

## Human-in-the-loop approvals

When the agent recommends a `retry` action, the run appears on the **Approvals** page in the UI. Click Approve or Reject — the poller picks up the decision and either executes the retry or skips it.

## Tests

```bash
make test        # unit tests only (no Docker required)
make test-all    # unit + integration
make test-int    # integration tests only (requires Docker)
```

Run a single test file:

```bash
uv run pytest tests/unit/test_nodes.py -v
```

## Clean up test data

Remove all persisted state (investigations, approvals, LangGraph checkpoints, vector memory, seen-run tracking):

```bash
rm -f .airflow_agent.db .airflow_agent.db-wal .airflow_agent.db-shm \
       .langgraph.db .langgraph.db-wal .langgraph.db-shm \
       .seen_runs.db && rm -rf .chroma
```

## Tear down Airflow

```bash
make down2
make down3
```

To remove volumes (Postgres data) as well:

```bash
docker compose -f docker/airflow2/docker-compose.yml down -v
docker compose -f docker/airflow3/docker-compose.yml down -v
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — system design, component map, data model, data flow, key design decisions
- [Development Guide](docs/DEVELOPMENT.md) — prerequisites, first-time setup, all environment variables, common tasks, debugging
- [Deployment](docs/DEPLOYMENT.md) — infrastructure, configuration management, rollback, database migrations
- [Runbook](docs/RUNBOOK.md) — health checks, common incidents, scheduled jobs, maintenance tasks, known issues
