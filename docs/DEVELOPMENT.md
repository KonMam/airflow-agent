# Development Guide

## Prerequisites

| Tool | Minimum Version | Notes |
|------|----------------|-------|
| Python | 3.11 | Required by `pyproject.toml` |
| uv | any current | Dependency and venv manager |
| Docker + Docker Compose | v2 (Compose V2 plugin) | Required for Airflow containers and integration tests |
| Ollama | any | Optional; only if using a local LLM (`ollama/...`) |

An LLM API key is required unless you run Ollama locally. Supported providers include Groq, OpenAI, and Anthropic (via LiteLLM).

## First-Time Setup

1. Clone the repository and enter the project directory:
   ```bash
   git clone <repo-url>
   cd airflow-agent
   ```

2. Install Python dependencies into a managed virtual environment:
   ```bash
   uv sync
   ```

3. Create your local environment file:
   ```bash
   cp .env.example .env
   ```

4. Edit `.env` and set at minimum:
   ```
   LLM_MODEL=groq/llama-3.3-70b-versatile   # or openai/gpt-4o, anthropic/claude-sonnet-4-6
   LLM_API_KEY=<your-api-key>
   ```

5. (Optional) Install and configure Ollama for a local fallback model:
   ```bash
   brew install ollama
   ollama pull qwen2.5:7b
   ollama serve   # runs on http://localhost:11434
   ```
   Then add to `.env`:
   ```
   LLM_FALLBACK_MODELS=ollama/qwen2.5:7b
   ```

6. Start an Airflow instance (choose one):
   ```bash
   make up2   # Airflow 2 on http://localhost:8080  (AIRFLOW_API_VERSION=v1)
   make up3   # Airflow 3 on http://localhost:8081  (AIRFLOW_API_VERSION=v2)
   ```
   Default Airflow credentials: `airflow` / `airflow`.

7. Verify the Airflow UI is reachable, then start the agent:
   ```bash
   make poller
   ```

8. (Optional) Start the dashboard in a second terminal:
   ```bash
   make ui    # opens http://localhost:8501
   ```

## Environment Variables

All variables are loaded from `.env` via `python-dotenv` in `config.py`.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `LLM_MODEL` | Yes | `openai/gpt-4o` | Primary LLM in LiteLLM provider-prefix format (e.g. `groq/llama-3.3-70b-versatile`, `anthropic/claude-sonnet-4-6`, `ollama/qwen2.5:7b`) |
| `LLM_API_KEY` | Yes (unless Ollama) | `""` | API key forwarded to LiteLLM |
| `LLM_MAX_TOKENS` | No | `1024` | Max output tokens per LLM call |
| `LLM_FALLBACK_MODELS` | No | `""` | Comma-separated fallback model list tried in order when primary is rate-limited |
| `LLM_FALLBACK_THRESHOLD_S` | No | `60` | Rate-limit wait seconds above which the agent switches to a fallback model instead of retrying |
| `EMBED_MODEL` | No | `""` | OpenAI-compatible embedding model name. Empty = use local `sentence-transformers/all-MiniLM-L6-v2` |
| `AIRFLOW_URL` | No | `http://localhost:8080` | Base URL of the Airflow webserver |
| `AIRFLOW_API_VERSION` | No | `v1` | `v1` for Airflow 2, `v2` for Airflow 3 |
| `AIRFLOW_USER` | No | `airflow` | Airflow basic-auth username |
| `AIRFLOW_PASSWORD` | No | `airflow` | Airflow basic-auth password |
| `AIRFLOW_API_TOKEN` | No | `""` | Bearer token; takes precedence over `AIRFLOW_USER`/`AIRFLOW_PASSWORD` |
| `POLL_INTERVAL` | No | `30` | Seconds between polling cycles |
| `POLL_DAG_IDS` | No | `""` | Comma-separated DAG IDs to restrict polling. Empty = all DAGs |
| `CHROMA_PATH` | No | `.chroma` | Directory for ChromaDB persistent storage |
| `DB_PATH` | No | `.airflow_agent.db` | Path to the main SQLite database |
| `LANGGRAPH_DB_PATH` | No | `.langgraph.db` | Path to the LangGraph checkpoint SQLite database |
| `DB_RETENTION_DAYS` | No | `90` | Records older than this are purged by the daily cleanup task |

## Running the Application

Start the polling agent (detects failures, runs investigations, processes approvals):
```bash
make poller
# equivalent to: uv run python poller.py
```

Start the Streamlit dashboard:
```bash
make ui
# equivalent to: uv run streamlit run ui/app.py
# opens http://localhost:8501
```

Both processes are independent; run them in separate terminals.

## Running Tests

Unit tests only (no Docker required, LLM and Airflow API are mocked):
```bash
make test
# equivalent to: uv run pytest tests/unit/ -v
```

All tests including integration (requires Docker):
```bash
make test-all
# equivalent to: uv run pytest tests/ -v
```

Integration tests only:
```bash
make test-int
# equivalent to: uv run pytest tests/integration/ -m integration -v
```

Run a single test file:
```bash
uv run pytest tests/unit/test_nodes.py -v
```

Run a single test by name:
```bash
uv run pytest tests/unit/test_nodes.py::test_diagnose_parses_all_fields -v
```

Integration tests (`tests/integration/test_airflow2.py`, `tests/integration/test_airflow3.py`) use session-scoped fixtures in `tests/conftest.py` that:
1. `docker compose up -d` the relevant Airflow stack
2. Wait up to 120 seconds for the `/health` endpoint to return `healthy`
3. Trigger test DAGs and wait for them to reach `failed` state
4. Tear down the stack (`docker compose down -v`) after the session

## Common Development Tasks

**Adding a new Airflow API call:**
1. Add a function to `agent/tools/airflow_client.py` using `_get()` or `_post()`.
2. If the call differs between v1 and v2, branch on `config.airflow_api_version`.
3. Optionally wrap it as a LangChain `@tool` in `agent/tools/airflow_tools.py` with a Pydantic `BaseModel` for input validation.
4. Add the tool to the `ALL_TOOLS` list at the bottom of `airflow_tools.py` to expose it to the LLM.

**Adding a new LangGraph node:**
1. Write the node function in `agent/nodes.py` with signature `(state: AgentState) -> dict`.
2. Register it in `agent/graph.py` with `g.add_node(...)` and wire edges.
3. Add any new state fields to `AgentState` in `agent/state.py`.

**Adding a new UI page:**
1. Create `ui/pages/NN_name.py` (Streamlit uses filename ordering for sidebar navigation).
2. The file must call `sys.path.insert(0, ...)` to reach the project root, as shown in existing pages.

**Updating dependencies:**
```bash
uv add <package>        # add a new runtime dependency
uv add --dev <package>  # add a dev dependency
uv sync                 # re-lock and install
```

**Reset all persisted state** (investigations, approvals, LangGraph checkpoints, vector memory, seen-run tracking):
```bash
make clean
# equivalent to:
# rm -f .airflow_agent.db .airflow_agent.db-wal .airflow_agent.db-shm .langgraph.db .langgraph.db-wal .langgraph.db-shm .seen_runs.db
# rm -rf .chroma
```

Reset only the seen-runs tracker (forces re-investigation of all existing failures):
```bash
rm .seen_runs.db
```

**Generating test failures** (for local development):
Trigger any test DAG manually from the Airflow UI. All DAGs in `dags/` have `schedule=None` and must be triggered manually. Example failing DAGs:
- `customer_data_pipeline` — fails when `CUSTOMER_DATA_PATH` env var points to a missing file
- `flaky_task` — fails ~50% of the time
- `import_error` — always produces a DAG import error (never creates a run)

## Debugging

**Enable verbose LangGraph tracing:** Set `LANGCHAIN_TRACING_V2=true` and `LANGCHAIN_API_KEY=<key>` to send traces to LangSmith.

**Inspect the SQLite database manually:**
```bash
sqlite3 .airflow_agent.db
sqlite> .schema
sqlite> SELECT run_id, dag_id, status, retry_count FROM investigations ORDER BY timestamp DESC LIMIT 10;
```

**Inspect ChromaDB contents:**
```python
import chromadb
c = chromadb.PersistentClient(path=".chroma")
col = c.get_collection("investigations")
print(col.count())
print(col.get(limit=5))
```

**Common failures:**

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `LLM returned empty response` | LiteLLM rate limited mid-investigation | Configure `LLM_FALLBACK_MODELS` with Ollama or a secondary cloud model |
| `tool_use_failed` in logs | Groq occasionally returns malformed tool call JSON | Automatic retry (up to 3 attempts) handles this; no action needed |
| Investigation status stuck at `in_progress` | Agent crashed mid-investigation | `resume_incomplete()` re-investigates these on next poller startup |
| ChromaDB `cannot connect to container` | `.chroma/` directory permissions | `rm -rf .chroma` and let it regenerate |
| Import error DAG not investigated | The `.seen_runs.db` has the synthetic ID marked seen | Delete `.seen_runs.db` to force re-check |

## Code Style & Conventions

Linting and formatting are handled by `ruff`:
```bash
make lint   # uv run ruff check .
make fmt    # uv run ruff format .
```

Key conventions observed in the codebase:
- Node functions return a plain `dict` of the state fields they modify; LangGraph merges these into `AgentState`.
- All Airflow HTTP calls go through `agent/tools/airflow_client.py` — no `httpx` calls in nodes directly (exception: `report` node fetches task instances for backfilling `primary_failing_task`).
- Tool errors are caught inside the investigate loop and fed back as string messages to the LLM (self-correction pattern) — do not raise from tool functions.
- `config.py` uses a class with class-level attributes (not `@dataclass` or `pydantic`) so that `monkeypatch.setattr("config.config.xxx", ...)` works cleanly in tests.
- Logging: standard `logging` module throughout. LiteLLM and httpx/httpcore loggers are set to `WARNING` to suppress their verbose output.
