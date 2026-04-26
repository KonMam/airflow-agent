# Deployment

## Environments

| Environment | URL / Cluster | Deployed From | Notes |
|-------------|--------------|---------------|-------|
| Local development | `http://localhost:8501` (UI), `http://localhost:8080` (Airflow 2), `http://localhost:8081` (Airflow 3) | Manual (`make poller`, `make ui`) | Docker Compose Airflow stacks |
| Production | Not found in codebase — fill in manually | Not found in codebase — fill in manually | No CI/CD pipeline exists |

## CI/CD Pipeline

No CI/CD configuration was found in the repository (no `.github/workflows/`, `.gitlab-ci.yml`, `Jenkinsfile`, or `.circleci/` directory). All testing and deployment is currently manual.

Recommended pipeline stages to add:

1. **Lint** — `uv run ruff check .`
2. **Format check** — `uv run ruff format --check .`
3. **Unit tests** — `uv run pytest tests/unit/ -v`
4. **Integration tests** (optional, Docker required) — `uv run pytest tests/integration/ -m integration -v`

## Release Process

No automated release process exists. To deploy a new version manually:

1. Install dependencies on the target machine:
   ```bash
   uv sync
   ```
2. Copy or update the `.env` file with production values.
3. Stop the running poller process.
4. Pull the new code.
5. Start the poller:
   ```bash
   uv run python poller.py
   ```
6. Start the UI (if applicable):
   ```bash
   uv run streamlit run ui/app.py
   ```

There is no versioning scheme or git tagging convention in use. All commits go directly to `main`.

## Infrastructure

**Local / development only.** No cloud infrastructure, Kubernetes, Helm charts, Terraform, or serverless configuration was found in the repository.

The two Docker Compose stacks under `docker/` are for local development and integration testing only:

**Airflow 2 stack** (`docker/airflow2/docker-compose.yml`):
- `postgres2` — PostgreSQL 15 (Airflow metadata DB)
- `airflow2-init` — one-shot init container (DB migrate + admin user creation)
- `airflow2` — webserver on port 8080; health check at `http://localhost:8080/health`
- `airflow2-scheduler` — scheduler process

**Airflow 3 stack** (`docker/airflow3/docker-compose.yml`):
- `postgres3` — PostgreSQL 15
- `airflow3-init` — one-shot init container
- `airflow3` — webserver on port 8081 (mapped from container port 8080); health check at `http://localhost:8080/health` (internal)
- `airflow3-scheduler` — scheduler process

Both stacks mount `../../dags` from the project root into `/opt/airflow/dags` in every container.

## Container / Build Artifacts

No Docker image is built for the agent itself. The agent runs directly on the host Python environment managed by `uv`. Only the Airflow containers (official `apache/airflow` images) are used.

To pull the Airflow images without starting the stacks:
```bash
docker pull apache/airflow:2.10.4
docker pull apache/airflow:3.0.1
```

## Configuration Management

All configuration is provided through environment variables loaded from a `.env` file at the project root. There is no secrets management integration (Vault, AWS SSM, Kubernetes secrets, etc.) in the current implementation.

For production use, replace `.env` file secrets with your organisation's preferred secrets manager and inject environment variables into the process at startup. The `AIRFLOW_API_TOKEN` variable supports Bearer token authentication as an alternative to username/password basic auth.

**Security note:** The poller logs a warning if `AIRFLOW_URL` uses plain HTTP:
```
AIRFLOW_URL uses plain HTTP — credentials will be sent unencrypted. Set AIRFLOW_URL to https:// for production use.
```

## Rollback Procedure

Since there is no automated deployment, rollback means reverting to the previous working code revision and restarting the processes:

1. Stop the running `poller.py` process.
2. Check out the previous revision:
   ```bash
   git log --oneline -10   # identify the target commit
   git checkout <commit-hash>
   ```
3. Re-install dependencies (in case they changed):
   ```bash
   uv sync
   ```
4. Restart the poller:
   ```bash
   uv run python poller.py
   ```

**Note:** The SQLite databases (`.airflow_agent.db`, `.langgraph.db`) and ChromaDB (`.chroma/`) are not versioned and persist across code rollbacks. In-flight investigations stored in `.langgraph.db` will be resumed by `resume_incomplete()` on startup. If a schema migration was applied in the rolled-back code, the databases may contain columns or data that the older code does not expect — in that case, consider deleting and re-creating the databases.

## Database Migrations

The database schema is managed inline in `db/store.py:init_db()`. It uses `CREATE TABLE IF NOT EXISTS` and a small migration block that attempts `ALTER TABLE ... ADD COLUMN` for columns added after the initial schema (currently `retry_count`, `last_error`, `llm_model`).

`init_db()` is called at poller startup and also by the Streamlit UI (`ui/app.py` calls `db.init_db()`).

There is no migration tool (Alembic, Flyway, etc.) — schema changes must be applied manually via the `ALTER TABLE` pattern in `init_db()`.

**To run migrations manually** (e.g. after adding a new column):
```bash
python -c "import db.store; db.store.init_db()"
```

**To roll back a migration:** SQLite does not support `DROP COLUMN` in versions before 3.35. For rolling back a column addition, recreate the table without the column or restore from a backup.
