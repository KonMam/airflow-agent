.PHONY: test test-all test-int ui poller up2 up3 down2 down3 lint fmt clean

test:
	uv run pytest tests/unit/ -v

test-all:
	uv run pytest tests/ -v

test-int:
	uv run pytest tests/integration/ -m integration -v

ui:
	uv run streamlit run ui/app.py

poller:
	uv run python poller.py

up2:
	docker compose -f docker/airflow2/docker-compose.yml up -d

up3:
	docker compose -f docker/airflow3/docker-compose.yml up -d

down2:
	docker compose -f docker/airflow2/docker-compose.yml down

down3:
	docker compose -f docker/airflow3/docker-compose.yml down

lint:
	uv run ruff check .

fmt:
	uv run ruff format .

clean:
	rm -f .airflow_agent.db .airflow_agent.db-wal .airflow_agent.db-shm .langgraph.db .langgraph.db-wal .langgraph.db-shm .seen_runs.db 
	rm -rf .chroma
