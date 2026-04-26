"""Pytest fixtures for unit and integration tests."""

import base64
import subprocess
import time
from pathlib import Path

import httpx
import pytest

PROJECT_ROOT = Path(__file__).parent.parent
AIRFLOW2_COMPOSE = PROJECT_ROOT / "docker" / "airflow2" / "docker-compose.yml"
AIRFLOW3_COMPOSE = PROJECT_ROOT / "docker" / "airflow3" / "docker-compose.yml"


def _airflow_headers(user: str = "airflow", password: str = "airflow") -> dict:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def _wait_for_airflow(url: str, timeout: int = 120) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = httpx.get(f"{url}/health", timeout=5)
            if r.status_code == 200 and r.json().get("status") == "healthy":
                return
        except Exception:
            pass
        time.sleep(3)
    raise TimeoutError(f"Airflow at {url} did not become healthy within {timeout}s")


def _trigger_dag(url: str, api_prefix: str, dag_id: str) -> str:
    headers = _airflow_headers()
    r = httpx.post(
        f"{url}{api_prefix}/dags/{dag_id}/dagRuns",
        headers=headers,
        json={},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["dag_run_id"]


def _wait_for_run_state(
    url: str,
    api_prefix: str,
    dag_id: str,
    run_id: str,
    target_state: str,
    timeout: int = 60,
) -> str:
    headers = _airflow_headers()
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = httpx.get(
            f"{url}{api_prefix}/dags/{dag_id}/dagRuns/{run_id}",
            headers=headers,
            timeout=10,
        )
        if r.status_code == 200:
            state = r.json().get("state", "")
            if state == target_state:
                return state
            if state in ("success", "failed") and state != target_state:
                return state
        time.sleep(3)
    raise TimeoutError(
        f"Run {run_id} did not reach state '{target_state}' within {timeout}s"
    )


@pytest.fixture(scope="session")
def airflow2_env():
    """Start Airflow 2, yield (url, api_prefix), then stop."""
    subprocess.run(
        ["docker", "compose", "-f", str(AIRFLOW2_COMPOSE), "up", "-d"],
        check=True,
    )
    url = "http://localhost:8080"
    _wait_for_airflow(url)
    yield url, "/api/v1"
    subprocess.run(
        ["docker", "compose", "-f", str(AIRFLOW2_COMPOSE), "down", "-v"],
        check=False,
    )


@pytest.fixture(scope="session")
def airflow3_env():
    """Start Airflow 3, yield (url, api_prefix), then stop."""
    subprocess.run(
        ["docker", "compose", "-f", str(AIRFLOW3_COMPOSE), "up", "-d"],
        check=True,
    )
    url = "http://localhost:8081"
    _wait_for_airflow(url)
    yield url, "/api/v2"
    subprocess.run(
        ["docker", "compose", "-f", str(AIRFLOW3_COMPOSE), "down", "-v"],
        check=False,
    )


@pytest.fixture
def trigger_dag_and_wait(request):
    """Factory fixture: trigger a DAG and wait for it to fail."""

    def _trigger(url: str, api_prefix: str, dag_id: str) -> str:
        run_id = _trigger_dag(url, api_prefix, dag_id)
        _wait_for_run_state(url, api_prefix, dag_id, run_id, "failed")
        return run_id

    return _trigger
