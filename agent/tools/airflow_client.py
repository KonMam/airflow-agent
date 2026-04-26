import base64
from typing import Any

import httpx

from config import config


def _auth_header() -> dict[str, str]:
    if config.airflow_api_token:
        return {"Authorization": f"Bearer {config.airflow_api_token}"}
    credentials = base64.b64encode(
        f"{config.airflow_user}:{config.airflow_password}".encode()
    ).decode()
    return {"Authorization": f"Basic {credentials}"}


def _url(path: str) -> str:
    return f"{config.airflow_url}{config.api_prefix}{path}"


def _get(path: str, params: dict | None = None) -> Any:
    response = httpx.get(_url(path), headers=_auth_header(), params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def _post(path: str, body: dict) -> Any:
    response = httpx.post(_url(path), headers=_auth_header(), json=body, timeout=30)
    response.raise_for_status()
    return response.json()


def list_dags() -> list[dict]:
    return _get("/dags")["dags"]


def list_failed_dag_runs(dag_id: str, limit: int = 10) -> list[dict]:
    if config.airflow_api_version == "v1":
        params = {"state": "failed", "limit": limit}
    else:
        # Airflow 3: repeated params, no comma-separated
        params = {"state": "failed", "limit": limit}
    return _get(f"/dags/{dag_id}/dagRuns", params=params).get("dag_runs", [])


def list_all_failed_dag_runs(limit: int = 20) -> list[dict]:
    """List failed runs across all DAGs by fetching the global dagRuns endpoint."""
    if config.airflow_api_version == "v1":
        # Airflow 2: POST /dags/~/dagRuns/list supports cross-dag queries
        body = {"states": ["failed"], "page_limit": limit}
        return _post("/dags/~/dagRuns/list", body).get("dag_runs", [])
    else:
        # Airflow 3: GET /dagRuns with state filter
        return _get("/dagRuns", params={"state": "failed", "limit": limit}).get(
            "dag_runs", []
        )


def get_dag_run(dag_id: str, run_id: str) -> dict:
    return _get(f"/dags/{dag_id}/dagRuns/{run_id}")


def get_task_instances(dag_id: str, run_id: str) -> list[dict]:
    return _get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances").get(
        "task_instances", []
    )


def get_task_log(dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
    response = httpx.get(
        _url(
            f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}"
        ),
        headers={**_auth_header(), "Accept": "text/plain"},
        timeout=30,
    )
    response.raise_for_status()
    return response.text


def get_dag_details(dag_id: str) -> dict:
    if config.airflow_api_version == "v1":
        return _get(f"/dags/{dag_id}")
    else:
        return _get(f"/dags/{dag_id}/details")


def clear_task_instances(dag_id: str, run_id: str, task_ids: list[str]) -> dict:
    body = {
        "dag_run_id": run_id,
        "task_ids": task_ids,
        "dry_run": False,
        "include_subdags": False,
        "include_parentdag": False,
        "reset_dag_runs": True,
    }
    return _post(f"/dags/{dag_id}/clearTaskInstances", body)


def get_import_errors() -> list[dict]:
    return _get("/importErrors").get("import_errors", [])


def get_health() -> dict:
    try:
        return _get("/health")
    except (httpx.HTTPError, httpx.ConnectError, httpx.TimeoutException):
        return {}


def list_all_dag_runs_since(start_date: str, limit: int = 500) -> list[dict]:
    """All DAG runs across all DAGs since start_date (ISO string)."""
    if config.airflow_api_version == "v1":
        body = {"execution_date_gte": start_date, "page_limit": limit}
        return _post("/dags/~/dagRuns/list", body).get("dag_runs", [])
    else:
        return _get(
            "/dagRuns", params={"start_date_gte": start_date, "limit": limit}
        ).get("dag_runs", [])


def run_date_field(run: dict) -> str:
    """Normalise execution_date (v1) vs logical_date (v2) to a single key."""
    return run.get("logical_date") or run.get("execution_date", "")
