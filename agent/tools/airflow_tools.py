"""LangGraph-compatible tool definitions wrapping airflow_client."""

from langchain_core.tools import tool
from pydantic import BaseModel, Field

from agent.tools import airflow_client as client


class ListFailedDagRunsInput(BaseModel):
    dag_id: str = Field(..., min_length=1, description="DAG ID to query")
    limit: int = Field(
        default=10, ge=1, le=100, description="Maximum number of runs to return"
    )


class GetDagRunDetailsInput(BaseModel):
    dag_id: str = Field(..., min_length=1, description="DAG ID")
    run_id: str = Field(..., min_length=1, description="DAG run ID")


class ListTaskInstancesInput(BaseModel):
    dag_id: str = Field(..., min_length=1, description="DAG ID")
    run_id: str = Field(..., min_length=1, description="DAG run ID")


class GetTaskLogInput(BaseModel):
    dag_id: str = Field(..., min_length=1, description="DAG ID")
    run_id: str = Field(..., min_length=1, description="DAG run ID")
    task_id: str = Field(..., min_length=1, description="Task ID")
    try_number: int = Field(default=1, ge=1, description="Attempt number (1-based)")


class GetDagStructureInput(BaseModel):
    dag_id: str = Field(..., min_length=1, description="DAG ID")


class ClearAndRetryTasksInput(BaseModel):
    dag_id: str = Field(..., min_length=1, description="DAG ID")
    run_id: str = Field(..., min_length=1, description="DAG run ID")
    task_ids: str = Field(
        ..., min_length=1, description="Comma-separated task IDs to clear"
    )


@tool(args_schema=ListFailedDagRunsInput)
def list_failed_dag_runs(dag_id: str, limit: int = 10) -> str:
    """List failed DAG runs for a specific DAG. Returns run IDs, states, and dates."""
    runs = client.list_failed_dag_runs(dag_id, limit=limit)
    if not runs:
        return f"No failed runs found for DAG '{dag_id}'."
    lines = []
    for r in runs:
        lines.append(
            f"run_id={r['dag_run_id']}  date={client.run_date_field(r)}  state={r['state']}"
        )
    return "\n".join(lines)


@tool(args_schema=GetDagRunDetailsInput)
def get_dag_run_details(dag_id: str, run_id: str) -> str:
    """Get details of a specific DAG run including state, start/end times, and notes."""
    run = client.get_dag_run(dag_id, run_id)
    return (
        f"dag_id={run['dag_id']}\n"
        f"run_id={run['dag_run_id']}\n"
        f"state={run['state']}\n"
        f"date={client.run_date_field(run)}\n"
        f"start_date={run.get('start_date', 'N/A')}\n"
        f"end_date={run.get('end_date', 'N/A')}"
    )


@tool(args_schema=ListTaskInstancesInput)
def list_task_instances(dag_id: str, run_id: str) -> str:
    """List all task instances for a DAG run, including their states and try numbers."""
    tasks = client.get_task_instances(dag_id, run_id)
    if not tasks:
        return "No task instances found."
    lines = []
    for t in tasks:
        lines.append(
            f"task_id={t['task_id']}  state={t['state']}  "
            f"try_number={t.get('try_number', 1)}  "
            f"duration={t.get('duration', 'N/A')}s"
        )
    return "\n".join(lines)


@tool(args_schema=GetTaskLogInput)
def get_task_log(dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
    """Fetch the log output for a specific task instance. Returns last 40 lines."""
    try:
        log = client.get_task_log(dag_id, run_id, task_id, try_number)
    except Exception as e:
        if "404" in str(e):
            return (
                f"No logs available for task '{task_id}' (try {try_number}). "
                "This usually means the task never ran — it may be in 'upstream_failed' state "
                "because a dependency failed before it. Investigate the upstream task instead."
            )
        return f"Failed to fetch log: {e}"
    lines = log.splitlines()
    if len(lines) > 40:
        lines = [f"[... truncated, showing last 40 of {len(lines)} lines ...]"] + lines[
            -40:
        ]
    return "\n".join(lines)


@tool(args_schema=GetDagStructureInput)
def get_dag_structure(dag_id: str) -> str:
    """Get DAG metadata including description, schedule, tags, and task count."""
    dag = client.get_dag_details(dag_id)
    return (
        f"dag_id={dag['dag_id']}\n"
        f"description={dag.get('description', 'N/A')}\n"
        f"schedule={dag.get('schedule_interval', dag.get('timetable_summary', 'N/A'))}\n"
        f"tags={[t['name'] for t in dag.get('tags', [])]}\n"
        f"is_paused={dag.get('is_paused', False)}\n"
        f"owners={dag.get('owners', [])}"
    )


@tool
def get_import_errors() -> str:
    """List any DAG import errors (broken DAG files that failed to parse)."""
    errors = client.get_import_errors()
    if not errors:
        return "No import errors found."
    lines = []
    for e in errors:
        lines.append(
            f"file={e.get('filename', 'N/A')}\nerror={e.get('stack_trace', 'N/A')}\n"
        )
    return "\n---\n".join(lines)


@tool(args_schema=ClearAndRetryTasksInput)
def clear_and_retry_tasks(dag_id: str, run_id: str, task_ids: str) -> str:
    """Clear and retry specific failed tasks. task_ids should be comma-separated task IDs."""
    ids = [t.strip() for t in task_ids.split(",") if t.strip()]
    result = client.clear_task_instances(dag_id, run_id, ids)
    return (
        f"Cleared {len(result.get('task_instances', []))} task instance(s) for retry."
    )


ALL_TOOLS = [
    list_failed_dag_runs,
    get_dag_run_details,
    list_task_instances,
    get_task_log,
    get_dag_structure,
    get_import_errors,
    # clear_and_retry_tasks is handled by execute_action node, not the investigation loop
]
