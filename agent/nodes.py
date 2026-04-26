"""LangGraph node functions."""

import json
import logging
import re
import statistics
import time
from typing import Literal

import httpx
import litellm
from langchain_core.tools import BaseTool
from pydantic import BaseModel, ValidationError

import db.store as db
from agent.memory.schema import InvestigationRecord
from agent.memory.store import get_store
from agent.prompts import SYSTEM_PROMPT, investigation_prompt
from agent.state import AgentState
from agent.tools import airflow_client as client
from agent.tools.airflow_tools import ALL_TOOLS
from config import config
from db.store import InvestigationStatus

litellm.suppress_debug_info = True
logging.getLogger("LiteLLM").setLevel(logging.WARNING)

_log = logging.getLogger(__name__)

_tools_by_name: dict[str, BaseTool] = {t.name: t for t in ALL_TOOLS}
_litellm_tools = [
    {
        "type": "function",
        "function": {
            "name": t.name,
            "description": t.description,
            "parameters": t.args_schema.model_json_schema()  # type: ignore[union-attr]
            if t.args_schema
            else {"type": "object", "properties": {}},
        },
    }
    for t in ALL_TOOLS
]


# ── LLM call with retry ───────────────────────────────────────────────────────


def _parse_rate_limit_wait(err_str: str) -> int:
    m = re.search(
        r"(?:try again|retry)\s+in\s+(?:(\d+)m)?([\d.]+)s", err_str, re.IGNORECASE
    )
    return int(m.group(1) or 0) * 60 + int(float(m.group(2))) + 1 if m else 0


def _llm_call(messages: list, max_retries: int = 3):
    """Call LiteLLM with retry and automatic model fallback on rate limits."""
    models = [config.llm_model] + config.llm_fallback_models

    for model_idx, model in enumerate(models):
        has_next = model_idx < len(models) - 1
        # If a fallback exists, don't waste time retrying a rate-limited model — switch fast
        effective_retries = 1 if has_next else max_retries
        for attempt in range(effective_retries):
            try:
                return litellm.completion(
                    model=model,
                    messages=messages,
                    tools=_litellm_tools,
                    tool_choice="auto",
                    api_key=config.llm_api_key or None,
                    max_tokens=config.llm_max_tokens,
                )
            except litellm.RateLimitError as e:
                err_str = str(e)
                wait = _parse_rate_limit_wait(err_str)
                if (
                    wait > config.llm_fallback_threshold_s or attempt == max_retries - 1
                ) and has_next:
                    _log.warning(
                        "Model '%s' rate limited (%ds wait) — switching to '%s'.",
                        model,
                        wait,
                        models[model_idx + 1],
                    )
                    break
                if attempt == max_retries - 1:
                    raise
                _log.warning(
                    "Model '%s' rate limited (%ds wait) — retrying %d/%d.",
                    model,
                    wait,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(wait)
            except litellm.BadRequestError as e:
                if "tool_use_failed" not in str(e) or attempt == max_retries - 1:
                    raise
                _log.warning(
                    "Malformed tool call from '%s', retrying %d/%d",
                    model,
                    attempt + 1,
                    max_retries,
                )
                messages = messages + [
                    {
                        "role": "user",
                        "content": "Your previous tool call was malformed. Use proper JSON tool_call format.",
                    }
                ]


# ── recall_memory ─────────────────────────────────────────────────────────────


def recall_memory(state: AgentState) -> dict:
    query = f"{state['dag_id']} {state['run_id']}"
    similar = get_store().query_similar(query, n_results=3)
    return {"similar_past_investigations": similar}


# ── investigate ───────────────────────────────────────────────────────────────


def investigate(state: AgentState) -> dict:
    if not state.get("dag_id") or not state.get("run_id"):
        raise ValueError(
            f"Missing required state fields: dag_id={state.get('dag_id')!r}, run_id={state.get('run_id')!r}"
        )

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": investigation_prompt(
                state["dag_id"], state["run_id"], state["similar_past_investigations"]
            ),
        },
    ]

    for _ in range(20):
        response = _llm_call(messages)
        msg = response.choices[0].message
        messages.append(msg.model_dump(exclude_none=True))

        if not msg.tool_calls:
            if not msg.content:
                raise ValueError(
                    "LLM returned empty response — likely rate-limited mid-investigation."
                )
            return {
                "investigation_notes": msg.content,
                "llm_model_used": response.model or "",
            }

        for tc in msg.tool_calls:
            tool = _tools_by_name.get(tc.function.name)
            if tool is None:
                result = f"Unknown tool: {tc.function.name}"
            else:
                args = json.loads(tc.function.arguments or "{}")
                try:
                    result = tool.invoke(args)
                except Exception as e:
                    # Tool errors are intentionally fed back to the LLM so it can self-correct
                    result = f"Tool error: {e}"
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": str(result),
                }
            )

    return {"investigation_notes": "Investigation exceeded max iterations."}


# ── diagnose ──────────────────────────────────────────────────────────────────


class _DiagnosisOutput(BaseModel):
    diagnosis: str
    failing_task: str = ""
    error_summary: str
    recommended_action: Literal[
        "retry", "fix_dag_code", "fix_infrastructure", "escalate", "monitor"
    ] = "monitor"


def _regex_diagnose(notes: str) -> dict:
    """Fallback parser for LLMs that don't produce valid JSON."""

    def extract(label: str) -> str:
        m = re.search(
            rf"^{label}:\s*(.+?)(?=\n[A-Z_]+:|$)", notes, re.MULTILINE | re.DOTALL
        )
        return m.group(1).strip() if m else ""

    return {
        "diagnosis": extract("DIAGNOSIS"),
        "primary_failing_task": extract("FAILING_TASK"),
        "error_summary": extract("ERROR_SUMMARY"),
        "recommended_action": extract("RECOMMENDED_ACTION"),
    }


def diagnose(state: AgentState) -> dict:
    notes = state["investigation_notes"]

    # Try to extract JSON — LLM may wrap it with surrounding text
    json_match = re.search(r"\{[\s\S]*\}", notes)
    if json_match:
        try:
            parsed = _DiagnosisOutput.model_validate_json(json_match.group())
            return {
                "diagnosis": parsed.diagnosis,
                "primary_failing_task": parsed.failing_task,
                "error_summary": parsed.error_summary,
                "recommended_action": parsed.recommended_action,
            }
        except (json.JSONDecodeError, ValidationError):
            pass

    return _regex_diagnose(notes)


# ── report ────────────────────────────────────────────────────────────────────


def report(state: AgentState) -> dict:
    if not state.get("diagnosis") and not state.get("error_summary"):
        raise ValueError(
            "LLM produced no structured output — will retry investigation."
        )

    lines = [
        "=" * 60,
        "AIRFLOW INCIDENT REPORT",
        f"DAG:    {state['dag_id']}",
        f"Run:    {state['run_id']}",
        f"Task:   {state.get('primary_failing_task', 'N/A')}",
        "-" * 60,
        f"DIAGNOSIS:\n{state.get('diagnosis', 'N/A')}",
        "-" * 60,
        f"RECOMMENDED ACTION: {state.get('recommended_action', 'N/A')}",
        "=" * 60,
    ]
    text = "\n".join(lines)
    print(text)

    # Fetch task instances first so we can backfill primary_task before writing to DB
    try:
        task_instances = client.get_task_instances(state["dag_id"], state["run_id"])
    except (httpx.HTTPError, httpx.ConnectError, httpx.TimeoutException):
        task_instances = []

    primary_task = state.get("primary_failing_task", "")
    if not primary_task or primary_task.lower() in ("none", "n/a", ""):
        failed = [t for t in task_instances if t.get("state") == "failed"]
        if failed:
            primary_task = failed[0]["task_id"]

    # Persist to SQLite — status depends on whether action is actionable
    action = state.get("recommended_action", "")
    status = (
        InvestigationStatus.PENDING_APPROVAL
        if action == "retry"
        else InvestigationStatus.COMPLETED
    )
    db.upsert_investigation(
        run_id=state["run_id"],
        dag_id=state["dag_id"],
        task_id=primary_task,
        diagnosis=state.get("diagnosis", ""),
        recommended_action=action,
        error_summary=state.get("error_summary", ""),
        airflow_version=state.get("airflow_version", config.airflow_api_version),
        status=status,
        llm_model=state.get("llm_model_used", ""),
    )
    if status == "pending_approval":
        db.create_pending_approval(
            run_id=state["run_id"],
            dag_id=state["dag_id"],
            action=action,
            details=state.get("diagnosis", ""),
        )

    return {
        "report": text,
        "task_instances": task_instances,
        "primary_failing_task": primary_task,
    }


# ── save_to_memory ────────────────────────────────────────────────────────────


def save_to_memory(state: AgentState) -> dict:
    record = InvestigationRecord(
        dag_id=state["dag_id"],
        run_id=state["run_id"],
        task_id=state.get("primary_failing_task", "unknown"),
        error_summary=state.get("error_summary", ""),
        diagnosis=state.get("diagnosis", ""),
        recommended_action=state.get("recommended_action", "unknown"),
        airflow_version=state.get("airflow_version", config.airflow_api_version),
    )
    get_store().save(record)

    # Record task durations for anomaly detection
    for ti in state.get("task_instances", []):
        db.insert_run_history(
            dag_id=state["dag_id"],
            task_id=ti.get("task_id", "unknown"),
            run_id=state["run_id"],
            duration_seconds=ti.get("duration"),
            state=ti.get("state", "unknown"),
        )

    return {}


# ── execute_action ────────────────────────────────────────────────────────────


def execute_action(state: AgentState) -> dict:
    """Human-approved action execution. LangGraph interrupts before this node."""
    decision = state.get("action_decision", "")
    action = state.get("recommended_action", "")

    model = state.get("llm_model_used", "")
    if decision != "approved":
        _log.info("Action '%s' was rejected or not approved — skipping.", action)
        db.upsert_investigation(
            run_id=state["run_id"],
            dag_id=state["dag_id"],
            task_id=state.get("primary_failing_task", ""),
            diagnosis=state.get("diagnosis", ""),
            recommended_action=action,
            error_summary=state.get("error_summary", ""),
            airflow_version=state.get("airflow_version", config.airflow_api_version),
            status=InvestigationStatus.REJECTED,
            llm_model=model,
        )
        return {}

    if action == "retry" and state.get("primary_failing_task"):
        try:
            client.clear_task_instances(
                state["dag_id"], state["run_id"], [state["primary_failing_task"]]
            )
            _log.info("Cleared task %s for retry.", state["primary_failing_task"])
            db.upsert_investigation(
                run_id=state["run_id"],
                dag_id=state["dag_id"],
                task_id=state.get("primary_failing_task", ""),
                diagnosis=state.get("diagnosis", ""),
                recommended_action=action,
                error_summary=state.get("error_summary", ""),
                airflow_version=state.get(
                    "airflow_version", config.airflow_api_version
                ),
                status=InvestigationStatus.RETRY_IN_PROGRESS,
                llm_model=model,
            )
        except (httpx.HTTPStatusError, httpx.ConnectError, httpx.TimeoutException) as e:
            _log.error("Failed to execute action: %s", e)

    return {}


# ── check_anomaly (called by proactive poller, not the main graph) ─────────────


def check_anomaly_for_run(
    dag_id: str, task_id: str, run_id: str, duration: float
) -> bool:
    """Return True if duration is anomalous vs historical p90. Logs to DB if so."""
    history = db.get_run_history(dag_id, task_id, limit=20)
    durations = [
        r["duration_seconds"] for r in history if r["duration_seconds"] is not None
    ]
    if len(durations) < 5:
        return False

    durations_sorted = sorted(durations)
    q1 = statistics.quantiles(durations_sorted, n=4)[0]
    q3 = statistics.quantiles(durations_sorted, n=4)[2]
    iqr = q3 - q1
    p90 = statistics.quantiles(durations_sorted, n=10)[8]
    threshold = p90 + 2.5 * iqr

    if duration > threshold:
        db.insert_anomaly(dag_id, task_id, run_id, duration, p90)
        _log.warning(
            "Anomaly detected: DAG=%s task=%s duration=%.1fs (p90=%.1fs)",
            dag_id,
            task_id,
            duration,
            p90,
        )
        return True
    return False
