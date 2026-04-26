"""Unit tests for agent nodes — LLM and Airflow API are mocked."""

from unittest.mock import MagicMock, patch

BASE_STATE = {
    "dag_id": "failing_task",
    "run_id": "manual__2024-01-01T00:00:00+00:00",
    "airflow_version": "v1",
    "triggered_by": "poller",
    "similar_past_investigations": [],
    "investigation_notes": "",
    "task_instances": [],
    "diagnosis": "",
    "recommended_action": "",
    "error_summary": "",
    "primary_failing_task": "",
    "report": "",
    "action_decision": "",
}


def _make_llm_response(content: str, tool_calls=None):
    msg = MagicMock()
    msg.content = content
    msg.tool_calls = tool_calls or []
    msg.model_dump.return_value = {"role": "assistant", "content": content}
    resp = MagicMock()
    resp.choices = [MagicMock(message=msg)]
    return resp


# ── diagnose ──────────────────────────────────────────────────────────────────


def test_diagnose_parses_all_fields():
    from agent.nodes import diagnose

    notes = (
        "DIAGNOSIS: The extract task failed due to a DB timeout.\n"
        "FAILING_TASK: extract\n"
        "ERROR_SUMMARY: DB connection timeout on extract step.\n"
        "RECOMMENDED_ACTION: retry\n"
        "DETAILS: Retry should resolve the transient issue."
    )
    result = diagnose({**BASE_STATE, "investigation_notes": notes})
    assert result["diagnosis"] == "The extract task failed due to a DB timeout."
    assert result["primary_failing_task"] == "extract"
    assert result["error_summary"] == "DB connection timeout on extract step."
    assert result["recommended_action"] == "retry"


def test_diagnose_handles_missing_fields():
    from agent.nodes import diagnose

    result = diagnose(
        {**BASE_STATE, "investigation_notes": "The model gave an unstructured answer."}
    )
    assert result["diagnosis"] == ""
    assert result["recommended_action"] == ""


# ── investigate ───────────────────────────────────────────────────────────────


def test_investigate_returns_on_no_tool_calls():
    from agent.nodes import investigate

    final_answer = (
        "DIAGNOSIS: Task failed.\nFAILING_TASK: fail\n"
        "ERROR_SUMMARY: ValueError.\nRECOMMENDED_ACTION: fix_dag_code\nDETAILS: none"
    )
    with patch("agent.nodes._llm_call", return_value=_make_llm_response(final_answer)):
        result = investigate(BASE_STATE)
    assert "DIAGNOSIS" in result["investigation_notes"]


def test_investigate_executes_tool_and_continues():
    from agent.nodes import investigate

    tool_call = MagicMock()
    tool_call.id = "call_1"
    tool_call.function.name = "list_task_instances"
    tool_call.function.arguments = '{"dag_id": "failing_task", "run_id": "run1"}'

    final_answer = "DIAGNOSIS: x\nFAILING_TASK: t\nERROR_SUMMARY: e\nRECOMMENDED_ACTION: retry\nDETAILS: d"

    call_count = 0

    def fake_llm(messages, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return _make_llm_response("", tool_calls=[tool_call])
        return _make_llm_response(final_answer)

    with (
        patch("agent.nodes._llm_call", side_effect=fake_llm),
        patch(
            "agent.nodes._tools_by_name",
            {"list_task_instances": MagicMock(invoke=lambda _: "task1: failed")},
        ),
    ):
        result = investigate(BASE_STATE)

    assert call_count == 2
    assert "DIAGNOSIS" in result["investigation_notes"]


# ── execute_action ────────────────────────────────────────────────────────────


def test_execute_action_skips_when_rejected():
    from agent.nodes import execute_action

    with (
        patch("agent.nodes.client.clear_task_instances") as mock_clear,
        patch("agent.nodes.db.upsert_investigation"),
    ):
        execute_action(
            {
                **BASE_STATE,
                "recommended_action": "retry",
                "primary_failing_task": "fail",
                "action_decision": "rejected",
            }
        )
        mock_clear.assert_not_called()


def test_execute_action_clears_task_when_approved():
    from agent.nodes import execute_action

    with (
        patch("agent.nodes.client.clear_task_instances") as mock_clear,
        patch("agent.nodes.db.upsert_investigation"),
    ):
        mock_clear.return_value = {"task_instances": [{}]}
        execute_action(
            {
                **BASE_STATE,
                "recommended_action": "retry",
                "primary_failing_task": "fail",
                "action_decision": "approved",
            }
        )
        mock_clear.assert_called_once_with(
            "failing_task", BASE_STATE["run_id"], ["fail"]
        )
