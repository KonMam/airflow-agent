"""Integration tests against live Airflow 3. Requires Docker."""

from unittest.mock import patch

import pytest

pytestmark = pytest.mark.integration

FINAL_ANSWER = (
    "DIAGNOSIS: The fail task raised a ValueError intentionally.\n"
    "FAILING_TASK: fail\n"
    "ERROR_SUMMARY: Intentional ValueError in failing_task DAG.\n"
    "RECOMMENDED_ACTION: fix_dag_code\n"
    "DETAILS: This is a test DAG designed to always fail."
)


def _mock_llm_response(content):
    from unittest.mock import MagicMock

    msg = MagicMock()
    msg.content = content
    msg.tool_calls = []
    msg.model_dump.return_value = {"role": "assistant", "content": content}
    resp = MagicMock()
    resp.choices = [MagicMock(message=msg)]
    return resp


@pytest.fixture(autouse=True)
def patch_config_for_airflow3(airflow3_env, monkeypatch, tmp_path):
    url, api_prefix = airflow3_env
    monkeypatch.setattr("config.config.airflow_url", url)
    monkeypatch.setattr("config.config.airflow_api_version", "v2")
    monkeypatch.setattr("config.config.chroma_path", str(tmp_path / "chroma"))
    monkeypatch.setattr("config.config.db_path", str(tmp_path / "agent.db"))
    monkeypatch.setattr("config.config.langgraph_db_path", str(tmp_path / "lg.db"))
    import agent.memory.store as store_module

    store_module._store = None
    import db.store as db

    db._local = type("L", (), {})()
    db.init_db()


def test_failing_task_investigated_airflow3(airflow3_env, trigger_dag_and_wait):
    url, api_prefix = airflow3_env
    run_id = trigger_dag_and_wait(url, api_prefix, "failing_task")

    from langgraph.checkpoint.memory import MemorySaver

    from agent.graph import build_graph

    graph = build_graph(checkpointer=MemorySaver())
    with patch("agent.nodes._llm_call", return_value=_mock_llm_response(FINAL_ANSWER)):
        result = graph.invoke(
            {
                "dag_id": "failing_task",
                "run_id": run_id,
                "airflow_version": "v2",
                "triggered_by": "test",
                "similar_past_investigations": [],
                "investigation_notes": "",
                "task_instances": [],
                "diagnosis": "",
                "recommended_action": "",
                "error_summary": "",
                "primary_failing_task": "",
                "report": "",
                "action_decision": "",
            },
            config={"configurable": {"thread_id": run_id}},
        )

    assert result["diagnosis"] != ""
    assert result["primary_failing_task"] != ""
