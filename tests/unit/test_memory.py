"""Unit tests for ChromaDB investigation store."""

import pytest

from agent.memory.schema import InvestigationRecord


@pytest.fixture
def tmp_store(monkeypatch, tmp_path):
    monkeypatch.setattr("config.config.chroma_path", str(tmp_path / "chroma"))
    # Reset singleton
    import agent.memory.store as store_module

    store_module._store = None
    from agent.memory.store import get_store

    return get_store()


def test_save_and_query(tmp_store):
    record = InvestigationRecord(
        dag_id="my_dag",
        run_id="run_001",
        task_id="extract",
        error_summary="DB timeout",
        diagnosis="Database connection issue",
        recommended_action="retry",
        airflow_version="v1",
    )
    tmp_store.save(record)

    results = tmp_store.query_similar("my_dag extract DB timeout", n_results=1)
    assert len(results) == 1
    assert results[0]["metadata"]["dag_id"] == "my_dag"
    assert results[0]["metadata"]["run_id"] == "run_001"


def test_upsert_deduplicates_by_run_id(tmp_store):
    for _ in range(3):
        tmp_store.save(
            InvestigationRecord(
                dag_id="dag",
                run_id="run_001",
                task_id="t",
                error_summary="err",
                diagnosis="diag",
                recommended_action="retry",
                airflow_version="v1",
            )
        )
    results = tmp_store.query_similar("dag", n_results=5)
    assert len(results) == 1


def test_empty_store_returns_empty_list(tmp_store):
    results = tmp_store.query_similar("anything", n_results=3)
    assert results == []
