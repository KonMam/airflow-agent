from typing import TypedDict


class AgentState(TypedDict):
    airflow_version: str
    dag_id: str
    run_id: str
    triggered_by: str  # "poller" | "manual" | "proactive"
    similar_past_investigations: list[dict]  # retrieved from ChromaDB
    investigation_notes: str  # accumulated by the ReAct node
    task_instances: list[dict]  # raw task instances for run_history
    diagnosis: str
    recommended_action: str
    error_summary: str  # short error for embedding
    primary_failing_task: str
    report: str
    action_decision: str  # "approved" | "rejected" | "" (set by poller on resume)
    llm_model_used: str  # model that produced the final diagnosis
