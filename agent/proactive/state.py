from typing import TypedDict


class ProactiveState(TypedDict):
    dag_ids: list[str]
    metrics: (
        dict  # dag_id → {failure_rate, avg_duration, trend_slope, investigation_count}
    )
    trend_analysis: str  # LLM narrative about trends
    recommendations: list[dict]  # [{dag_id, content}]
