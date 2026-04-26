"""Proactive monitoring nodes: metrics collection, trend analysis, recommendations."""

import json
import logging
import statistics

import db.store as db
from agent.nodes import _llm_call
from agent.proactive.state import ProactiveState

_log = logging.getLogger(__name__)


def collect_dag_metrics(state: ProactiveState) -> dict:
    metrics = {}
    investigations = db.get_recent_investigations(limit=200)
    for dag_id in state["dag_ids"]:
        failure_count = db.get_dag_failure_count(dag_id, days=7)
        if failure_count == 0:
            continue
        history = db.get_run_history(dag_id, limit=50)
        durations = [r["duration_seconds"] for r in history if r["duration_seconds"]]
        avg_dur = statistics.mean(durations) if durations else None
        dag_invs = [i for i in investigations if i["dag_id"] == dag_id]
        recent_actions = list(
            {i["recommended_action"] for i in dag_invs if i["recommended_action"]}
        )
        metrics[dag_id] = {
            "failures_last_7d": failure_count,
            "avg_task_duration_seconds": round(avg_dur, 1) if avg_dur else None,
            "recent_recommended_actions": recent_actions,
        }
    return {"metrics": metrics}


_TREND_PROMPT = """You are an Airflow reliability engineer. Analyze these DAG metrics from the last 7 days and identify:
1. DAGs with high or increasing failure rates
2. DAGs with unusual run durations
3. Recurring failure patterns

Metrics (JSON):
{metrics}

Write a concise analysis (3-5 bullet points). Then for each problematic DAG, write a RECOMMENDATION section:

RECOMMENDATION for <dag_id>:
<specific actionable suggestion: retry policy, scheduling change, code fix, infrastructure improvement>

End with: DONE
"""


def analyze_trends(state: ProactiveState) -> dict:
    if not state["metrics"]:
        return {"trend_analysis": "No data available for trend analysis."}

    metrics_json = json.dumps(state["metrics"], indent=2)
    try:
        response = _llm_call(
            [{"role": "user", "content": _TREND_PROMPT.format(metrics=metrics_json)}]
        )
        analysis = response.choices[0].message.content or ""
    except Exception as e:
        _log.error("Trend analysis LLM call failed: %s", e)
        analysis = f"Analysis failed: {e}"

    return {"trend_analysis": analysis}


def generate_recommendations(state: ProactiveState) -> dict:
    analysis = state.get("trend_analysis", "")
    recommendations = []

    import re

    for m in re.finditer(
        r"RECOMMENDATION for ([^\n:]+):\s*(.+?)(?=RECOMMENDATION for|DONE|$)",
        analysis,
        re.DOTALL,
    ):
        dag_id = m.group(1).strip()
        content = m.group(2).strip()
        if dag_id and content:
            db.upsert_recommendation(dag_id, content)
            recommendations.append({"dag_id": dag_id, "content": content})
            _log.info("Saved recommendation for DAG=%s", dag_id)

    return {"recommendations": recommendations}
