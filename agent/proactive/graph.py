from langgraph.graph import END, START, StateGraph

from agent.proactive.nodes import (
    analyze_trends,
    collect_dag_metrics,
    generate_recommendations,
)
from agent.proactive.state import ProactiveState


def _build():
    g = StateGraph(ProactiveState)
    g.add_node("collect_dag_metrics", collect_dag_metrics)
    g.add_node("analyze_trends", analyze_trends)
    g.add_node("generate_recommendations", generate_recommendations)

    g.add_edge(START, "collect_dag_metrics")
    g.add_edge("collect_dag_metrics", "analyze_trends")
    g.add_edge("analyze_trends", "generate_recommendations")
    g.add_edge("generate_recommendations", END)

    return g.compile()


proactive_graph = _build()
