import sqlite3

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph

from agent.nodes import (
    diagnose,
    execute_action,
    investigate,
    recall_memory,
    report,
    save_to_memory,
)
from agent.state import AgentState
from config import config


def build_graph(checkpointer=None):
    g = StateGraph(AgentState)

    g.add_node("recall_memory", recall_memory)
    g.add_node("investigate", investigate)
    g.add_node("diagnose", diagnose)
    g.add_node("report", report)
    g.add_node("save_to_memory", save_to_memory)
    g.add_node("execute_action", execute_action)

    def _route_after_save(state: AgentState) -> str:
        return "execute_action" if state.get("recommended_action") == "retry" else END

    g.add_edge(START, "recall_memory")
    g.add_edge("recall_memory", "investigate")
    g.add_edge("investigate", "diagnose")
    g.add_edge("diagnose", "report")
    g.add_edge("report", "save_to_memory")
    g.add_conditional_edges(
        "save_to_memory", _route_after_save, ["execute_action", END]
    )
    g.add_edge("execute_action", END)

    return g.compile(
        checkpointer=checkpointer,
        interrupt_before=["execute_action"],
    )


def get_graph():
    conn = sqlite3.connect(config.langgraph_db_path, check_same_thread=False)
    checkpointer = SqliteSaver(conn)
    return build_graph(checkpointer=checkpointer)
