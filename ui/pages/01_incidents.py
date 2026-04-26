"""Incidents page — filterable list with full investigation detail."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import streamlit as st

import db.store as db

st.set_page_config(page_title="Incidents", layout="wide")
st.title("Incidents")

investigations = db.get_recent_investigations(limit=200)
if not investigations:
    st.info("No investigations yet.")
    st.stop()

# ── filters ───────────────────────────────────────────────────────────────────
all_dags = sorted({i["dag_id"] for i in investigations})
all_actions = sorted(
    {i["recommended_action"] for i in investigations if i["recommended_action"]}
)

col1, col2 = st.columns(2)
with col1:
    selected_dags = st.multiselect("Filter by DAG", all_dags)
with col2:
    selected_actions = st.multiselect("Filter by action", all_actions)

filtered = investigations
if selected_dags:
    filtered = [i for i in filtered if i["dag_id"] in selected_dags]
if selected_actions:
    filtered = [i for i in filtered if i["recommended_action"] in selected_actions]

st.caption(f"Showing {len(filtered)} of {len(investigations)} incidents")
st.divider()

# ── incident cards ────────────────────────────────────────────────────────────
STATUS_ICONS = {
    "in_progress": "⏳",
    "completed": "🟢",
    "pending_approval": "🟡",
    "retry_in_progress": "🔄",
    "resolved": "✅",
    "rejected": "⚪",
    "pending_retry": "🔴",
    "failed_permanent": "⛔",
    "stale": "🌫️",
}

for inv in filtered:
    status_icon = STATUS_ICONS.get(inv["status"], "⚫")
    with st.expander(
        f"{status_icon} **{inv['dag_id']}** — {inv['recommended_action'] or 'unknown'} | {inv['timestamp'][:16]}"
    ):
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**Run ID:** `{inv['run_id']}`")
            st.markdown(f"**Failing Task:** `{inv['task_id'] or 'N/A'}`")
            st.markdown(f"**Status:** {inv['status']}")
            st.markdown(f"**Airflow Version:** {inv['airflow_version'] or 'N/A'}")
            if inv.get("llm_model"):
                st.markdown(f"**Model:** `{inv['llm_model']}`")
            retry_count = inv.get("retry_count", 0)
            if retry_count:
                st.markdown(f"**Retry attempts:** {retry_count}")
        with c2:
            st.markdown(f"**Error Summary:**\n{inv['error_summary'] or 'N/A'}")
            if inv.get("last_error"):
                st.caption(f"Last error: {inv['last_error'][:200]}")

        st.markdown("**Diagnosis:**")
        st.info(inv["diagnosis"] or "N/A")
