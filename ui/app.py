"""Airflow Agent — Dashboard."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import time

import streamlit as st

import db.store as db

db.init_db()

st.set_page_config(page_title="Airflow Agent", page_icon="✈", layout="wide")
st.title("Airflow Agent Dashboard")

# ── metrics row ───────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

with col1:
    total = db.count_investigations_since(days=7)
    st.metric("Incidents (7d)", total)

with col2:
    pending = len(db.get_pending_approvals())
    st.metric(
        "Pending Approvals",
        pending,
        delta=pending if pending else None,
        delta_color="inverse",
    )

with col3:
    retrying = len(db.get_pending_retry_investigations())
    st.metric(
        "Pending Retry",
        retrying,
        delta=retrying if retrying else None,
        delta_color="inverse",
    )

with col4:
    top_dag = db.most_failing_dag(days=7) or "—"
    st.metric("Most Failing DAG", top_dag)

st.divider()

# ── recent incidents ──────────────────────────────────────────────────────────
st.subheader("Recent Incidents")

investigations = db.get_recent_investigations(limit=10)
if not investigations:
    st.info("No investigations yet. Start the poller and trigger a failing DAG.")
else:
    for inv in investigations:
        status_color = {
            "in_progress": "⏳",
            "completed": "🟢",
            "pending_approval": "🟡",
            "retry_in_progress": "🔄",
            "resolved": "✅",
            "rejected": "⚪",
            "pending_retry": "🔴",
            "failed_permanent": "⛔",
            "stale": "🌫️",
        }.get(inv["status"], "⚫")

        with st.expander(
            f"{status_color} {inv['dag_id']} — {inv['recommended_action'] or 'N/A'} "
            f"| {inv['timestamp'][:16]}"
        ):
            st.markdown(f"**Run ID:** `{inv['run_id']}`")
            st.markdown(f"**Failing Task:** {inv['task_id'] or 'N/A'}")
            st.markdown(f"**Status:** {inv['status']}")
            st.markdown(f"**Diagnosis:**\n\n{inv['diagnosis'] or 'N/A'}")

st.divider()
st.caption("Auto-refreshes every 30s. Navigate using the sidebar.")

time.sleep(30)
st.rerun()
