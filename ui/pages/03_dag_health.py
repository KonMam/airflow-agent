"""DAG Health page — failure trends, duration trends, anomalies, recommendations."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


import plotly.graph_objects as go
import streamlit as st

import db.store as db

st.set_page_config(page_title="DAG Health", layout="wide")
st.title("DAG Health")

dag_ids = db.get_all_dag_ids()
if not dag_ids:
    st.info(
        "No DAG investigation data yet. Run the poller against Airflow to collect data."
    )
    st.stop()

# ── anomaly alerts ────────────────────────────────────────────────────────────
anomalies = db.get_recent_anomalies(days=7)
if anomalies:
    st.subheader("⚠️ Anomaly Alerts (7d)")
    for a in anomalies:
        st.warning(
            f"**{a['dag_id']}** / `{a['task_id']}` — "
            f"ran for **{a['actual_duration']:.0f}s** (expected p90: {a['expected_p90']:.0f}s) "
            f"at {a['detected_at'][:16]}"
        )
    st.divider()

# ── per-DAG section ───────────────────────────────────────────────────────────
selected_dag = st.selectbox("Select DAG", dag_ids)
if not selected_dag:
    st.stop()

st.subheader(f"Failure Rate — {selected_dag}")

# Build failure rate per day from investigations
investigations = db.get_recent_investigations(limit=500)
dag_invs = [i for i in investigations if i["dag_id"] == selected_dag]

if dag_invs:
    from collections import defaultdict

    daily_counts: dict[str, dict] = defaultdict(lambda: {"total": 0, "failed": 0})
    for inv in dag_invs:
        day = inv["timestamp"][:10]
        daily_counts[day]["total"] += 1
        if inv["status"] in ("completed", "pending_approval"):
            daily_counts[day]["failed"] += 1

    days_sorted = sorted(daily_counts)
    failure_rates = [
        daily_counts[d]["failed"] / daily_counts[d]["total"] * 100
        if daily_counts[d]["total"]
        else 0
        for d in days_sorted
    ]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=days_sorted,
            y=failure_rates,
            mode="lines+markers",
            name="Failure Rate %",
            line=dict(color="#e74c3c"),
        )
    )
    fig.update_layout(
        yaxis_title="Failure Rate (%)",
        xaxis_title="Date",
        height=300,
        margin=dict(t=20),
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No investigation data for this DAG yet.")

# ── duration trend ────────────────────────────────────────────────────────────
st.subheader(f"Task Duration Trend — {selected_dag}")
duration_data = db.get_dag_duration_history(selected_dag, days=14)

if duration_data:
    task_ids = sorted({d["task_id"] for d in duration_data})
    fig2 = go.Figure()
    for task_id in task_ids:
        task_data = [d for d in duration_data if d["task_id"] == task_id]
        fig2.add_trace(
            go.Scatter(
                x=[d["day"] for d in task_data],
                y=[d["avg_duration"] for d in task_data],
                mode="lines+markers",
                name=task_id,
            )
        )
    fig2.update_layout(
        yaxis_title="Avg Duration (s)",
        xaxis_title="Date",
        height=300,
        margin=dict(t=20),
    )
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("No duration history yet. Duration data is collected after investigations.")

# ── recommendations ───────────────────────────────────────────────────────────
recs = db.get_recommendations(dag_id=selected_dag)
if recs:
    st.subheader(f"Recommendations — {selected_dag}")
    for rec in recs:
        with st.expander(f"Generated {rec['generated_at'][:16]}"):
            st.markdown(rec["content"])
