"""Metrics page — Airflow system health and per-DAG run statistics."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import io

import pandas as pd
import plotly.express as px
import streamlit as st

import db.store as db

st.set_page_config(page_title="Metrics", layout="wide")
st.title("Airflow Metrics")

snapshot = db.get_latest_snapshot()

if not snapshot:
    st.info(
        "No metrics collected yet. The poller collects a snapshot every 15 minutes on startup."
    )
    st.stop()

st.caption(f"Last collected: {snapshot['collected_at'][:19]} UTC")

# ── system health ─────────────────────────────────────────────────────────────
st.subheader("System Health")

scheduler_icon = "🟢" if snapshot["scheduler_status"] == "healthy" else "🔴"
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Scheduler", f"{scheduler_icon} {snapshot['scheduler_status'] or '—'}")
c2.metric("Total DAGs", snapshot["total_dags"] or 0)
c3.metric("Active DAGs", snapshot["active_dags"] or 0)
c4.metric("Paused DAGs", snapshot["paused_dags"] or 0)
c5.metric(
    "Import Errors",
    snapshot["import_errors"] or 0,
    delta=snapshot["import_errors"] or None,
    delta_color="inverse",
)

st.divider()

# ── 24h run volume ────────────────────────────────────────────────────────────
st.subheader("Last 24 Hours")
runs = snapshot["runs_24h"] or 0
success = snapshot["runs_24h_success"] or 0
failed = snapshot["runs_24h_failed"] or 0
success_rate = f"{round(success / runs * 100)}%" if runs else "—"

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Runs", runs)
c2.metric("Successful", success)
c3.metric("Failed", failed, delta=failed or None, delta_color="inverse")
c4.metric("Success Rate", success_rate)

st.divider()

# ── run volume over time ──────────────────────────────────────────────────────
st.subheader("Run Volume Over Time (7d)")
history = db.get_snapshot_history(days=7)

if len(history) > 1:
    df_hist = pd.DataFrame(history)
    df_hist["collected_at"] = pd.to_datetime(df_hist["collected_at"])
    df_hist["date"] = df_hist["collected_at"].dt.date
    daily = (
        df_hist.groupby("date")
        .agg(
            successful=("runs_24h_success", "mean"),
            failed=("runs_24h_failed", "mean"),
        )
        .reset_index()
    )
    fig = px.line(
        daily,
        x="date",
        y=["successful", "failed"],
        labels={"value": "Runs", "date": "Date", "variable": "State"},
        color_discrete_map={"successful": "#2ecc71", "failed": "#e74c3c"},
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Not enough history yet — chart appears after a few collection cycles.")

st.divider()

# ── per-dag stats table ───────────────────────────────────────────────────────
st.subheader("Per-DAG Statistics (Last 7 Days)")
dag_stats = db.get_latest_dag_stats()

if not dag_stats:
    st.info("No per-DAG stats yet.")
else:
    rows = []
    for s in dag_stats:
        rate = s["success_rate"]
        rows.append(
            {
                "DAG": s["dag_id"],
                "Runs (7d)": s["runs_7d"],
                "Success": s["success_7d"],
                "Failed": s["failed_7d"],
                "Success Rate": f"{round(rate * 100)}%" if rate is not None else "—",
                "Avg Duration (s)": s["avg_duration_seconds"] or "—",
                "Last Run": (s["last_run_at"] or "")[:16],
                "Last State": s["last_run_state"] or "—",
            }
        )

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # ── CSV export ────────────────────────────────────────────────────────────
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    st.download_button("Download CSV", buf.getvalue(), "dag_stats.csv", "text/csv")
