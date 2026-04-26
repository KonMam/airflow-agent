"""Approvals page — human-in-the-loop action approval."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import streamlit as st

import db.store as db

st.set_page_config(page_title="Approvals", layout="wide")
st.title("Pending Approvals")
st.caption(
    "The agent will execute the recommended action only after you approve it here."
)

pending = db.get_pending_approvals()

if not pending:
    st.success("No pending approvals. All clear.")
    st.stop()

for approval in pending:
    run_id = approval["run_id"]
    inv = db.get_investigation(run_id)

    with st.container(border=True):
        st.markdown(f"### {approval['dag_id']}")
        st.markdown(f"**Run ID:** `{run_id}`")
        st.markdown(f"**Proposed Action:** `{approval['action']}`")

        if inv:
            st.markdown(f"**Failing Task:** `{inv['task_id'] or 'N/A'}`")
            if inv.get("llm_model"):
                st.caption(f"Diagnosed by `{inv['llm_model']}`")
            st.markdown("**Diagnosis:**")
            st.info(inv["diagnosis"] or "N/A")
            if inv["error_summary"]:
                st.markdown(f"**Error:** {inv['error_summary']}")

        col1, col2, _ = st.columns([1, 1, 4])
        with col1:
            if st.button("✅ Approve", key=f"approve_{run_id}", type="primary"):
                db.resolve_approval(run_id, "approved")
                st.success("Approved — the poller will execute the action shortly.")
                st.rerun()
        with col2:
            if st.button("❌ Reject", key=f"reject_{run_id}"):
                db.resolve_approval(run_id, "rejected")
                st.warning("Rejected — no action will be taken.")
                st.rerun()
