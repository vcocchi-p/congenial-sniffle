"""Streamlit monitoring dashboard for the retrieval oversight workflow."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.dashboard.components import (
    render_documents_table,
    render_event_log,
    render_global_metrics,
    render_pipeline_requests,
    render_retrieval_overview,
    render_run_history,
    render_run_summary,
    render_stage_cards,
    render_trace_table,
)
from src.dashboard.constants import DEFAULT_SOURCE_URL
from src.dashboard.state import (
    get_current_run_id,
    get_run_summary,
    get_selected_run_id,
    initialize_state,
    load_documents,
    load_events,
    load_global_metrics,
    load_pipeline_requests,
    load_recent_runs,
    load_retrieval_overview,
    load_retrieval_trace,
    load_stage_snapshots,
    reset_demo_state,
    select_run,
    submit_pipeline_request,
)

st.set_page_config(page_title="Westminster Pipeline Monitor", layout="wide")
initialize_state(st.session_state)

st.title("Westminster Council Pipeline Monitor")
st.caption("Operations and oversight dashboard for the agent pipeline.")

request_col, reset_col = st.columns([3, 1])
with request_col:
    with st.form("start-pipeline-form", clear_on_submit=False):
        source_url = st.text_input(
            "Pipeline start URL",
            value=DEFAULT_SOURCE_URL,
            help="Queue a manual start request for the retrieval pipeline.",
        )
        submitted = st.form_submit_button("Queue Pipeline Start", type="primary")
        if submitted:
            try:
                request = submit_pipeline_request(st.session_state, source_url)
            except ValueError as exc:
                st.error(str(exc))
            else:
                st.success(
                    f"Queued `{request.request_id}` for `{request.source_url}`. "
                    "This branch records the request and will hand it to the live retrieval agent later."
                )
with reset_col:
    if st.button("Reset Demo State", use_container_width=True):
        reset_demo_state(st.session_state)
        st.rerun()

st.divider()
render_stage_cards(load_stage_snapshots(st.session_state))

st.divider()
render_global_metrics(load_global_metrics(st.session_state))

st.divider()
st.subheader("Queued Pipeline Starts")
render_pipeline_requests(load_pipeline_requests(st.session_state))

st.divider()
st.subheader("Current Retrieval Status")
current_run_id = get_current_run_id(st.session_state)
recent_runs = load_recent_runs(st.session_state)
selected_run_id = get_selected_run_id(st.session_state)
current_overview = load_retrieval_overview(st.session_state, current_run_id)

current_run_summary = get_run_summary(st.session_state, current_run_id)
if current_run_summary is not None:
    render_run_summary(current_run_summary, history_mode=False)
else:
    st.info(
        "No live retrieval run is active. New URLs are queued here until the retrieval agent "
        "integration is ready."
    )
render_retrieval_overview(current_overview)

st.divider()
st.subheader("Historical Retrieval Detail")
if recent_runs:
    run_lookup = {run.run_id: run for run in recent_runs}
    run_labels = {
        run.run_id: f"{run.run_id} · {run.status.title()} · {run.documents_fetched} docs"
        for run in recent_runs
    }
    selected_index = next(
        (index for index, run in enumerate(recent_runs) if run.run_id == selected_run_id),
        0,
    )
    chosen_run_id = st.selectbox(
        "Inspect run",
        options=[run.run_id for run in recent_runs],
        index=selected_index,
        format_func=lambda run_id: run_labels[run_id],
    )
    if chosen_run_id != selected_run_id:
        select_run(st.session_state, chosen_run_id)
        st.rerun()

    selected_run_id = chosen_run_id
    selected_summary = run_lookup[selected_run_id]
    render_run_summary(selected_summary, history_mode=True)
    render_retrieval_overview(load_retrieval_overview(st.session_state, selected_run_id))

    st.markdown("**Retrieval Trace**")
    render_trace_table(load_retrieval_trace(st.session_state, selected_run_id))

    st.markdown("**Fetched Documents**")
    render_documents_table(load_documents(st.session_state, selected_run_id))
else:
    st.info("No historical retrieval runs are available yet.")

st.divider()
st.subheader("Recent Runs")
render_run_history(recent_runs, current_run_id=current_run_id)

st.divider()
render_event_log(
    load_events(st.session_state, selected_run_id),
    title="Selected Run Event Log",
    empty_message="No events are available for the selected run.",
)

st.divider()
render_event_log(
    load_events(st.session_state),
    title="Global Event Log",
    empty_message="No events have been recorded yet.",
)
