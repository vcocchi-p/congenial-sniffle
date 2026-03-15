"""Rendering helpers for the monitoring dashboard."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import streamlit as st

from src.dashboard.state import PipelineRequest, RetrievalTraceStep, RunSummary, StageSnapshot
from src.models.documents import AgentEvent, CouncilDocument


def render_stage_cards(stages: list[StageSnapshot]) -> None:
    columns = st.columns(len(stages))
    for column, stage in zip(columns, stages):
        with column:
            st.markdown(f"**{stage.label}**")
            st.metric("Status", format_status(stage.status))
            st.caption(stage.message)
            if stage.last_updated is not None:
                st.caption(f"Updated {format_timestamp(stage.last_updated)}")
            else:
                st.caption("No activity yet")


def render_global_metrics(metrics: dict[str, Any]) -> None:
    columns = st.columns(6)
    labels = (
        ("Active Run", format_status(str(metrics["active_run_status"]))),
        ("Docs Discovered", metrics["documents_discovered"]),
        ("Docs Fetched", metrics["documents_fetched"]),
        ("Queued Starts", metrics["queued_requests"]),
        ("Recent Errors", metrics["recent_errors"]),
        ("Last Run", format_timestamp(metrics["last_run_at"])),
    )
    for column, (label, value) in zip(columns, labels):
        with column:
            st.metric(label, value)


def render_run_summary(summary: RunSummary | None, *, history_mode: bool) -> None:
    if summary is None:
        st.info("No retrieval run is available yet.")
        return

    if history_mode:
        st.info(f"Inspecting historical run `{summary.run_id}` in read-only mode.")

    col1, col2, col3 = st.columns(3)
    col1.metric("Run ID", summary.run_id)
    col2.metric("Status", format_status(summary.status))
    col3.metric("Documents Fetched", summary.documents_fetched)

    st.caption(
        "Started "
        f"{format_timestamp(summary.started_at)}"
        f" | Completed {format_timestamp(summary.completed_at)}"
        f" | Errors {summary.errors}"
    )


def render_retrieval_overview(overview: dict[str, Any]) -> None:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Current Step", format_step_name(overview["current_step"]))
    col2.metric("Source", truncate_url(overview["current_source_url"]))
    col3.metric("Discovered", overview["documents_discovered"])
    col4.metric("Fetched", overview["documents_fetched"])

    active_request = overview.get("active_request")
    if active_request is not None and overview.get("summary") is None:
        st.info(
            f"Manual start request `{active_request.request_id}` is queued for "
            f"`{active_request.source_url}`."
        )

    if overview["latest_error"]:
        st.error(overview["latest_error"])


def render_trace_table(trace_steps: list[RetrievalTraceStep]) -> None:
    if not trace_steps:
        st.info("No retrieval trace is available for this run.")
        return

    st.table(
        [
            {
                "Time": format_timestamp(step.timestamp),
                "Step": format_step_name(step.step_name),
                "Status": format_status(step.event_type),
                "Message": step.message,
                "Document": step.document_title or "—",
                "Detail": step.detail or "—",
            }
            for step in trace_steps
        ]
    )


def render_documents_table(documents: list[CouncilDocument]) -> None:
    if not documents:
        st.info("No documents were fetched for this run.")
        return

    ordered_documents = sorted(documents, key=lambda document: document.fetched_at, reverse=True)
    st.table(
        [
            {
                "Fetched At": format_timestamp(document.fetched_at),
                "Title": document.title,
                "Type": document.doc_type.value,
                "Committee": document.committee or "—",
            }
            for document in ordered_documents
        ]
    )


def render_run_history(runs: list[RunSummary], *, current_run_id: str | None) -> None:
    if not runs:
        st.info("No run history is available yet.")
        return

    st.table(
        [
            {
                "Run ID": run.run_id,
                "View": "Current" if run.run_id == current_run_id else "History",
                "Status": format_status(run.status),
                "Started": format_timestamp(run.started_at),
                "Completed": format_timestamp(run.completed_at),
                "Docs Fetched": run.documents_fetched,
                "Errors": run.errors,
            }
            for run in runs
        ]
    )


def render_pipeline_requests(requests: list[PipelineRequest]) -> None:
    if not requests:
        st.info("No pipeline start requests have been queued yet.")
        return

    st.table(
        [
            {
                "Request ID": request.request_id,
                "Status": format_status(request.status),
                "Requested At": format_timestamp(request.requested_at),
                "Source URL": request.source_url,
                "Message": request.message,
            }
            for request in requests
        ]
    )


def render_event_log(events: list[AgentEvent], *, title: str, empty_message: str) -> None:
    st.subheader(title)
    if not events:
        st.info(empty_message)
        return

    for event in reversed(events):
        metadata = event.metadata or {}
        detail = metadata.get("detail")
        suffix = f" | {detail}" if detail else ""
        st.markdown(
            f"**{format_status(event.event_type)}** `{event.agent_name}`"
            f" {event.message}  \n"
            f"<small>{format_timestamp(event.timestamp)}{suffix}</small>",
            unsafe_allow_html=True,
        )


def format_status(status: str) -> str:
    return status.replace("_", " ").title() if status else "Unknown"


def format_step_name(step_name: str | None) -> str:
    if not step_name:
        return "Unknown"
    return step_name.replace("/", " / ").title()


def format_timestamp(value: datetime | None) -> str:
    if value is None:
        return "—"
    return value.strftime("%Y-%m-%d %H:%M:%S UTC")


def truncate_url(value: str | None) -> str:
    if not value:
        return "—"
    if len(value) <= 36:
        return value
    return value[:33] + "..."
