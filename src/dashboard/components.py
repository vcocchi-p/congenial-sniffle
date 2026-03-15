"""Rendering helpers for the monitoring dashboard."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import quote

import pandas as pd
import streamlit as st

from src.dashboard.state import (
    AnalysisOverview,
    PipelineRequest,
    RetrievalTraceStep,
    RunSummary,
    StageSnapshot,
)
from src.models.analysis import AgendaItemAnalysis
from src.models.documents import AgentEvent, MeetingDocument, RetrievalBundle


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
    columns = st.columns(7)
    labels = (
        ("Active Run", format_status(str(metrics["active_run_status"]))),
        ("Docs Discovered", metrics["documents_discovered"]),
        ("Docs Fetched", metrics["documents_fetched"]),
        ("Voter Briefs", metrics["summaries_generated"]),
        ("Manual Starts", metrics["manual_requests"]),
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
        f" | Trigger {format_status(summary.trigger_type)}"
        f" | Errors {summary.errors}"
    )


def render_retrieval_overview(overview: dict[str, Any]) -> None:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Current Step", format_step_name(overview["current_step"]))
    col2.markdown("**Source**")
    if overview["current_source_url"]:
        col2.markdown(
            f"[{truncate_url(overview['current_source_url'])}]"
            f"({overview['current_source_url']})"
        )
    else:
        col2.write("—")
    col3.metric("Discovered", overview["documents_discovered"])
    col4.metric("Fetched", overview["documents_fetched"])

    active_request = overview.get("active_request")
    if active_request is not None and overview.get("summary") is None:
        st.info(
            f"Manual start request `{active_request.request_id}` is running for "
            f"`{active_request.source_url}`."
        )

    if overview["latest_error"]:
        st.error(overview["latest_error"])


def render_trace_table(trace_steps: list[RetrievalTraceStep]) -> None:
    if not trace_steps:
        st.info("No retrieval trace is available for this run.")
        return

    st.dataframe(
        pd.DataFrame(build_trace_rows(trace_steps)),
        hide_index=True,
        use_container_width=True,
        column_config={
            "Document / Context": st.column_config.LinkColumn(
                "Document / Context",
                display_text=r".*#trace-label=(.*)$",
            ),
        },
    )


def render_documents_table(documents: list[MeetingDocument]) -> None:
    if not documents:
        st.info("No documents were fetched for this run.")
        return

    ordered_documents = list(documents)
    st.dataframe(
        pd.DataFrame(build_document_rows(ordered_documents)),
        hide_index=True,
        use_container_width=True,
        column_config={
            "URL": st.column_config.LinkColumn("URL", display_text="Open"),
        },
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
                "Run ID": request.run_id or "—",
                "Status": format_status(request.status),
                "Requested At": format_timestamp(request.requested_at),
                "Source URL": request.source_url,
                "Message": request.message,
            }
            for request in requests
        ]
    )


def render_resource_bundle(bundle: RetrievalBundle | None) -> None:
    if bundle is None:
        st.info("No retrieval resource bundle is stored for this run.")
        return

    metric_cols = st.columns(5)
    counts = (
        ("Committees", len(bundle.committees)),
        ("Meetings", len(bundle.meetings)),
        ("Documents", len(bundle.documents)),
        ("Agenda Items", len(bundle.agenda_items)),
        ("Decisions", len(bundle.decisions)),
    )
    for column, (label, value) in zip(metric_cols, counts):
        with column:
            st.metric(label, value)

    tabs = st.tabs(["Committees", "Meetings", "Agenda Items", "Decisions"])
    with tabs[0]:
        if bundle.committees:
            st.dataframe(
                pd.DataFrame(build_committee_rows(bundle)),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "URL": st.column_config.LinkColumn("URL", display_text="Open"),
                },
            )
        else:
            st.info("No committees are stored for this run.")
    with tabs[1]:
        if bundle.meetings:
            st.table(
                [
                    {
                        "Meeting ID": meeting.meeting_id,
                        "Committee": meeting.committee_name,
                        "Date": meeting.date,
                        "Upcoming": "Yes" if meeting.is_upcoming else "No",
                        "Attendees": len(meeting.attendees),
                    }
                    for meeting in bundle.meetings
                ]
            )
        else:
            st.info("No meetings are stored for this run.")
    with tabs[2]:
        if bundle.agenda_items:
            st.table(
                [
                    {
                        "Meeting ID": item.meeting_id,
                        "Item": item.item_number,
                        "Title": item.title,
                        "Has Decision": "Yes" if item.decision_url else "No",
                    }
                    for item in bundle.agenda_items
                ]
            )
        else:
            st.info("No agenda items are stored for this run.")
    with tabs[3]:
        if bundle.decisions:
            st.table(
                [
                    {
                        "Agenda Title": decision.agenda_title or "—",
                        "Title": decision.title,
                        "Made By": decision.made_by or "—",
                        "Date": decision.date or "—",
                    }
                    for decision in bundle.decisions
                ]
            )
        else:
            st.info("No decision details are stored for this run.")


def render_analysis_overview(overview: AnalysisOverview | None) -> None:
    if overview is None:
        st.info("No persisted analysis output is available for this run yet.")
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Meeting", f"{overview.meeting_id}")
    col2.metric("Committee", overview.committee_name)
    col3.metric("Voter Briefs", overview.items_generated)
    col4.metric("Notify Voters", overview.notify_voters)

    st.caption(
        f"Analysis Run `{overview.analysis_run_id}`"
        f" | Mode {format_status(overview.analysis_mode)}"
        f" | Status {format_status(overview.status)}"
        f" | Meeting Date {overview.meeting_date}"
        f" | Completed {format_timestamp(overview.completed_at)}"
    )
    st.markdown(f"**Why this meeting was selected:** {overview.selected_reason}")


def render_analysis_items(items: list[AgendaItemAnalysis]) -> None:
    if not items:
        st.info("No analysed agenda items are stored for this run.")
        return

    st.dataframe(
        pd.DataFrame(build_analysis_rows(items)),
        hide_index=True,
        use_container_width=True,
        column_config={
            "Source": st.column_config.LinkColumn("Source", display_text="Open"),
        },
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


def build_trace_rows(trace_steps: list[RetrievalTraceStep]) -> list[dict[str, str]]:
    return [
        {
            "Time": format_timestamp(step.timestamp),
            "Step": format_step_name(step.step_name),
            "Status": format_status(step.event_type),
            "Message": step.message,
            "Document / Context": build_trace_context_link(step),
            "Detail": step.detail or "—",
        }
        for step in trace_steps
    ]


def build_document_rows(documents: list[MeetingDocument]) -> list[dict[str, str]]:
    return [
        {
            "Fetched At": format_timestamp(getattr(document, "fetched_at", None)),
            "Title": document.title,
            "Type": document.doc_type.value,
            "Source": getattr(document, "committee", None) or f"Meeting {document.meeting_id}",
            "URL": document.url,
        }
        for document in documents
    ]


def build_committee_rows(bundle: RetrievalBundle) -> list[dict[str, str | int]]:
    return [
        {
            "ID": committee.id,
            "Name": committee.name,
            "URL": committee.url,
        }
        for committee in bundle.committees
    ]


def build_analysis_rows(items: list[AgendaItemAnalysis]) -> list[dict[str, str]]:
    return [
        {
            "Item": item.item_number,
            "Title": item.title,
            "Summary": item.plain_summary,
            "Why It Matters": item.why_it_matters,
            "Watch": item.what_to_watch,
            "Notify": "Yes" if item.notify_voters else "No",
            "Source": item.source_urls[0] if item.source_urls else "",
        }
        for item in items
    ]


def format_trace_context(step: RetrievalTraceStep) -> str:
    if step.document_title:
        return step.document_title
    if step.committee:
        return step.committee
    if step.meeting_id is not None:
        return f"Meeting {step.meeting_id}"
    if step.source_url:
        return f"Source: {truncate_url(step.source_url)}"
    return "—"


def build_trace_context_link(step: RetrievalTraceStep) -> str:
    target = step.document_url or step.source_url
    if not target:
        return ""
    label = format_trace_context(step)
    return f"{target}#trace-label={quote(label, safe=' ')}"
