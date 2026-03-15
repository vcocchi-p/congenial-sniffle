"""Pure helpers for dashboard session state and derived monitoring views."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, MutableMapping
from urllib.parse import urlparse

from src.dashboard.constants import (
    DASHBOARD_CURRENT_RUN_KEY,
    DASHBOARD_PIPELINE_REQUESTS_KEY,
    DASHBOARD_REQUEST_SEQUENCE_KEY,
    DASHBOARD_RUN_DOCUMENTS_KEY,
    DASHBOARD_RUN_EVENTS_KEY,
    DASHBOARD_RUN_ORDER_KEY,
    DASHBOARD_RUN_SEQUENCE_KEY,
    DASHBOARD_SELECTED_RUN_KEY,
    DASHBOARD_SUMMARIES_GENERATED_KEY,
    HISTORY_LIMIT,
    PIPELINE_STAGES,
    PLACEHOLDER_STAGE_MESSAGE,
    STAGE_LABELS,
)
from src.models.documents import AgentEvent, CouncilDocument


@dataclass(frozen=True)
class StageSnapshot:
    stage: str
    label: str
    status: str
    message: str
    last_updated: datetime | None


@dataclass(frozen=True)
class RunSummary:
    run_id: str
    status: str
    started_at: datetime | None
    completed_at: datetime | None
    documents_discovered: int
    documents_fetched: int
    errors: int
    latest_message: str
    source_url: str | None


@dataclass(frozen=True)
class RetrievalTraceStep:
    timestamp: datetime
    step_name: str
    event_type: str
    message: str
    source_url: str | None
    document_title: str | None
    document_type: str | None
    detail: str | None


@dataclass(frozen=True)
class PipelineRequest:
    request_id: str
    source_url: str
    requested_at: datetime
    status: str
    message: str


SessionState = MutableMapping[str, Any]


def initialize_state(state: SessionState) -> None:
    if _missing_state(state):
        _replace_dashboard_state(state)


def reset_demo_state(state: SessionState) -> None:
    _replace_dashboard_state(state, force=True)


def load_runs(state: SessionState) -> list[RunSummary]:
    run_events = state.get(DASHBOARD_RUN_EVENTS_KEY, {})
    run_documents = state.get(DASHBOARD_RUN_DOCUMENTS_KEY, {})
    summaries: list[RunSummary] = []
    for run_id in state.get(DASHBOARD_RUN_ORDER_KEY, []):
        events = run_events.get(run_id, [])
        documents = run_documents.get(run_id, [])
        summary = summarize_run(run_id, events, documents)
        if summary is not None:
            summaries.append(summary)
    return sorted(
        summaries,
        key=lambda summary: summary.started_at or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )


def load_events(state: SessionState, run_id: str | None = None) -> list[AgentEvent]:
    run_events = state.get(DASHBOARD_RUN_EVENTS_KEY, {})
    if run_id is not None:
        return list(run_events.get(run_id, []))

    events: list[AgentEvent] = []
    for events_for_run in run_events.values():
        events.extend(events_for_run)
    return sorted(events, key=lambda event: event.timestamp)


def load_documents(state: SessionState, run_id: str | None = None) -> list[CouncilDocument]:
    run_documents = state.get(DASHBOARD_RUN_DOCUMENTS_KEY, {})
    if run_id is not None:
        return list(run_documents.get(run_id, []))

    documents: list[CouncilDocument] = []
    for documents_for_run in run_documents.values():
        documents.extend(documents_for_run)
    return sorted(documents, key=lambda document: document.fetched_at, reverse=True)


def load_stage_snapshots(state: SessionState) -> list[StageSnapshot]:
    current_run_id = get_current_run_id(state)
    current_run_summary = get_run_summary(state, current_run_id)
    current_events = load_events(state, current_run_id) if current_run_id else []
    latest_event = current_events[-1] if current_events else None
    active_request = get_active_request(state)
    recent_runs = load_recent_runs(state)
    latest_run = recent_runs[0] if recent_runs else None

    snapshots: list[StageSnapshot] = []
    for stage in PIPELINE_STAGES:
        if stage == "retrieval" and current_run_summary is not None:
            snapshots.append(
                StageSnapshot(
                    stage=stage,
                    label=STAGE_LABELS[stage],
                    status=current_run_summary.status,
                    message=current_run_summary.latest_message,
                    last_updated=latest_event.timestamp if latest_event else current_run_summary.completed_at,
                )
            )
            continue
        if stage == "retrieval" and active_request is not None:
            snapshots.append(
                StageSnapshot(
                    stage=stage,
                    label=STAGE_LABELS[stage],
                    status="queued",
                    message=f"Queued manual start from {active_request.source_url}",
                    last_updated=active_request.requested_at,
                )
            )
            continue
        if stage == "retrieval":
            message = "Awaiting manual start URL."
            last_updated = None
            if latest_run is not None:
                message = f"No active run. Last run {latest_run.run_id} finished {latest_run.status}."
                last_updated = latest_run.completed_at or latest_run.started_at
            snapshots.append(
                StageSnapshot(
                    stage=stage,
                    label=STAGE_LABELS[stage],
                    status="idle",
                    message=message,
                    last_updated=last_updated,
                )
            )
            continue

        snapshots.append(
            StageSnapshot(
                stage=stage,
                label=STAGE_LABELS[stage],
                status="placeholder",
                message=PLACEHOLDER_STAGE_MESSAGE,
                last_updated=None,
            )
        )
    return snapshots


def load_global_metrics(state: SessionState) -> dict[str, Any]:
    current_run_id = get_current_run_id(state)
    current_run = get_run_summary(state, current_run_id)
    recent_runs = load_recent_runs(state)
    active_request = get_active_request(state)
    last_run_at = None
    if current_run is not None:
        last_run_at = current_run.completed_at or current_run.started_at
    elif recent_runs:
        last_run_at = recent_runs[0].completed_at or recent_runs[0].started_at

    active_run_status = "idle"
    if current_run is not None:
        active_run_status = current_run.status
    elif active_request is not None:
        active_run_status = active_request.status

    return {
        "active_run_status": active_run_status,
        "documents_discovered": current_run.documents_discovered if current_run is not None else 0,
        "documents_fetched": current_run.documents_fetched if current_run is not None else 0,
        "queued_requests": len(load_pipeline_requests(state)),
        "summaries_generated": state.get(DASHBOARD_SUMMARIES_GENERATED_KEY, 0),
        "recent_errors": sum(run.errors for run in recent_runs),
        "last_run_at": last_run_at,
    }


def load_recent_runs(state: SessionState) -> list[RunSummary]:
    return load_runs(state)[:HISTORY_LIMIT]


def get_current_run_id(state: SessionState) -> str | None:
    current_run_id = state.get(DASHBOARD_CURRENT_RUN_KEY)
    if current_run_id in state.get(DASHBOARD_RUN_EVENTS_KEY, {}):
        return current_run_id
    return None


def get_selected_run_id(state: SessionState) -> str | None:
    selected_run_id = state.get(DASHBOARD_SELECTED_RUN_KEY)
    if selected_run_id in state.get(DASHBOARD_RUN_EVENTS_KEY, {}):
        return selected_run_id
    recent_runs = load_recent_runs(state)
    return recent_runs[0].run_id if recent_runs else get_current_run_id(state)


def select_run(state: SessionState, run_id: str | None) -> None:
    if run_id is None or run_id not in state.get(DASHBOARD_RUN_EVENTS_KEY, {}):
        recent_runs = load_recent_runs(state)
        state[DASHBOARD_SELECTED_RUN_KEY] = recent_runs[0].run_id if recent_runs else None
        return
    state[DASHBOARD_SELECTED_RUN_KEY] = run_id


def load_pipeline_requests(state: SessionState) -> list[PipelineRequest]:
    requests = list(state.get(DASHBOARD_PIPELINE_REQUESTS_KEY, []))
    return sorted(requests, key=lambda request: request.requested_at, reverse=True)


def get_active_request(state: SessionState) -> PipelineRequest | None:
    requests = load_pipeline_requests(state)
    return requests[0] if requests else None


def submit_pipeline_request(state: SessionState, source_url: str) -> PipelineRequest:
    initialize_state(state)
    normalized_url = source_url.strip()
    parsed = urlparse(normalized_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Enter a valid http or https URL to start the pipeline.")

    sequence = int(state.get(DASHBOARD_REQUEST_SEQUENCE_KEY, 0)) + 1
    request = PipelineRequest(
        request_id=f"request-{sequence:03d}",
        source_url=normalized_url,
        requested_at=datetime.now(timezone.utc),
        status="queued",
        message="Queued for retrieval agent handoff once the live pipeline is wired.",
    )
    state[DASHBOARD_PIPELINE_REQUESTS_KEY].append(request)
    state[DASHBOARD_REQUEST_SEQUENCE_KEY] = sequence
    state[DASHBOARD_CURRENT_RUN_KEY] = None
    return request


def summarize_run(
    run_id: str, events: Iterable[AgentEvent], documents: Iterable[CouncilDocument]
) -> RunSummary | None:
    events = list(events)
    documents = list(documents)
    if not events:
        return None

    started_at = min(event.timestamp for event in events)
    latest_event = max(events, key=lambda event: event.timestamp)
    status = map_event_type_to_status(latest_event.event_type)
    completed_at = latest_event.timestamp if status in {"completed", "error"} else None
    discovered = max(_event_progress_total(event) for event in events)
    source_url = _latest_non_empty_metadata(events, "source_url")

    return RunSummary(
        run_id=run_id,
        status=status,
        started_at=started_at,
        completed_at=completed_at,
        documents_discovered=discovered,
        documents_fetched=len(documents),
        errors=sum(1 for event in events if event.event_type == "error"),
        latest_message=latest_event.message,
        source_url=source_url,
    )


def get_run_summary(state: SessionState, run_id: str | None) -> RunSummary | None:
    if run_id is None:
        return None
    events = state.get(DASHBOARD_RUN_EVENTS_KEY, {}).get(run_id, [])
    documents = state.get(DASHBOARD_RUN_DOCUMENTS_KEY, {}).get(run_id, [])
    return summarize_run(run_id, events, documents)


def load_retrieval_overview(state: SessionState, run_id: str | None = None) -> dict[str, Any]:
    active_request = get_active_request(state)
    current_run_id = get_current_run_id(state)
    if run_id is None and current_run_id is None and active_request is not None:
        return {
            "run_id": None,
            "status": active_request.status,
            "current_step": "queued for start",
            "current_source_url": active_request.source_url,
            "documents_discovered": 0,
            "documents_fetched": 0,
            "latest_error": None,
            "summary": None,
            "active_request": active_request,
        }

    run_id = run_id or get_selected_run_id(state)
    summary = get_run_summary(state, run_id)
    events = load_events(state, run_id)
    latest_error = next((event.message for event in reversed(events) if event.event_type == "error"), None)
    latest_event = events[-1] if events else None
    current_source_url = _latest_non_empty_metadata(events, "source_url")
    current_step = None
    if latest_event is not None:
        current_step = _event_metadata(latest_event).get("step_name")
    elif active_request is not None:
        current_step = "queued for start"
        current_source_url = active_request.source_url

    return {
        "run_id": run_id,
        "status": summary.status if summary is not None else active_request.status if active_request else "idle",
        "current_step": current_step,
        "current_source_url": current_source_url,
        "documents_discovered": summary.documents_discovered if summary is not None else 0,
        "documents_fetched": summary.documents_fetched if summary is not None else 0,
        "latest_error": latest_error,
        "summary": summary,
        "active_request": active_request,
    }


def load_retrieval_trace(state: SessionState, run_id: str | None = None) -> list[RetrievalTraceStep]:
    events = load_events(state, run_id)
    trace: list[RetrievalTraceStep] = []
    for event in events:
        metadata = _event_metadata(event)
        trace.append(
            RetrievalTraceStep(
                timestamp=event.timestamp,
                step_name=str(metadata.get("step_name") or "unknown"),
                event_type=event.event_type,
                message=event.message,
                source_url=metadata.get("source_url"),
                document_title=metadata.get("document_title"),
                document_type=metadata.get("document_type"),
                detail=metadata.get("detail"),
            )
        )
    return trace


def map_event_type_to_status(event_type: str) -> str:
    return {
        "started": "running",
        "progress": "running",
        "completed": "completed",
        "error": "error",
    }.get(event_type, "idle")


def _missing_state(state: SessionState) -> bool:
    required_keys = (
        DASHBOARD_RUN_EVENTS_KEY,
        DASHBOARD_RUN_DOCUMENTS_KEY,
        DASHBOARD_RUN_ORDER_KEY,
        DASHBOARD_CURRENT_RUN_KEY,
        DASHBOARD_SELECTED_RUN_KEY,
        DASHBOARD_RUN_SEQUENCE_KEY,
        DASHBOARD_SUMMARIES_GENERATED_KEY,
        DASHBOARD_PIPELINE_REQUESTS_KEY,
        DASHBOARD_REQUEST_SEQUENCE_KEY,
    )
    return any(key not in state for key in required_keys)


def _replace_dashboard_state(state: SessionState, force: bool = False) -> None:
    from src.dashboard.mock_data import create_seeded_dashboard_data

    seeded = create_seeded_dashboard_data()
    for key, value in seeded.items():
        if force or key not in state:
            state[key] = value


def _event_metadata(event: AgentEvent) -> dict[str, Any]:
    return dict(event.metadata or {})


def _event_progress_total(event: AgentEvent) -> int:
    metadata = _event_metadata(event)
    value = metadata.get("progress_total")
    return int(value) if value is not None else 0


def _latest_non_empty_metadata(events: Iterable[AgentEvent], key: str) -> str | None:
    for event in reversed(list(events)):
        value = _event_metadata(event).get(key)
        if value:
            return str(value)
    return None
