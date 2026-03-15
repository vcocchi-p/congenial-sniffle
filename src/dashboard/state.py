"""Helpers for dashboard session state, real retrieval runs, and derived views."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Iterable, MutableMapping, TypeVar
from urllib.parse import urlparse

from src.agents.pipeline import run_pipeline
from src.analysis.agent import analyse_meeting
from src.analysis.db import load_analysis_result_for_retrieval_run
from src.dashboard.constants import (
    DASHBOARD_CURRENT_RUN_KEY,
    DASHBOARD_PIPELINE_REQUESTS_KEY,
    DASHBOARD_REQUEST_SEQUENCE_KEY,
    DASHBOARD_RUN_BUNDLES_KEY,
    DASHBOARD_RUN_DOCUMENTS_KEY,
    DASHBOARD_RUN_EVENTS_KEY,
    DASHBOARD_RUN_ORDER_KEY,
    DASHBOARD_RUN_SEQUENCE_KEY,
    DASHBOARD_SELECTED_RUN_KEY,
    DASHBOARD_SUMMARIES_GENERATED_KEY,
    DEFAULT_ANALYSIS_MEETING_ID,
    DEFAULT_ANALYSIS_MODE,
    HISTORY_LIMIT,
    PIPELINE_STAGES,
    PLACEHOLDER_STAGE_MESSAGE,
    RESOURCE_SECTIONS,
    STAGE_LABELS,
)
from src.dashboard.simulation import next_run_id
from src.models.analysis import AgendaItemAnalysis, MeetingAnalysisResult
from src.models.documents import AgentEvent, MeetingDocument, RetrievalBundle
from src.retrieval.db import (
    get_latest_run_sequence,
    record_retrieval_event,
    record_retrieval_run_result,
    record_retrieval_run_started,
)

T = TypeVar("T")


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
    trigger_type: str


@dataclass(frozen=True)
class RetrievalTraceStep:
    timestamp: datetime
    step_name: str
    event_type: str
    message: str
    source_url: str | None
    document_url: str | None
    document_title: str | None
    document_type: str | None
    detail: str | None
    meeting_id: int | None
    committee: str | None


@dataclass(frozen=True)
class PipelineRequest:
    request_id: str
    run_id: str | None
    trigger_type: str
    source_url: str
    requested_at: datetime
    status: str
    message: str


SessionState = MutableMapping[str, Any]
PipelineRunner = Callable[..., Awaitable[RetrievalBundle] | RetrievalBundle]
AnalysisRunner = Callable[..., MeetingAnalysisResult]


@dataclass(frozen=True)
class AnalysisOverview:
    analysis_run_id: str
    retrieval_run_id: str
    meeting_id: int
    committee_name: str
    meeting_date: str
    analysis_mode: str
    status: str
    items_generated: int
    notify_voters: int
    selected_reason: str
    completed_at: datetime | None


def initialize_state(state: SessionState) -> None:
    if _missing_state(state):
        _replace_dashboard_state(state)


def reset_demo_state(state: SessionState) -> None:
    _replace_dashboard_state(state, force=True)


def load_runs(state: SessionState) -> list[RunSummary]:
    run_events = state.get(DASHBOARD_RUN_EVENTS_KEY, {})
    run_documents = state.get(DASHBOARD_RUN_DOCUMENTS_KEY, {})
    run_bundles = state.get(DASHBOARD_RUN_BUNDLES_KEY, {})
    summaries: list[RunSummary] = []
    for run_id in state.get(DASHBOARD_RUN_ORDER_KEY, []):
        events = run_events.get(run_id, [])
        documents = run_documents.get(run_id, [])
        bundle = run_bundles.get(run_id)
        summary = summarize_run(run_id, events, documents, bundle)
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


def load_documents(state: SessionState, run_id: str | None = None) -> list[MeetingDocument]:
    bundle = load_run_bundle(state, run_id)
    if bundle is not None:
        return list(bundle.documents)
    run_documents = state.get(DASHBOARD_RUN_DOCUMENTS_KEY, {})
    if run_id is not None:
        return list(run_documents.get(run_id, []))

    documents: list[MeetingDocument] = []
    for documents_for_run in run_documents.values():
        documents.extend(documents_for_run)
    return documents


def load_run_bundle(state: SessionState, run_id: str | None) -> RetrievalBundle | None:
    if run_id is None:
        return None
    return state.get(DASHBOARD_RUN_BUNDLES_KEY, {}).get(run_id)


def load_analysis_result(run_id: str | None) -> MeetingAnalysisResult | None:
    if run_id is None:
        return None
    return load_analysis_result_for_retrieval_run(run_id, analysis_mode=DEFAULT_ANALYSIS_MODE)


def load_analysis_items(run_id: str | None) -> list[AgendaItemAnalysis]:
    result = load_analysis_result(run_id)
    return list(result.items) if result is not None else []


def load_analysis_overview(run_id: str | None) -> AnalysisOverview | None:
    result = load_analysis_result(run_id)
    if result is None:
        return None
    return AnalysisOverview(
        analysis_run_id=result.run.analysis_run_id,
        retrieval_run_id=result.run.retrieval_run_id,
        meeting_id=result.selection.meeting_id,
        committee_name=result.selection.committee_name,
        meeting_date=result.selection.meeting_date,
        analysis_mode=result.selection.analysis_mode,
        status=result.run.status,
        items_generated=len(result.items),
        notify_voters=sum(1 for item in result.items if item.notify_voters),
        selected_reason=result.selection.reason_selected,
        completed_at=result.run.completed_at,
    )


def load_resource_counts(state: SessionState, run_id: str | None) -> dict[str, int]:
    bundle = load_run_bundle(state, run_id)
    if bundle is None:
        return {section: 0 for section in RESOURCE_SECTIONS}
    return {
        "committees": len(bundle.committees),
        "meetings": len(bundle.meetings),
        "documents": len(bundle.documents),
        "agenda_items": len(bundle.agenda_items),
        "decisions": len(bundle.decisions),
    }


def load_stage_snapshots(state: SessionState) -> list[StageSnapshot]:
    current_run_id = get_current_run_id(state)
    current_events = load_events(state, current_run_id) if current_run_id else []
    active_request = get_active_request(state)
    recent_runs = load_recent_runs(state)
    latest_run = recent_runs[0] if recent_runs else None
    latest_run_id = latest_run.run_id if latest_run is not None else None
    latest_analysis = load_analysis_overview(latest_run_id)

    snapshots: list[StageSnapshot] = []
    for stage in PIPELINE_STAGES:
        latest_stage_event = _latest_stage_event(current_events, stage)
        if latest_stage_event is not None:
            snapshots.append(
                StageSnapshot(
                    stage=stage,
                    label=STAGE_LABELS[stage],
                    status=map_event_type_to_status(latest_stage_event.event_type),
                    message=latest_stage_event.message,
                    last_updated=latest_stage_event.timestamp,
                )
            )
            continue
        if stage == "retrieval" and active_request is not None:
            snapshots.append(
                StageSnapshot(
                    stage=stage,
                    label=STAGE_LABELS[stage],
                    status=active_request.status,
                    message=active_request.message,
                    last_updated=active_request.requested_at,
                )
            )
            continue
        if stage == "analysis" and latest_analysis is not None:
            snapshots.append(
                StageSnapshot(
                    stage=stage,
                    label=STAGE_LABELS[stage],
                    status=latest_analysis.status,
                    message=(
                        f"Latest analysis for meeting {latest_analysis.meeting_id}: "
                        f"{latest_analysis.items_generated} voter briefs generated."
                    ),
                    last_updated=latest_analysis.completed_at,
                )
            )
            continue
        if stage == "analysis" and current_run_id is not None:
            snapshots.append(
                StageSnapshot(
                    stage=stage,
                    label=STAGE_LABELS[stage],
                    status="idle",
                    message="Awaiting analysis start for this run.",
                    last_updated=None,
                )
            )
            continue
        if stage == "analysis":
            snapshots.append(
                StageSnapshot(
                    stage=stage,
                    label=STAGE_LABELS[stage],
                    status="idle",
                    message="Analysis starts after retrieval completes.",
                    last_updated=None,
                )
            )
            continue
        if stage == "retrieval":
            message = "Awaiting manual retrieval start."
            last_updated = None
            if latest_run is not None:
                message = (
                    f"No active run. Last run {latest_run.run_id} "
                    f"finished {latest_run.status}."
                )
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
    current_analysis = load_analysis_result(current_run_id)
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
        "manual_requests": len(load_pipeline_requests(state)),
        "summaries_generated": (
            len(current_analysis.items)
            if current_analysis is not None
            else state.get(DASHBOARD_SUMMARIES_GENERATED_KEY, 0)
        ),
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
    for request in load_pipeline_requests(state):
        if request.status in {"queued", "running"}:
            return request
    return None


def start_retrieval_run(
    state: SessionState,
    source_url: str,
    *,
    pipeline_runner: PipelineRunner = run_pipeline,
    analysis_runner: AnalysisRunner = analyse_meeting,
    on_event: Callable[[AgentEvent], None] | None = None,
) -> PipelineRequest:
    initialize_state(state)
    normalized_url = _validate_source_url(source_url)
    run_sequence = max(
        int(state.get(DASHBOARD_RUN_SEQUENCE_KEY, 0)),
        get_latest_run_sequence(),
    ) + 1
    request_sequence = int(state.get(DASHBOARD_REQUEST_SEQUENCE_KEY, 0)) + 1
    run_id = next_run_id(run_sequence)
    requested_at = datetime.now(timezone.utc)

    request = PipelineRequest(
        request_id=f"request-{request_sequence:03d}",
        run_id=run_id,
        trigger_type="manual",
        source_url=normalized_url,
        requested_at=requested_at,
        status="running",
        message="Retrieval run is in progress.",
    )

    state[DASHBOARD_PIPELINE_REQUESTS_KEY].append(request)
    state[DASHBOARD_REQUEST_SEQUENCE_KEY] = request_sequence
    state[DASHBOARD_RUN_SEQUENCE_KEY] = run_sequence
    state[DASHBOARD_CURRENT_RUN_KEY] = run_id
    state[DASHBOARD_SELECTED_RUN_KEY] = run_id
    state[DASHBOARD_RUN_EVENTS_KEY][run_id] = []
    state[DASHBOARD_RUN_DOCUMENTS_KEY][run_id] = []
    state[DASHBOARD_RUN_BUNDLES_KEY][run_id] = RetrievalBundle(source_url=normalized_url)
    record_retrieval_run_started(
        run_id,
        source_url=normalized_url,
        trigger_type="manual",
        requested_at=requested_at,
    )
    analysis_started = False

    def capture(event: AgentEvent) -> None:
        normalized_event = _normalize_pipeline_event(event, run_id, normalized_url)
        state[DASHBOARD_RUN_EVENTS_KEY][run_id].append(normalized_event)
        record_retrieval_event(run_id, normalized_event)
        if on_event is not None:
            on_event(normalized_event)

    try:
        bundle = _run_pipeline_sync(
            pipeline_runner,
            run_id=run_id,
            source_url=normalized_url,
            trigger_type="manual",
            on_event=capture,
        )
        state[DASHBOARD_RUN_BUNDLES_KEY][run_id] = bundle
        state[DASHBOARD_RUN_DOCUMENTS_KEY][run_id] = list(bundle.documents)
        _append_run_once(state, run_id)
        summary = summarize_run(
            run_id,
            state[DASHBOARD_RUN_EVENTS_KEY][run_id],
            state[DASHBOARD_RUN_DOCUMENTS_KEY][run_id],
            bundle,
        )
        if summary is not None:
            record_retrieval_run_result(
                run_id,
                status=summary.status,
                completed_at=summary.completed_at,
                latest_message=summary.latest_message,
                documents_discovered=summary.documents_discovered,
                documents_fetched=summary.documents_fetched,
                bundle=bundle,
            )
        analysis_started = True
        analysis_result = _run_analysis_sync(
            analysis_runner,
            run_id=run_id,
            preferred_meeting_id=DEFAULT_ANALYSIS_MEETING_ID,
            analysis_mode=DEFAULT_ANALYSIS_MODE,
            on_event=capture,
        )
        state[DASHBOARD_SUMMARIES_GENERATED_KEY] = len(analysis_result.items)
        final_summary = summarize_run(
            run_id,
            state[DASHBOARD_RUN_EVENTS_KEY][run_id],
            state[DASHBOARD_RUN_DOCUMENTS_KEY][run_id],
            bundle,
        )
        if final_summary is not None:
            record_retrieval_run_result(
                run_id,
                status=final_summary.status,
                completed_at=final_summary.completed_at,
                latest_message=final_summary.latest_message,
                documents_discovered=final_summary.documents_discovered,
                documents_fetched=final_summary.documents_fetched,
                bundle=bundle,
            )
        request = PipelineRequest(
            request_id=request.request_id,
            run_id=run_id,
            trigger_type="manual",
            source_url=normalized_url,
            requested_at=requested_at,
            status="completed",
            message="Retrieval and analysis completed successfully.",
        )
    except Exception as exc:
        error_event = _build_dashboard_error_event(
            run_id,
            normalized_url,
            str(exc),
            stage="analysis" if analysis_started else "retrieval",
            agent_name="analysis" if analysis_started else "retriever",
            message=(
                "Analysis run failed before completion"
                if analysis_started
                else "Retrieval run failed before completion"
            ),
        )
        state[DASHBOARD_RUN_EVENTS_KEY][run_id].append(error_event)
        record_retrieval_event(run_id, error_event)
        if on_event is not None:
            on_event(error_event)
        _append_run_once(state, run_id)
        summary = summarize_run(
            run_id,
            state[DASHBOARD_RUN_EVENTS_KEY][run_id],
            state[DASHBOARD_RUN_DOCUMENTS_KEY][run_id],
            state[DASHBOARD_RUN_BUNDLES_KEY][run_id],
        )
        if summary is not None:
            record_retrieval_run_result(
                run_id,
                status="error",
                completed_at=summary.completed_at,
                latest_message=summary.latest_message,
                documents_discovered=summary.documents_discovered,
                documents_fetched=summary.documents_fetched,
                error_message=str(exc),
                bundle=state[DASHBOARD_RUN_BUNDLES_KEY][run_id],
            )
        request = PipelineRequest(
            request_id=request.request_id,
            run_id=run_id,
            trigger_type="manual",
            source_url=normalized_url,
            requested_at=requested_at,
            status="error",
            message=str(exc),
        )

    _replace_request(state, request)
    return request


def summarize_run(
    run_id: str,
    events: Iterable[AgentEvent],
    documents: Iterable[MeetingDocument],
    bundle: RetrievalBundle | None = None,
) -> RunSummary | None:
    events = list(events)
    documents = list(bundle.documents if bundle is not None else documents)
    if not events:
        return None

    started_at = min(event.timestamp for event in events)
    latest_event = events[-1]
    status = map_event_type_to_status(latest_event.event_type)
    completed_at = latest_event.timestamp if status in {"completed", "error"} else None
    discovered = max(len(documents), max(_event_progress_total(event) for event in events))
    source_url = _latest_non_empty_metadata(events, "source_url")
    trigger_type = _latest_non_empty_metadata(events, "trigger_type") or "manual"

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
        trigger_type=trigger_type,
    )


def get_run_summary(state: SessionState, run_id: str | None) -> RunSummary | None:
    if run_id is None:
        return None
    events = state.get(DASHBOARD_RUN_EVENTS_KEY, {}).get(run_id, [])
    documents = state.get(DASHBOARD_RUN_DOCUMENTS_KEY, {}).get(run_id, [])
    bundle = load_run_bundle(state, run_id)
    return summarize_run(run_id, events, documents, bundle)


def load_retrieval_overview(state: SessionState, run_id: str | None = None) -> dict[str, Any]:
    active_request = get_active_request(state)
    current_run_id = get_current_run_id(state)
    if run_id is None and current_run_id is None and active_request is not None:
        return {
            "run_id": None,
            "status": active_request.status,
            "current_step": "running",
            "current_source_url": active_request.source_url,
            "documents_discovered": 0,
            "documents_fetched": 0,
            "latest_error": None,
            "summary": None,
            "active_request": active_request,
        }

    run_id = run_id or get_selected_run_id(state)
    summary = get_run_summary(state, run_id)
    events = load_stage_events(state, "retrieval", run_id)
    latest_error = next(
        (event.message for event in reversed(events) if event.event_type == "error"),
        None,
    )
    latest_event = events[-1] if events else None
    current_source_url = _latest_non_empty_metadata(events, "source_url")
    current_step = None
    if latest_event is not None:
        current_step = _event_metadata(latest_event).get("step_name")
    elif active_request is not None:
        current_step = "running"
        current_source_url = active_request.source_url

    return {
        "run_id": run_id,
        "status": (
            map_event_type_to_status(latest_event.event_type)
            if latest_event is not None
            else active_request.status if active_request else "idle"
        ),
        "current_step": current_step,
        "current_source_url": current_source_url,
        "documents_discovered": (
            summary.documents_discovered if summary is not None else 0
        ),
        "documents_fetched": summary.documents_fetched if summary is not None else 0,
        "latest_error": latest_error,
        "summary": summary,
        "active_request": active_request,
    }


def load_retrieval_trace(
    state: SessionState,
    run_id: str | None = None,
) -> list[RetrievalTraceStep]:
    events = load_stage_events(state, "retrieval", run_id)
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
                document_url=metadata.get("document_url"),
                document_title=metadata.get("document_title"),
                document_type=metadata.get("document_type"),
                detail=metadata.get("detail"),
                meeting_id=_optional_int(metadata.get("meeting_id")),
                committee=_optional_str(metadata.get("committee")),
            )
        )
    return trace


def map_event_type_to_status(event_type: str) -> str:
    return {
        "started": "running",
        "progress": "running",
        "completed": "completed",
        "error": "error",
        "queued": "queued",
    }.get(event_type, "idle")


def load_stage_events(
    state: SessionState,
    stage: str,
    run_id: str | None = None,
) -> list[AgentEvent]:
    return [
        event
        for event in load_events(state, run_id)
        if _event_stage(event) == stage
    ]


def _missing_state(state: SessionState) -> bool:
    required_keys = (
        DASHBOARD_RUN_EVENTS_KEY,
        DASHBOARD_RUN_DOCUMENTS_KEY,
        DASHBOARD_RUN_BUNDLES_KEY,
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


def _validate_source_url(source_url: str) -> str:
    normalized_url = source_url.strip()
    parsed = urlparse(normalized_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Enter a valid http or https Westminster URL to start the pipeline.")
    if parsed.netloc != "committees.westminster.gov.uk":
        raise ValueError(
            "Only Westminster committee URLs are supported in the retrieval dashboard."
        )
    return normalized_url


def _run_pipeline_sync(
    pipeline_runner: PipelineRunner, **kwargs: Any
) -> RetrievalBundle:
    result = pipeline_runner(**kwargs)
    if inspect.isawaitable(result):
        return _run_awaitable(result)
    return result


def _run_awaitable(awaitable: Awaitable[T]) -> T:
    try:
        return asyncio.run(awaitable)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(awaitable)
        finally:
            loop.close()

def _run_analysis_sync(analysis_runner: AnalysisRunner, **kwargs: Any) -> MeetingAnalysisResult:
    return analysis_runner(**kwargs)


def _normalize_pipeline_event(event: AgentEvent, run_id: str, source_url: str) -> AgentEvent:
    metadata = dict(event.metadata or {})
    stage = str(metadata.get("stage") or _default_stage_for_agent(event.agent_name))
    metadata.setdefault("run_id", run_id)
    metadata.setdefault("stage", stage)
    metadata.setdefault("step_name", "progress")
    metadata.setdefault("source_url", source_url)
    metadata.setdefault("document_url", None)
    metadata.setdefault("document_title", None)
    metadata.setdefault("document_type", None)
    metadata.setdefault("progress_current", None)
    metadata.setdefault("progress_total", None)
    metadata.setdefault("detail", None)
    metadata.setdefault("trigger_type", "manual")
    return event.model_copy(
        update={
            "agent_name": event.agent_name or _default_agent_for_stage(stage),
            "metadata": metadata,
        }
    )


def _build_dashboard_error_event(
    run_id: str,
    source_url: str,
    detail: str,
    *,
    stage: str,
    agent_name: str,
    message: str,
) -> AgentEvent:
    return AgentEvent(
        agent_name=agent_name,
        event_type="error",
        message=message,
        timestamp=datetime.now(timezone.utc),
        metadata={
            "run_id": run_id,
            "stage": stage,
            "step_name": "error",
            "source_url": source_url,
            "document_url": None,
            "document_title": None,
            "document_type": None,
            "progress_current": None,
            "progress_total": None,
            "detail": detail,
            "trigger_type": "manual",
        },
    )


def _append_run_once(state: SessionState, run_id: str) -> None:
    if run_id not in state[DASHBOARD_RUN_ORDER_KEY]:
        state[DASHBOARD_RUN_ORDER_KEY].append(run_id)


def _replace_request(state: SessionState, updated: PipelineRequest) -> None:
    requests = list(state[DASHBOARD_PIPELINE_REQUESTS_KEY])
    for index, request in enumerate(requests):
        if request.request_id == updated.request_id:
            requests[index] = updated
            state[DASHBOARD_PIPELINE_REQUESTS_KEY] = requests
            return
    requests.append(updated)
    state[DASHBOARD_PIPELINE_REQUESTS_KEY] = requests


def _event_metadata(event: AgentEvent) -> dict[str, Any]:
    return dict(event.metadata or {})


def _event_stage(event: AgentEvent) -> str:
    metadata = _event_metadata(event)
    return str(metadata.get("stage") or _default_stage_for_agent(event.agent_name))


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


def _latest_stage_event(events: Iterable[AgentEvent], stage: str) -> AgentEvent | None:
    for event in reversed(list(events)):
        if _event_stage(event) == stage:
            return event
    return None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _default_stage_for_agent(agent_name: str) -> str:
    if agent_name == "analysis":
        return "analysis"
    return "retrieval"


def _default_agent_for_stage(stage: str) -> str:
    if stage == "analysis":
        return "analysis"
    return "retriever"
