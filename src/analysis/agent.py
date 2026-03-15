"""Meeting selection and voter-facing agenda analysis on top of retrieval outputs."""

from __future__ import annotations

import os
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone

from src.agents.retriever import emit_event
from src.analysis.db import (
    get_latest_analysis_run_sequence,
    record_analysis_run_result,
    record_analysis_run_started,
    record_meeting_selection,
)
from src.models.analysis import (
    AgendaItemAnalysis,
    AnalysisRun,
    MeetingAnalysisResult,
    MeetingSelection,
)
from src.models.documents import AgendaItem, AgentEvent
from src.parser.summariser import generate_voter_brief
from src.retrieval.db import AnalysisInput, load_analysis_inputs

IMPORTANT_KEYWORDS = (
    "budget",
    "housing",
    "transport",
    "planning",
    "tax",
    "finance",
    "capital",
    "investment",
    "highways",
    "sustainable",
)

DEMO_UPCOMING_MODE = "demo_upcoming"
AnalysisEventCallback = Callable[[AgentEvent], None]


def detect_important_meeting(
    *,
    run_id: str | None = None,
    preferred_meeting_id: int | None = None,
    analysis_mode: str = DEMO_UPCOMING_MODE,
    selected_at: datetime | None = None,
) -> MeetingSelection:
    """Select the most relevant meeting from persisted retrieval outputs."""
    inputs = load_analysis_inputs(run_id)
    if not inputs:
        raise ValueError("No retrieval inputs are available for analysis.")

    grouped_inputs = _group_inputs_by_meeting(inputs)
    selected_at = selected_at or datetime.now(timezone.utc)

    if preferred_meeting_id is not None:
        if preferred_meeting_id in grouped_inputs:
            selection = _build_forced_selection(
                grouped_inputs[preferred_meeting_id],
                analysis_mode=analysis_mode,
                selected_at=selected_at,
            )
        else:
            selection = _score_and_select_meeting(
                grouped_inputs,
                analysis_mode=analysis_mode,
                selected_at=selected_at,
            )
    else:
        selection = _score_and_select_meeting(
            grouped_inputs,
            analysis_mode=analysis_mode,
            selected_at=selected_at,
        )

    record_meeting_selection(selection)
    return selection


def analyse_meeting(
    *,
    meeting_id: int | None = None,
    run_id: str | None = None,
    preferred_meeting_id: int | None = None,
    analysis_mode: str = DEMO_UPCOMING_MODE,
    client=None,
    model: str = "gpt-4o",
    started_at: datetime | None = None,
    on_event: AnalysisEventCallback | None = None,
) -> MeetingAnalysisResult:
    """Generate and persist voter-facing analysis for one selected meeting."""
    _ensure_analysis_credentials(client)
    started_at = started_at or datetime.now(timezone.utc)
    selection = detect_important_meeting(
        run_id=run_id,
        preferred_meeting_id=meeting_id or preferred_meeting_id,
        analysis_mode=analysis_mode,
        selected_at=started_at,
    )

    analysis_run = AnalysisRun(
        analysis_run_id=f"analysis-run-{get_latest_analysis_run_sequence() + 1:03d}",
        retrieval_run_id=selection.retrieval_run_id,
        meeting_id=selection.meeting_id,
        analysis_mode=selection.analysis_mode,
        status="running",
        model=model,
        selected_reason=selection.reason_selected,
        started_at=started_at,
    )
    record_analysis_run_started(analysis_run)

    inputs = [
        item
        for item in load_analysis_inputs(selection.retrieval_run_id)
        if item.meeting is not None and item.meeting.meeting_id == selection.meeting_id
    ]
    source_url = inputs[0].meeting.url if inputs and inputs[0].meeting is not None else None

    _emit_analysis_event(
        on_event,
        event_type="started",
        message=(
            f"Selected {selection.committee_name} meeting {selection.meeting_id} "
            "for voter analysis"
        ),
        run_id=selection.retrieval_run_id,
        analysis_run_id=analysis_run.analysis_run_id,
        analysis_mode=selection.analysis_mode,
        step_name="meeting selection",
        source_url=source_url,
        detail=selection.reason_selected,
        progress_current=0,
        progress_total=len(inputs),
        meeting_id=selection.meeting_id,
        committee=selection.committee_name,
    )

    try:
        items = []
        ordered_inputs = sorted(
            inputs,
            key=lambda item: _item_sort_key(item.agenda_item.item_number),
        )
        for index, input_item in enumerate(ordered_inputs, start=1):
            item_analysis = _analyse_input(
                input_item,
                analysis_run=analysis_run,
                analysis_mode=selection.analysis_mode,
                client=client,
                model=model,
                created_at=started_at,
            )
            items.append(item_analysis)
            _emit_analysis_event(
                on_event,
                event_type="progress",
                message=f"Analysed item {item_analysis.item_number}: {item_analysis.title}",
                run_id=selection.retrieval_run_id,
                analysis_run_id=analysis_run.analysis_run_id,
                analysis_mode=selection.analysis_mode,
                step_name="agenda item analysis",
                source_url=source_url,
                document_url=item_analysis.source_urls[0] if item_analysis.source_urls else None,
                document_title=item_analysis.title,
                detail="Generated a voter-facing brief for this agenda item.",
                progress_current=index,
                progress_total=len(ordered_inputs),
                meeting_id=selection.meeting_id,
                committee=selection.committee_name,
                item_key=item_analysis.item_key,
            )
    except Exception as exc:
        _emit_analysis_event(
            on_event,
            event_type="error",
            message=f"Analysis failed for meeting {selection.meeting_id}",
            run_id=selection.retrieval_run_id,
            analysis_run_id=analysis_run.analysis_run_id,
            analysis_mode=selection.analysis_mode,
            step_name="error",
            source_url=source_url,
            detail=str(exc),
            progress_current=None,
            progress_total=len(inputs),
            meeting_id=selection.meeting_id,
            committee=selection.committee_name,
        )
        record_analysis_run_result(
            analysis_run.analysis_run_id,
            status="failed",
            completed_at=datetime.now(timezone.utc),
            items=[],
            error_message=str(exc),
        )
        raise

    completed_at = datetime.now(timezone.utc)
    record_analysis_run_result(
        analysis_run.analysis_run_id,
        status="completed",
        completed_at=completed_at,
        items=items,
    )
    _emit_analysis_event(
        on_event,
        event_type="completed",
        message=f"Generated {len(items)} voter briefs for meeting {selection.meeting_id}",
        run_id=selection.retrieval_run_id,
        analysis_run_id=analysis_run.analysis_run_id,
        analysis_mode=selection.analysis_mode,
        step_name="completed",
        source_url=source_url,
        detail="Persisted voter-facing analysis outputs for the selected meeting.",
        progress_current=len(items),
        progress_total=len(items),
        meeting_id=selection.meeting_id,
        committee=selection.committee_name,
    )
    final_run = analysis_run.model_copy(
        update={"status": "completed", "completed_at": completed_at}
    )
    return MeetingAnalysisResult(selection=selection, run=final_run, items=items)


def _analyse_input(
    input_item: AnalysisInput,
    *,
    analysis_run: AnalysisRun,
    analysis_mode: str,
    client,
    model: str,
    created_at: datetime,
) -> AgendaItemAnalysis:
    sanitized_item, is_upcoming = _prepare_item_for_analysis(input_item, analysis_mode)
    result = generate_voter_brief(
        sanitized_item,
        is_upcoming=is_upcoming,
        client=client,
        model=model,
    )
    return AgendaItemAnalysis(
        analysis_run_id=analysis_run.analysis_run_id,
        retrieval_run_id=analysis_run.retrieval_run_id,
        meeting_id=sanitized_item.meeting_id,
        item_key=input_item.item_key,
        item_number=sanitized_item.item_number,
        title=sanitized_item.title,
        plain_summary=result.get("summary", ""),
        why_it_matters=result.get("why_it_matters", ""),
        pros=result.get("pros", []),
        cons=result.get("cons", []),
        what_to_watch=result.get("what_to_watch", ""),
        councillors_involved=result.get("councillors", []),
        source_urls=_build_source_urls(input_item),
        notify_voters=bool(result.get("notify_voters", True)),
        analysis_mode=analysis_mode,
        created_at=created_at,
    )


def _prepare_item_for_analysis(
    input_item: AnalysisInput,
    analysis_mode: str,
) -> tuple[AgendaItem, bool]:
    is_upcoming = analysis_mode.endswith("upcoming")
    if not is_upcoming:
        return input_item.agenda_item, input_item.is_upcoming

    sanitized = input_item.agenda_item.model_copy(
        update={
            "decision_text": "",
            "minutes_text": "",
        }
    )
    return sanitized, True


def _build_source_urls(input_item: AnalysisInput) -> list[str]:
    source_urls: list[str] = []
    if input_item.meeting is not None:
        source_urls.append(input_item.meeting.url)
    if input_item.agenda_item.decision_url:
        source_urls.append(input_item.agenda_item.decision_url)
    seen = set()
    deduped = []
    for url in source_urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def _group_inputs_by_meeting(
    inputs: list[AnalysisInput],
) -> dict[int, list[AnalysisInput]]:
    grouped: dict[int, list[AnalysisInput]] = defaultdict(list)
    for item in inputs:
        if item.meeting is None:
            continue
        grouped[item.meeting.meeting_id].append(item)
    return dict(grouped)


def _build_forced_selection(
    meeting_inputs: list[AnalysisInput],
    *,
    analysis_mode: str,
    selected_at: datetime,
) -> MeetingSelection:
    meeting = meeting_inputs[0].meeting
    assert meeting is not None
    return MeetingSelection(
        retrieval_run_id=meeting_inputs[0].run_id,
        meeting_id=meeting.meeting_id,
        committee_name=meeting.committee_name,
        meeting_date=meeting.date,
        analysis_mode=analysis_mode,
        priority_score=999.0,
        reason_selected=(
            "Pinned this meeting for the voter demo so the Analysis agent can prepare "
            "an upcoming-meeting briefing from persisted retrieval data."
        ),
        selected_at=selected_at,
        item_keys=[item.item_key for item in meeting_inputs],
    )


def _score_and_select_meeting(
    grouped_inputs: dict[int, list[AnalysisInput]],
    *,
    analysis_mode: str,
    selected_at: datetime,
) -> MeetingSelection:
    best_inputs = max(grouped_inputs.values(), key=_meeting_priority_score)
    meeting = best_inputs[0].meeting
    assert meeting is not None
    score = _meeting_priority_score(best_inputs)
    keyword_matches = _keyword_hits(best_inputs)
    upcoming_text = "upcoming" if any(item.is_upcoming for item in best_inputs) else "high-impact"
    keyword_summary = ", ".join(keyword_matches[:3]) if keyword_matches else "general civic impact"
    reason = (
        f"Selected {meeting.committee_name} because it scored highest across committee "
        f"importance, item volume, and resident-relevant topics ({keyword_summary}). "
        f"Treat this as an {upcoming_text} meeting."
    )
    return MeetingSelection(
        retrieval_run_id=best_inputs[0].run_id,
        meeting_id=meeting.meeting_id,
        committee_name=meeting.committee_name,
        meeting_date=meeting.date,
        analysis_mode=analysis_mode,
        priority_score=score,
        reason_selected=reason,
        selected_at=selected_at,
        item_keys=[item.item_key for item in best_inputs],
    )


def _meeting_priority_score(meeting_inputs: list[AnalysisInput]) -> float:
    meeting = meeting_inputs[0].meeting
    assert meeting is not None
    score = float(len(meeting_inputs) * 10)
    committee_name = meeting.committee_name.lower()
    if "cabinet" in committee_name:
        score += 100.0
    elif "council" in committee_name:
        score += 50.0
    if any(item.is_upcoming for item in meeting_inputs):
        score += 30.0
    score += float(len(_keyword_hits(meeting_inputs)) * 12)
    return score


def _keyword_hits(meeting_inputs: list[AnalysisInput]) -> list[str]:
    combined_text = " ".join(
        f"{item.agenda_item.title} {item.agenda_item.description}".lower()
        for item in meeting_inputs
    )
    return [keyword for keyword in IMPORTANT_KEYWORDS if keyword in combined_text]


def _item_sort_key(item_number: str) -> tuple[int, str]:
    try:
        return (int(item_number), item_number)
    except ValueError:
        return (10_000, item_number)


def _emit_analysis_event(
    on_event: AnalysisEventCallback | None,
    *,
    event_type: str,
    message: str,
    run_id: str,
    analysis_run_id: str,
    analysis_mode: str,
    step_name: str,
    source_url: str | None,
    detail: str | None,
    progress_current: int | None,
    progress_total: int | None,
    meeting_id: int,
    committee: str,
    document_url: str | None = None,
    document_title: str | None = None,
    item_key: str | None = None,
) -> None:
    if on_event is None:
        return
    on_event(
        emit_event(
            "analysis",
            event_type,
            message,
            run_id=run_id,
            stage="analysis",
            step_name=step_name,
            source_url=source_url,
            document_url=document_url,
            document_title=document_title,
            document_type="agenda_item" if document_title else None,
            progress_current=progress_current,
            progress_total=progress_total,
            detail=detail,
            trigger_type="manual",
            analysis_run_id=analysis_run_id,
            analysis_mode=analysis_mode,
            meeting_id=meeting_id,
            committee=committee,
            item_key=item_key,
        )
    )


def _ensure_analysis_credentials(client) -> None:
    if client is not None:
        return
    if os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_KEY"):
        return
    raise ValueError(
        "Analysis requires OPENAI_API_KEY or OPENAI_KEY to be set for the dashboard process."
    )
