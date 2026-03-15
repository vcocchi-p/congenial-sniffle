"""Pipeline orchestrator: fetches committees → meetings → documents → decisions."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from urllib.parse import parse_qs, urlparse

from src.agents.retriever import (
    emit_event,
    fetch_all_committees,
    fetch_attendance,
    fetch_decision,
    fetch_meeting_detail,
    fetch_meetings,
)
from src.models.documents import (
    AgendaItem,
    AgentEvent,
    Committee,
    DecisionDetail,
    Meeting,
    MeetingDocument,
    RetrievalBundle,
)

# Key committees voters are most likely to care about
PRIORITY_COMMITTEE_IDS = {
    175,  # Council
    130,  # Cabinet
    565,  # Strategic Planning Committee
    566,  # Planning Sub-Committee (1)
    567,  # Planning Sub-Committee (2)
    545,  # Climate Action, Environment and Highways
    546,  # Housing and Regeneration
    549,  # Overview and Scrutiny
    547,  # Vulnerable Adults, Health and Communities
    548,  # Young People, Learning and Employment
}


def resolve_committee_ids_from_source_url(
    source_url: str | None, default_ids: set[int] | None = None
) -> set[int]:
    """Infer committee IDs from a Westminster URL when possible."""
    if not source_url:
        return set(default_ids or PRIORITY_COMMITTEE_IDS)

    parsed = urlparse(source_url)
    if parsed.netloc and parsed.netloc != "committees.westminster.gov.uk":
        raise ValueError("Only Westminster committee URLs are supported for retrieval runs.")

    params = parse_qs(parsed.query)
    committee_id = None
    if "mgCommitteeDetails" in parsed.path:
        committee_id = _parse_int_param(params, "ID")
    elif "ieListMeetings" in parsed.path or "ieListDocuments" in parsed.path:
        committee_id = _parse_int_param(params, "CId")

    if committee_id is not None:
        return {committee_id}

    return set(default_ids or PRIORITY_COMMITTEE_IDS)


async def run_pipeline(
    run_id: str,
    source_url: str | None = None,
    trigger_type: str = "manual",
    committee_ids: set[int] | None = None,
    max_meetings_per_committee: int = 5,
    on_event: Callable[[AgentEvent], None] | None = None,
) -> RetrievalBundle:
    """Run the full retrieval pipeline.

    Args:
        run_id: Identifier for the retrieval run.
        source_url: Optional operator-supplied Westminster URL used to scope the run.
        committee_ids: Set of committee IDs to fetch. Defaults to PRIORITY_COMMITTEE_IDS.
        max_meetings_per_committee: Max recent meetings to fetch per committee.
        on_event: Callback for monitoring events (fed to the dashboard).

    Returns:
        RetrievalBundle with normalized resources for downstream analysis.
    """
    target_ids = committee_ids or resolve_committee_ids_from_source_url(source_url)
    manual_source = source_url or "https://committees.westminster.gov.uk/mgListCommittees.aspx"

    def event(
        msg: str,
        event_type: str = "progress",
        *,
        step_name: str,
        source: str | None = None,
        progress_current: int | None = None,
        progress_total: int | None = None,
        detail: str | None = None,
        document: MeetingDocument | None = None,
        **meta,
    ):
        e = emit_event(
            "retriever",
            event_type,
            msg,
            run_id=run_id,
            stage="retrieval",
            step_name=step_name,
            source_url=source or manual_source,
            document_url=document.url if document else None,
            document_title=document.title if document else None,
            document_type=document.doc_type.value if document else None,
            progress_current=progress_current,
            progress_total=progress_total,
            detail=detail,
            trigger_type=trigger_type,
            **meta,
        )
        if on_event:
            on_event(e)

    event(
        "Starting retrieval pipeline",
        event_type="started",
        step_name="source discovery",
        source=manual_source,
        detail="Resolving the retrieval scope and fetching committee listings.",
    )

    # 1. Fetch all committees and filter to targets
    all_committees = await fetch_all_committees()
    committees = [c for c in all_committees if c.id in target_ids]
    event(
        f"Found {len(committees)} target committees out of {len(all_committees)} total",
        step_name="source discovery",
        source=manual_source,
        progress_current=len(committees),
        progress_total=len(all_committees),
        detail="Filtered the Westminster committee listing to the requested retrieval scope.",
    )

    # 2. Fetch meetings for each committee (concurrently)
    all_meetings: list[Meeting] = []

    async def _get_meetings(committee: Committee):
        try:
            meetings = await fetch_meetings(committee)
        except Exception as exc:
            event(
                f"{committee.name}: failed to fetch meetings",
                event_type="error",
                step_name="meeting discovery",
                source=committee.url,
                detail=str(exc),
                committee=committee.name,
            )
            return []

        trimmed = meetings[:max_meetings_per_committee]
        event(
            f"{committee.name}: {len(trimmed)} meetings",
            step_name="meeting discovery",
            source=committee.url,
            progress_current=len(trimmed),
            progress_total=max_meetings_per_committee,
            detail="Fetched the recent meetings for this committee.",
            committee=committee.name,
        )
        return trimmed

    meeting_batches = await asyncio.gather(
        *[_get_meetings(c) for c in committees], return_exceptions=True
    )
    for batch in meeting_batches:
        if isinstance(batch, Exception):
            event(
                "Unexpected error while collecting meetings",
                event_type="error",
                step_name="meeting discovery",
                source=manual_source,
                detail=str(batch),
            )
            continue
        all_meetings.extend(batch)

    event(
        f"Fetched {len(all_meetings)} meetings total",
        step_name="meeting discovery",
        source=manual_source,
        progress_current=len(all_meetings),
        progress_total=len(all_meetings),
        detail="Completed committee-level meeting discovery.",
    )

    # 3. Fetch documents, agenda items, and attendance for each meeting (concurrently)
    all_docs: list[MeetingDocument] = []
    all_items: list[AgendaItem] = []
    enriched_meetings: list[Meeting] = []

    async def _get_detail(meeting: Meeting):
        try:
            docs, items = await fetch_meeting_detail(meeting)
        except Exception as exc:
            event(
                f"Failed to fetch meeting detail for {meeting.date}",
                event_type="error",
                step_name="meeting detail",
                source=meeting.url,
                detail=str(exc),
                meeting_id=meeting.meeting_id,
            )
            return meeting, [], []

        try:
            attendees = await fetch_attendance(meeting)
        except Exception as exc:
            attendees = []
            event(
                f"Attendance unavailable for {meeting.date}",
                event_type="error",
                step_name="attendance fetch",
                source=meeting.url,
                detail=str(exc),
                meeting_id=meeting.meeting_id,
            )

        enriched = meeting.model_copy(update={"attendees": attendees})
        event(
            f"{meeting.committee_name}: {len(docs)} documents, {len(items)} agenda items",
            step_name="meeting detail",
            source=meeting.url,
            progress_current=len(docs),
            progress_total=len(docs),
            detail="Fetched documents, agenda items, and attendance for the meeting.",
            meeting_id=meeting.meeting_id,
        )
        for document in docs:
            event(
                f"Discovered {document.title}",
                step_name="document discovery",
                source=meeting.url,
                detail="Captured a meeting document for this run.",
                document=document,
                meeting_id=meeting.meeting_id,
            )
        return enriched, docs, items

    detail_batches = await asyncio.gather(
        *[_get_detail(m) for m in all_meetings], return_exceptions=True
    )
    for batch in detail_batches:
        if isinstance(batch, Exception):
            event(
                "Unexpected error while collecting meeting detail",
                event_type="error",
                step_name="meeting detail",
                source=manual_source,
                detail=str(batch),
            )
            continue
        meeting, docs, items = batch
        enriched_meetings.append(meeting)
        all_docs.extend(docs)
        all_items.extend(items)

    event(
        f"Fetched {len(all_docs)} documents, {len(all_items)} agenda items",
        step_name="document discovery",
        source=manual_source,
        progress_current=len(all_docs),
        progress_total=len(all_docs),
        detail="Completed document and agenda extraction across meetings.",
    )

    # 4. Fetch decision details for agenda items that have decision URLs
    all_decisions: list[DecisionDetail] = []
    items_with_decisions = [i for i in all_items if i.decision_url]

    async def _get_decision(item: AgendaItem):
        try:
            detail = await fetch_decision(item.decision_url)
        except Exception as exc:
            event(
                f"Failed to fetch decision detail for {item.title}",
                event_type="error",
                step_name="decision fetch",
                source=item.decision_url,
                detail=str(exc),
            )
            return None

        decision = DecisionDetail(agenda_title=item.title, **detail)
        event(
            f"Fetched decision detail for {item.title}",
            step_name="decision fetch",
            source=item.decision_url,
            progress_current=1,
            progress_total=1,
            detail="Fetched and normalized the linked decision detail page.",
        )
        return decision

    decision_batches = await asyncio.gather(
        *[_get_decision(i) for i in items_with_decisions], return_exceptions=True
    )
    for result in decision_batches:
        if isinstance(result, Exception):
            event(
                "Unexpected error while collecting decision detail",
                event_type="error",
                step_name="decision fetch",
                source=manual_source,
                detail=str(result),
            )
            continue
        if result is not None:
            all_decisions.append(result)

    event(
        f"Pipeline complete: {len(all_decisions)} decisions retrieved",
        event_type="completed",
        step_name="completed",
        source=manual_source,
        progress_current=len(all_docs),
        progress_total=len(all_docs),
        detail="Stored the retrieval bundle for downstream dashboard and analysis use.",
    )

    return RetrievalBundle(
        source_url=manual_source,
        committees=committees,
        meetings=enriched_meetings or all_meetings,
        documents=all_docs,
        agenda_items=all_items,
        decisions=all_decisions,
    )


def _parse_int_param(params: dict[str, list[str]], name: str) -> int | None:
    values = params.get(name, [])
    if not values:
        return None
    try:
        return int(values[0])
    except ValueError:
        return None
