"""Pipeline orchestrator: fetches committees → meetings → documents → decisions."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from src.agents.retriever import (
    emit_event,
    fetch_all_committees,
    fetch_decision,
    fetch_meeting_detail,
    fetch_meetings,
)
from src.models.documents import AgendaItem, AgentEvent, Committee, Meeting, MeetingDocument

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


async def run_pipeline(
    committee_ids: set[int] | None = None,
    max_meetings_per_committee: int = 5,
    on_event: Callable[[AgentEvent], None] | None = None,
) -> dict:
    """Run the full retrieval pipeline.

    Args:
        committee_ids: Set of committee IDs to fetch. Defaults to PRIORITY_COMMITTEE_IDS.
        max_meetings_per_committee: Max recent meetings to fetch per committee.
        on_event: Callback for monitoring events (fed to the dashboard).

    Returns:
        Dict with keys: committees, meetings, documents, agenda_items, decisions.
    """
    target_ids = committee_ids or PRIORITY_COMMITTEE_IDS

    def event(msg: str, event_type: str = "progress", **meta):
        e = emit_event("pipeline", event_type, msg, **meta)
        if on_event:
            on_event(e)

    event("Starting pipeline", event_type="started")

    # 1. Fetch all committees and filter to targets
    all_committees = await fetch_all_committees()
    committees = [c for c in all_committees if c.id in target_ids]
    event(f"Found {len(committees)} target committees out of {len(all_committees)} total")

    # 2. Fetch meetings for each committee (concurrently)
    all_meetings: list[Meeting] = []

    async def _get_meetings(committee: Committee):
        meetings = await fetch_meetings(committee)
        trimmed = meetings[:max_meetings_per_committee]
        event(f"{committee.name}: {len(trimmed)} meetings", committee=committee.name)
        return trimmed

    meeting_batches = await asyncio.gather(
        *[_get_meetings(c) for c in committees], return_exceptions=True
    )
    for batch in meeting_batches:
        if isinstance(batch, list):
            all_meetings.extend(batch)

    event(f"Fetched {len(all_meetings)} meetings total")

    # 3. Fetch documents + agenda items for each meeting (concurrently)
    all_docs: list[MeetingDocument] = []
    all_items: list[AgendaItem] = []

    async def _get_detail(meeting: Meeting):
        docs, items = await fetch_meeting_detail(meeting)
        return docs, items

    detail_batches = await asyncio.gather(
        *[_get_detail(m) for m in all_meetings], return_exceptions=True
    )
    for batch in detail_batches:
        if isinstance(batch, tuple):
            docs, items = batch
            all_docs.extend(docs)
            all_items.extend(items)

    event(f"Fetched {len(all_docs)} documents, {len(all_items)} agenda items")

    # 4. Fetch decision details for agenda items that have decision URLs
    all_decisions: list[dict] = []
    items_with_decisions = [i for i in all_items if i.decision_url]

    async def _get_decision(item: AgendaItem):
        detail = await fetch_decision(item.decision_url)
        detail["agenda_title"] = item.title
        return detail

    decision_batches = await asyncio.gather(
        *[_get_decision(i) for i in items_with_decisions], return_exceptions=True
    )
    for result in decision_batches:
        if isinstance(result, dict):
            all_decisions.append(result)

    event(
        f"Pipeline complete: {len(all_decisions)} decisions retrieved",
        event_type="completed",
    )

    return {
        "committees": committees,
        "meetings": all_meetings,
        "documents": all_docs,
        "agenda_items": all_items,
        "decisions": all_decisions,
    }
