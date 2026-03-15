"""Presentation helpers for the voter-facing app."""

from __future__ import annotations

from src.analysis.db import load_latest_meeting_selection
from src.analysis.relevance import is_voter_relevant_agenda_item
from src.models.documents import AgendaItem, Meeting


def is_demo_mode_active() -> bool:
    """Return whether a demo upcoming meeting selection is active."""
    selection = load_latest_meeting_selection()
    return selection is not None and selection.analysis_mode.endswith("upcoming")


def is_demo_upcoming(item: AgendaItem, meeting: Meeting | None) -> bool:
    """Treat only the selected demo meeting as upcoming for the voter experience."""
    selection = load_latest_meeting_selection()
    if selection is not None and selection.analysis_mode.endswith("upcoming"):
        item_key = f"{item.meeting_id}-{item.item_number}"
        return (
            item.meeting_id == selection.meeting_id
            and item_key in set(selection.item_keys)
            and is_voter_relevant_agenda_item(item)
        )
    return meeting is not None and meeting.is_upcoming
