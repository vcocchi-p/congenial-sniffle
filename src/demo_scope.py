"""Shared demo-scope helpers for voter and councillor views."""

from __future__ import annotations

import re
from typing import Any

FEATURED_MEETING_ID = 6718

_PROCEDURAL_TITLE_PATTERNS = (
    r"^welcome$",
    r"^apologies",
    r"^declarations? of interest$",
    r"^minutes$",
    r"^agenda$",
    r"^urgent business$",
    r"^questions? by members",
    r"^questions? from the public",
    r"^petitions?$",
    r"^any other business$",
)


def _get_value(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key)


def is_voter_relevant_title(title: str) -> bool:
    """Return whether an agenda-item title is substantive enough for voting."""
    normalized = title.strip().lower()
    if not normalized:
        return False
    return not any(re.match(pattern, normalized) for pattern in _PROCEDURAL_TITLE_PATTERNS)


def is_voter_relevant_item(item: Any) -> bool:
    """Return whether an agenda item or DB row should appear in civic voting views."""
    return is_voter_relevant_title(str(_get_value(item, "title") or ""))


def get_featured_meeting_id(meetings: list[Any]) -> int | None:
    """Return the featured demo meeting if it is present in the current data set."""
    for meeting in meetings:
        if _get_value(meeting, "meeting_id") == FEATURED_MEETING_ID:
            return FEATURED_MEETING_ID
    return None
