"""Rules for deciding whether an agenda item should be surfaced to voters."""

from __future__ import annotations

import re

from src.models.documents import AgendaItem

_PROCEDURAL_TITLE_PATTERNS = (
    r"^welcome$",
    r"^apologies",
    r"^declarations? of interest$",
    r"^minutes$",
    r"^urgent business$",
    r"^questions? by members",
    r"^questions? from the public",
    r"^petitions?$",
    r"^any other business$",
)


def is_voter_relevant_agenda_item(item: AgendaItem) -> bool:
    """Return whether an agenda item is substantive enough for voter briefing."""
    title = item.title.strip().lower()
    if not title:
        return False
    return not any(re.match(pattern, title) for pattern in _PROCEDURAL_TITLE_PATTERNS)
