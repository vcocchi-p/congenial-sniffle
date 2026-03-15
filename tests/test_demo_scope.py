from __future__ import annotations

from src.demo_scope import (
    FEATURED_MEETING_ID,
    get_featured_meeting_id,
    is_voter_relevant_item,
    is_voter_relevant_title,
)


def test_procedural_titles_are_excluded():
    assert is_voter_relevant_title("Welcome") is False
    assert is_voter_relevant_title("Declarations of Interest") is False
    assert is_voter_relevant_title("Minutes") is False
    assert is_voter_relevant_title("Agenda") is False


def test_substantive_titles_are_included():
    assert is_voter_relevant_title("Business and Financial Planning 2026/27 to 2028/29") is True
    assert is_voter_relevant_title("Affordable Housing Pipeline - Strategic Outline Case") is True


def test_voter_relevant_item_accepts_dict_rows():
    assert is_voter_relevant_item({"title": "Sustainable Transport Strategy 2026 - 2036"}) is True
    assert is_voter_relevant_item({"title": "Minutes"}) is False


def test_featured_meeting_is_detected_when_present():
    meetings = [
        {"meeting_id": 1000, "committee_name": "Other"},
        {"meeting_id": FEATURED_MEETING_ID, "committee_name": "Cabinet"},
    ]

    assert get_featured_meeting_id(meetings) == FEATURED_MEETING_ID


def test_featured_meeting_returns_none_when_absent():
    meetings = [{"meeting_id": 1000, "committee_name": "Other"}]

    assert get_featured_meeting_id(meetings) is None
