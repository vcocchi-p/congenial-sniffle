from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.models.analysis import MeetingSelection
from src.models.documents import AgendaItem, Meeting
from src.voter.presentation import is_demo_mode_active, is_demo_upcoming


@pytest.fixture
def demo_selection() -> MeetingSelection:
    return MeetingSelection(
        retrieval_run_id="run-008",
        meeting_id=6718,
        committee_name="Cabinet",
        meeting_date="23 Feb 2026 6.30 pm",
        analysis_mode="demo_upcoming",
        priority_score=999.0,
        reason_selected="Pinned for demo",
        selected_at=datetime(2026, 3, 15, 16, 0, tzinfo=timezone.utc),
        item_keys=["6718-4", "6718-5"],
    )


def test_demo_selection_limits_upcoming_items(monkeypatch: pytest.MonkeyPatch, demo_selection):
    monkeypatch.setattr(
        "src.voter.presentation.load_latest_meeting_selection",
        lambda: demo_selection,
    )
    meeting = Meeting(
        committee_id=130,
        committee_name="Cabinet",
        meeting_id=6718,
        date="23 Feb 2026 6.30 pm",
        url="https://example.com/meeting/6718",
        is_upcoming=False,
    )
    included_item = AgendaItem(meeting_id=6718, item_number="4", title="Budget")
    excluded_item = AgendaItem(meeting_id=6718, item_number="9", title="Other")
    unrelated_item = AgendaItem(meeting_id=7000, item_number="1", title="Different Meeting")

    assert is_demo_mode_active() is True
    assert is_demo_upcoming(included_item, meeting) is True
    assert is_demo_upcoming(excluded_item, meeting) is False
    assert is_demo_upcoming(unrelated_item, None) is False


def test_procedural_demo_items_never_count_as_upcoming(
    monkeypatch: pytest.MonkeyPatch, demo_selection
):
    monkeypatch.setattr(
        "src.voter.presentation.load_latest_meeting_selection",
        lambda: demo_selection,
    )
    meeting = Meeting(
        committee_id=130,
        committee_name="Cabinet",
        meeting_id=6718,
        date="23 Feb 2026 6.30 pm",
        url="https://example.com/meeting/6718",
        is_upcoming=False,
    )
    procedural_item = AgendaItem(meeting_id=6718, item_number="1", title="Welcome")

    assert is_demo_upcoming(procedural_item, meeting) is False


def test_without_demo_selection_real_upcoming_meetings_stay_upcoming(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr("src.voter.presentation.load_latest_meeting_selection", lambda: None)
    meeting = Meeting(
        committee_id=130,
        committee_name="Cabinet",
        meeting_id=7001,
        date="31 Mar 2026 6.30 pm",
        url="https://example.com/meeting/7001",
        is_upcoming=True,
    )
    item = AgendaItem(meeting_id=7001, item_number="1", title="Live Upcoming Item")

    assert is_demo_mode_active() is False
    assert is_demo_upcoming(item, meeting) is True
