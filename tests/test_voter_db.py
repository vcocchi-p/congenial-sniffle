"""Tests for voter SQLite persistence (src/voter/db.py)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

import src.retrieval.db as retrieval_db
import src.voter.db as db_module
from src.models.documents import AgendaItem, Committee, Meeting, RetrievalBundle
from src.retrieval.db import (
    init_retrieval_db,
    record_retrieval_run_result,
    record_retrieval_run_started,
)
from src.voter.db import (
    get_agenda_items,
    get_item_tallies_for_meeting,
    get_latest_run_id,
    get_meetings,
    get_user_votes,
    get_vote_tallies,
    init_db,
    register_user,
    submit_votes,
    user_exists,
)


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Redirect DB_PATH to a temporary file for every test."""
    test_db = tmp_path / "test_quorum.db"
    monkeypatch.setattr(db_module, "DB_PATH", test_db)
    monkeypatch.setattr(retrieval_db, "DB_PATH", test_db)
    init_db()
    init_retrieval_db()
    return test_db


def _sample_bundle() -> RetrievalBundle:
    return RetrievalBundle(
        source_url="https://committees.westminster.gov.uk/ieListMeetings.aspx?CId=130&Year=0",
        committees=[
            Committee(
                id=130,
                name="Cabinet",
                url="https://committees.westminster.gov.uk/mgCommitteeDetails.aspx?ID=130",
            )
        ],
        meetings=[
            Meeting(
                committee_id=130,
                committee_name="Cabinet",
                meeting_id=6718,
                date="23 Feb 2026 6.30 pm",
                url="https://committees.westminster.gov.uk/ieListDocuments.aspx?CId=130&MId=6718&Ver=4",
                is_upcoming=False,
            )
        ],
        agenda_items=[
            AgendaItem(
                meeting_id=6718,
                item_number="4",
                title="Budget",
                description="Annual budget report",
                decision_text="Approved budget",
                minutes_text="Members discussed finances.",
            )
        ],
    )


class TestRegisterUser:
    def test_new_user_returns_username(self):
        assert register_user("alice") == "alice"

    def test_new_user_is_persisted(self):
        register_user("alice")
        assert user_exists("alice")

    def test_unknown_user_does_not_exist(self):
        assert not user_exists("ghost")

    def test_duplicate_gets_suffix(self):
        register_user("alice")
        result = register_user("alice")
        assert result == "alice2"

    def test_duplicate_increments_suffix_until_free(self):
        register_user("alice")
        register_user("alice")   # → alice2
        result = register_user("alice")
        assert result == "alice3"

    def test_all_disambiguated_names_are_persisted(self):
        register_user("bob")
        register_user("bob")
        register_user("bob")
        assert user_exists("bob")
        assert user_exists("bob2")
        assert user_exists("bob3")


class TestSubmitVotes:
    def test_votes_are_saved(self):
        register_user("alice")
        count = submit_votes("alice", {
            "m1-1": {"vote": "for", "title": "Item 1"},
            "m1-2": {"vote": "against", "title": "Item 2"},
        })
        assert count == 2

    def test_resubmit_updates_vote(self):
        register_user("alice")
        submit_votes("alice", {"m1-1": {"vote": "for", "title": "Item 1"}})
        submit_votes("alice", {"m1-1": {"vote": "against", "title": "Item 1"}})
        votes = get_user_votes("alice")
        assert votes["m1-1"] == "against"

    def test_empty_votes_returns_zero(self):
        register_user("alice")
        assert submit_votes("alice", {}) == 0


class TestGetVoteTallies:
    def test_empty_tallies_for_unknown_item(self):
        tallies = get_vote_tallies("unknown-key")
        assert tallies == {"for": 0, "against": 0, "abstain": 0}

    def test_tallies_count_correctly(self):
        for name in ["alice", "bob", "carol"]:
            register_user(name)
        submit_votes("alice", {"m1-1": {"vote": "for", "title": "Item 1"}})
        submit_votes("bob", {"m1-1": {"vote": "for", "title": "Item 1"}})
        submit_votes("carol", {"m1-1": {"vote": "against", "title": "Item 1"}})

        tallies = get_vote_tallies("m1-1")
        assert tallies["for"] == 2
        assert tallies["against"] == 1
        assert tallies["abstain"] == 0

    def test_abstain_counted(self):
        register_user("alice")
        submit_votes("alice", {"m1-1": {"vote": "abstain", "title": "Item 1"}})
        assert get_vote_tallies("m1-1")["abstain"] == 1


class TestGetItemTalliesForMeeting:
    def test_returns_items_for_meeting(self):
        register_user("alice")
        submit_votes("alice", {
            "m1-1": {"vote": "for", "title": "Motion A"},
            "m1-2": {"vote": "against", "title": "Motion B"},
        })
        items = get_item_tallies_for_meeting("m1")
        keys = [i["item_key"] for i in items]
        assert "m1-1" in keys
        assert "m1-2" in keys

    def test_empty_for_unknown_meeting(self):
        assert get_item_tallies_for_meeting("unknown") == []

    def test_sorted_by_total_votes_descending(self):
        for name in ["alice", "bob", "carol"]:
            register_user(name)
        submit_votes("alice", {"m1-1": {"vote": "for", "title": "A"}})
        submit_votes("bob", {"m1-1": {"vote": "for", "title": "A"}})
        submit_votes("carol", {"m1-2": {"vote": "against", "title": "B"}})
        items = get_item_tallies_for_meeting("m1")
        assert items[0]["item_key"] == "m1-1"  # 2 votes > 1 vote


class TestGetUserVotes:
    def test_returns_votes_for_user(self):
        register_user("alice")
        submit_votes("alice", {"m1-1": {"vote": "for", "title": "Item 1"}})
        assert get_user_votes("alice") == {"m1-1": "for"}

    def test_returns_empty_for_user_with_no_votes(self):
        register_user("alice")
        assert get_user_votes("alice") == {}

    def test_users_votes_are_isolated(self):
        register_user("alice")
        register_user("bob")
        submit_votes("alice", {"m1-1": {"vote": "for", "title": "Item 1"}})
        submit_votes("bob", {"m1-1": {"vote": "against", "title": "Item 1"}})
        assert get_user_votes("alice") == {"m1-1": "for"}
        assert get_user_votes("bob") == {"m1-1": "against"}


class TestPipelineBackedContent:
    def test_fresh_database_returns_no_latest_run(self):
        assert get_latest_run_id() is None

    def test_returns_latest_completed_run_id(self):
        requested_at = datetime(2026, 3, 15, 16, 0, tzinfo=timezone.utc)
        record_retrieval_run_started(
            "run-001",
            source_url=_sample_bundle().source_url,
            trigger_type="manual",
            requested_at=requested_at,
        )
        record_retrieval_run_result(
            "run-001",
            status="completed",
            completed_at=requested_at,
            latest_message="Completed",
            documents_discovered=0,
            documents_fetched=0,
            bundle=_sample_bundle(),
        )

        assert get_latest_run_id() == "run-001"

    def test_loads_meetings_and_agenda_items_from_current_retrieval_schema(self):
        requested_at = datetime(2026, 3, 15, 16, 0, tzinfo=timezone.utc)
        bundle = _sample_bundle()
        record_retrieval_run_started(
            "run-001",
            source_url=bundle.source_url,
            trigger_type="manual",
            requested_at=requested_at,
        )
        record_retrieval_run_result(
            "run-001",
            status="completed",
            completed_at=requested_at,
            latest_message="Completed",
            documents_discovered=0,
            documents_fetched=0,
            bundle=bundle,
        )

        meetings = get_meetings("run-001")
        agenda_items = get_agenda_items("run-001")

        assert meetings[0]["meeting_id"] == 6718
        assert meetings[0]["committee_name"] == "Cabinet"
        assert agenda_items[0]["meeting_id"] == 6718
        assert agenda_items[0]["title"] == "Budget"
