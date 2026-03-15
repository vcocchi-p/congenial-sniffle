from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

import src.analysis.db as analysis_db
import src.retrieval.db as retrieval_db
from src.analysis.agent import analyse_meeting, detect_important_meeting
from src.analysis.db import init_analysis_db, load_item_analysis
from src.models.documents import AgendaItem, Committee, Meeting, RetrievalBundle
from src.retrieval.db import (
    init_retrieval_db,
    record_retrieval_run_result,
    record_retrieval_run_started,
)


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    test_db = tmp_path / "analysis_agent.db"
    monkeypatch.setattr(retrieval_db, "DB_PATH", test_db)
    monkeypatch.setattr(analysis_db, "DB_PATH", test_db)
    init_retrieval_db()
    init_analysis_db()
    return test_db


def _bundle_for_demo() -> RetrievalBundle:
    return RetrievalBundle(
        source_url="https://committees.westminster.gov.uk/ieDocHome.aspx?Categories=",
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
                title="Business and Financial Planning 2026/27 to 2028/29",
                description="To consider the annual budget report.",
                decision_text="Budget approved after debate.",
                minutes_text="Members discussed tax impacts.",
                decision_url="https://committees.westminster.gov.uk/ieDecisionDetails.aspx?AIId=22914",
            ),
            AgendaItem(
                meeting_id=6718,
                item_number="11",
                title="Sustainable Transport Strategy 2026 - 2036",
                description="To consider approval of the Sustainable Transport Strategy.",
            ),
        ],
    )


class _FakeCompletions:
    def create(self, **_: object):
        payload = {
            "summary": "Residents are being briefed on an upcoming council item.",
            "why_it_matters": "This could affect local services and long-term priorities.",
            "pros": ["Could improve council planning."],
            "cons": ["Could reduce flexibility later."],
            "what_to_watch": "Watch for amendments before the final vote.",
            "councillors": ["Leader of the Council"],
            "notify_voters": True,
        }
        return type(
            "Response",
            (),
            {
                "choices": [
                    type(
                        "Choice",
                        (),
                        {"message": type("Message", (), {"content": json.dumps(payload)})()},
                    )
                ]
            },
        )()


class _FakeClient:
    def __init__(self):
        self.chat = type("Chat", (), {"completions": _FakeCompletions()})()


def test_detect_important_meeting_can_pin_demo_meeting():
    requested_at = datetime(2026, 3, 15, 16, 0, tzinfo=timezone.utc)
    record_retrieval_run_started(
        "run-005",
        source_url=_bundle_for_demo().source_url,
        trigger_type="manual",
        requested_at=requested_at,
    )
    record_retrieval_run_result(
        "run-005",
        status="completed",
        completed_at=requested_at,
        latest_message="Retrieval complete",
        documents_discovered=0,
        documents_fetched=0,
        bundle=_bundle_for_demo(),
    )

    selection = detect_important_meeting(
        run_id="run-005",
        preferred_meeting_id=6718,
        selected_at=requested_at,
    )

    assert selection.meeting_id == 6718
    assert selection.analysis_mode == "demo_upcoming"
    assert selection.priority_score == 999.0


def test_detect_important_meeting_falls_back_when_demo_meeting_missing():
    requested_at = datetime(2026, 3, 15, 16, 0, tzinfo=timezone.utc)
    bundle = _bundle_for_demo().model_copy(
        update={
            "meetings": [
                _bundle_for_demo().meetings[0].model_copy(update={"meeting_id": 9001})
            ],
            "agenda_items": [
                item.model_copy(update={"meeting_id": 9001})
                for item in _bundle_for_demo().agenda_items
            ],
        }
    )
    record_retrieval_run_started(
        "run-006",
        source_url=bundle.source_url,
        trigger_type="manual",
        requested_at=requested_at,
    )
    record_retrieval_run_result(
        "run-006",
        status="completed",
        completed_at=requested_at,
        latest_message="Retrieval complete",
        documents_discovered=0,
        documents_fetched=0,
        bundle=bundle,
    )

    selection = detect_important_meeting(
        run_id="run-006",
        preferred_meeting_id=6718,
        selected_at=requested_at,
    )

    assert selection.meeting_id == 9001
    assert selection.priority_score > 0


def test_analyse_meeting_persists_demo_upcoming_output():
    requested_at = datetime(2026, 3, 15, 16, 0, tzinfo=timezone.utc)
    record_retrieval_run_started(
        "run-005",
        source_url=_bundle_for_demo().source_url,
        trigger_type="manual",
        requested_at=requested_at,
    )
    record_retrieval_run_result(
        "run-005",
        status="completed",
        completed_at=requested_at,
        latest_message="Retrieval complete",
        documents_discovered=0,
        documents_fetched=0,
        bundle=_bundle_for_demo(),
    )

    result = analyse_meeting(
        run_id="run-005",
        meeting_id=6718,
        client=_FakeClient(),
        started_at=requested_at,
    )

    stored_item = load_item_analysis("6718-4", analysis_mode="demo_upcoming")

    assert result.run.status == "completed"
    assert len(result.items) == 2
    assert stored_item is not None
    assert stored_item.analysis_mode == "demo_upcoming"
    assert stored_item.source_urls[0].endswith("MId=6718&Ver=4")
