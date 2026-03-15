"""Tests for retrieval SQLite persistence."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

import src.retrieval.db as retrieval_db
from src.models.documents import (
    AgendaItem,
    AgentEvent,
    Committee,
    Councillor,
    DecisionDetail,
    DocumentType,
    Meeting,
    MeetingDocument,
    RetrievalBundle,
)
from src.retrieval.db import (
    get_latest_run_id,
    init_retrieval_db,
    list_retrieval_runs,
    load_analysis_inputs,
    load_latest_retrieval_bundle,
    load_retrieval_bundle,
    load_retrieval_events,
    record_retrieval_event,
    record_retrieval_run_result,
    record_retrieval_run_started,
)


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    test_db = tmp_path / "test_quorum.db"
    monkeypatch.setattr(retrieval_db, "DB_PATH", test_db)
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
                meeting_id=9001,
                date="15 Mar 2026 6.30 pm",
                url="https://committees.westminster.gov.uk/ieListDocuments.aspx?CId=130&MId=9001",
                is_upcoming=True,
                attendees=[Councillor(name="Adam Hug", role="Chair")],
            )
        ],
        documents=[
            MeetingDocument(
                meeting_id=9001,
                title="Budget Report",
                doc_type=DocumentType.AGENDA,
                url="https://committees.westminster.gov.uk/documents/9001/budget.pdf",
                fetched_at=datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc),
            )
        ],
        agenda_items=[
            AgendaItem(
                meeting_id=9001,
                item_number="4",
                title="Budget",
                description="Approve the annual budget",
                decision_text="Approved the annual budget",
                minutes_text="Members debated the budget.",
                decision_url="https://committees.westminster.gov.uk/ieDecisionDetails.aspx?AIId=99",
            )
        ],
        decisions=[
            DecisionDetail(
                agenda_title="Budget",
                title="Decision - Budget",
                decision="Approved",
                reasons="Budget agreed",
                made_by="Cabinet",
                date="15 Mar 2026",
            )
        ],
    )


def test_run_event_and_bundle_round_trip():
    requested_at = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)
    bundle = _sample_bundle()
    event = AgentEvent(
        agent_name="retriever",
        event_type="completed",
        message="Pipeline complete",
        timestamp=requested_at,
        metadata={"run_id": "run-100", "stage": "retrieval", "step_name": "completed"},
    )

    record_retrieval_run_started(
        "run-100",
        source_url=bundle.source_url,
        trigger_type="manual",
        requested_at=requested_at,
    )
    record_retrieval_event("run-100", event)
    record_retrieval_run_result(
        "run-100",
        status="completed",
        completed_at=requested_at,
        latest_message="Pipeline complete",
        documents_discovered=1,
        documents_fetched=1,
        bundle=bundle,
    )

    runs = list_retrieval_runs()
    events = load_retrieval_events("run-100")
    stored_bundle = load_retrieval_bundle("run-100")

    assert runs[0]["run_id"] == "run-100"
    assert runs[0]["status"] == "completed"
    assert events[0].message == "Pipeline complete"
    assert stored_bundle is not None
    assert stored_bundle.meetings[0].attendees[0].name == "Adam Hug"
    assert stored_bundle.documents[0].fetched_at == requested_at
    assert stored_bundle.agenda_items[0].title == "Budget"


def test_latest_bundle_and_analysis_inputs_use_newest_run():
    first_at = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)
    second_at = datetime(2026, 3, 15, 13, 0, tzinfo=timezone.utc)
    first_bundle = _sample_bundle()
    second_bundle = _sample_bundle().model_copy(
        update={
            "meetings": [
                _sample_bundle().meetings[0].model_copy(update={"meeting_id": 9002})
            ],
            "documents": [
                _sample_bundle().documents[0].model_copy(update={"meeting_id": 9002})
            ],
            "agenda_items": [
                _sample_bundle().agenda_items[0].model_copy(
                    update={"meeting_id": 9002, "item_number": "5", "title": "Housing"}
                )
            ],
        }
    )

    record_retrieval_run_started(
        "run-100",
        source_url=first_bundle.source_url,
        trigger_type="manual",
        requested_at=first_at,
    )
    record_retrieval_run_result(
        "run-100",
        status="completed",
        completed_at=first_at,
        latest_message="First run complete",
        documents_discovered=1,
        documents_fetched=1,
        bundle=first_bundle,
    )
    record_retrieval_run_started(
        "run-101",
        source_url=second_bundle.source_url,
        trigger_type="manual",
        requested_at=second_at,
    )
    record_retrieval_run_result(
        "run-101",
        status="completed",
        completed_at=second_at,
        latest_message="Second run complete",
        documents_discovered=1,
        documents_fetched=1,
        bundle=second_bundle,
    )

    latest_bundle = load_latest_retrieval_bundle()
    analysis_inputs = load_analysis_inputs()

    assert get_latest_run_id() == "run-101"
    assert latest_bundle is not None
    assert latest_bundle.agenda_items[0].title == "Housing"
    assert analysis_inputs[0].run_id == "run-101"
    assert analysis_inputs[0].item_key == "9002-5"
    assert analysis_inputs[0].is_upcoming is True
