"""Tests for the retrieval pipeline orchestrator."""

import asyncio

from src.agents import pipeline
from src.models.documents import (
    AgendaItem,
    Committee,
    Councillor,
    DocumentType,
    Meeting,
    MeetingDocument,
)


def test_resolve_committee_ids_from_committee_url():
    committee_ids = pipeline.resolve_committee_ids_from_source_url(
        "https://committees.westminster.gov.uk/mgCommitteeDetails.aspx?ID=130"
    )

    assert committee_ids == {130}


def test_resolve_committee_ids_from_meeting_url():
    committee_ids = pipeline.resolve_committee_ids_from_source_url(
        "https://committees.westminster.gov.uk/ieListMeetings.aspx?CId=565&Year=0"
    )

    assert committee_ids == {565}


def test_run_pipeline_returns_bundle_and_structured_events(monkeypatch):
    events = []

    async def fake_fetch_all_committees():
        return [
            Committee(
                id=130,
                name="Cabinet",
                url="https://committees.westminster.gov.uk/mgCommitteeDetails.aspx?ID=130",
            )
        ]

    async def fake_fetch_meetings(committee):
        return [
            Meeting(
                committee_id=committee.id,
                committee_name=committee.name,
                meeting_id=9001,
                date="15 Mar 2026 6.30 pm",
                url="https://committees.westminster.gov.uk/ieListDocuments.aspx?CId=130&MId=9001",
            )
        ]

    async def fake_fetch_meeting_detail(meeting):
        return (
            [
                MeetingDocument(
                    meeting_id=meeting.meeting_id,
                    title="Budget Report",
                    doc_type=DocumentType.AGENDA,
                    url="https://committees.westminster.gov.uk/documents/9001/budget.pdf",
                )
            ],
            [
                AgendaItem(
                    meeting_id=meeting.meeting_id,
                    item_number="4",
                    title="Budget",
                    description="Approve the annual budget",
                    decision_url="https://committees.westminster.gov.uk/ieDecisionDetails.aspx?AIId=99",
                )
            ],
        )

    async def fake_fetch_attendance(meeting):
        return [Councillor(name="Adam Hug", role="Chair")]

    async def fake_fetch_decision(url):
        return {
            "title": "Decision - Budget",
            "decision": "Approved",
            "reasons": "Budget agreed",
            "made_by": "Cabinet",
            "date": "15 Mar 2026",
        }

    monkeypatch.setattr(pipeline, "fetch_all_committees", fake_fetch_all_committees)
    monkeypatch.setattr(pipeline, "fetch_meetings", fake_fetch_meetings)
    monkeypatch.setattr(pipeline, "fetch_meeting_detail", fake_fetch_meeting_detail)
    monkeypatch.setattr(pipeline, "fetch_attendance", fake_fetch_attendance)
    monkeypatch.setattr(pipeline, "fetch_decision", fake_fetch_decision)

    bundle = asyncio.run(
        pipeline.run_pipeline(
            run_id="run-100",
            source_url="https://committees.westminster.gov.uk/ieListMeetings.aspx?CId=130&Year=0",
            max_meetings_per_committee=1,
            on_event=events.append,
        )
    )

    assert len(bundle.committees) == 1
    assert len(bundle.meetings) == 1
    assert len(bundle.documents) == 1
    assert len(bundle.agenda_items) == 1
    assert len(bundle.decisions) == 1
    assert bundle.meetings[0].attendees[0].name == "Adam Hug"
    assert bundle.decisions[0].agenda_title == "Budget"

    assert events[0].agent_name == "retriever"
    assert events[0].metadata["run_id"] == "run-100"
    assert events[0].metadata["stage"] == "retrieval"
    assert events[-1].event_type == "completed"
