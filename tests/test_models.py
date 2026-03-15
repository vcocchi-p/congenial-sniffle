"""Tests for data models."""

from datetime import datetime, timezone

from src.models.documents import (
    AgentEvent,
    CouncilDocument,
    DocumentType,
    VoterSummary,
    VotingIntention,
)


def test_council_document_creation(sample_document):
    assert sample_document.doc_type == DocumentType.MINUTES
    assert sample_document.committee is None
    assert "Planning Committee" in sample_document.title


def test_voter_summary_creation():
    summary = VoterSummary(
        document_id="doc-1",
        title="Housing Decision",
        plain_summary="The council approved new housing.",
        key_points=["New homes on Elm Street", "3 councillors voted yes"],
        councillors_involved=["Smith", "Jones", "Patel"],
    )
    assert len(summary.key_points) == 2
    assert summary.decision_date is None


def test_voting_intention_creation():
    intention = VotingIntention(
        decision_id="dec-1",
        voter_id="voter-42",
        support=True,
        comment="I support more housing",
        submitted_at=datetime(2026, 3, 15, tzinfo=timezone.utc),
    )
    assert intention.support is True


def test_agent_event_creation():
    event = AgentEvent(
        agent_name="retriever",
        event_type="started",
        message="Fetching documents",
        timestamp=datetime.now(timezone.utc),
    )
    assert event.metadata is None
