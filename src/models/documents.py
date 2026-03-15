from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class DocumentType(str, Enum):
    MINUTES = "minutes"
    AGENDA = "agenda"
    DECISION = "decision"
    VOTING_RECORD = "voting_record"


class CouncilDocument(BaseModel):
    """A raw document fetched from Westminster Council."""

    url: str
    title: str
    doc_type: DocumentType
    fetched_at: datetime
    raw_content: str
    committee: str | None = None


class VoterSummary(BaseModel):
    """Plain-language summary of a council decision for voters."""

    document_id: str
    title: str
    plain_summary: str
    key_points: list[str]
    councillors_involved: list[str]
    decision_date: datetime | None = None


class VotingIntention(BaseModel):
    """A voter's response to a council decision."""

    decision_id: str
    voter_id: str
    support: bool
    comment: str | None = None
    submitted_at: datetime


class AgentEvent(BaseModel):
    """An event emitted by an agent for the monitoring dashboard."""

    agent_name: str
    event_type: str  # "started", "progress", "completed", "error"
    message: str
    timestamp: datetime
    metadata: dict | None = None
