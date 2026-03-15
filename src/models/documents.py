from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class DocumentType(str, Enum):
    MINUTES = "minutes"
    AGENDA = "agenda"
    DECISION = "decision"
    VOTING_RECORD = "voting_record"


class Committee(BaseModel):
    """A Westminster Council committee."""

    id: int  # CId parameter on the site
    name: str
    url: str


class Councillor(BaseModel):
    """A Westminster councillor."""

    name: str
    role: str = ""  # e.g. "Chair", "Cabinet Member for Finance"
    profile_url: str | None = None


class Meeting(BaseModel):
    """A single committee meeting."""

    committee_id: int
    committee_name: str
    meeting_id: int  # MId parameter on the site
    date: str  # e.g. "31 Mar 2025 6.30 pm"
    url: str
    is_upcoming: bool = False
    attendees: list[Councillor] = []


class MeetingDocument(BaseModel):
    """A downloadable document from a meeting (PDF agenda, minutes, etc.)."""

    meeting_id: int
    title: str
    doc_type: DocumentType
    url: str


class AgendaItem(BaseModel):
    """A single agenda item from a meeting, with full inline content."""

    meeting_id: int
    item_number: str
    title: str
    description: str = ""
    decision_text: str = ""
    minutes_text: str = ""
    decision_url: str | None = None


class CouncilDocument(BaseModel):
    """A raw document fetched from Westminster Council."""

    url: str
    title: str
    doc_type: DocumentType
    fetched_at: datetime
    raw_content: str
    committee: str | None = None
    meeting_id: int | None = None


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
