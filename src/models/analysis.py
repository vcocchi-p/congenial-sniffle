from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class MeetingSelection(BaseModel):
    """A meeting chosen for downstream voter-facing analysis."""

    retrieval_run_id: str
    meeting_id: int
    committee_name: str
    meeting_date: str
    analysis_mode: str
    priority_score: float
    reason_selected: str
    selected_at: datetime
    item_keys: list[str] = Field(default_factory=list)


class AnalysisRun(BaseModel):
    """Metadata for a single analysis execution over one meeting."""

    analysis_run_id: str
    retrieval_run_id: str
    meeting_id: int
    analysis_mode: str
    status: str
    model: str
    selected_reason: str = ""
    started_at: datetime
    completed_at: datetime | None = None
    error_message: str | None = None


class AgendaItemAnalysis(BaseModel):
    """Persisted voter-facing analysis for a single agenda item."""

    analysis_run_id: str
    retrieval_run_id: str
    meeting_id: int
    item_key: str
    item_number: str
    title: str
    plain_summary: str
    why_it_matters: str
    pros: list[str]
    cons: list[str]
    what_to_watch: str
    councillors_involved: list[str] = Field(default_factory=list)
    source_urls: list[str] = Field(default_factory=list)
    notify_voters: bool = True
    analysis_mode: str
    created_at: datetime


class MeetingAnalysisResult(BaseModel):
    """The output of an analysis run over a selected meeting."""

    selection: MeetingSelection
    run: AnalysisRun
    items: list[AgendaItemAnalysis]
