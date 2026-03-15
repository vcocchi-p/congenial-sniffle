"""Tests for the retriever agent parsers."""

from src.agents.retriever import (
    emit_event,
    parse_agenda_items,
    parse_committees,
    parse_decision_detail,
    parse_meeting_documents,
    parse_meetings,
)
from src.models.documents import DocumentType


class TestParseCommittees:
    def test_extracts_committees(self, committees_html):
        committees = parse_committees(committees_html)
        assert len(committees) == 3

    def test_committee_ids(self, committees_html):
        committees = parse_committees(committees_html)
        ids = {c.id for c in committees}
        assert ids == {130, 175, 565}

    def test_committee_names(self, committees_html):
        committees = parse_committees(committees_html)
        names = {c.name for c in committees}
        assert "Cabinet" in names
        assert "Council" in names

    def test_committee_urls_absolute(self, committees_html):
        committees = parse_committees(committees_html)
        for c in committees:
            assert c.url.startswith("https://")

    def test_ignores_non_committee_links(self, committees_html):
        committees = parse_committees(committees_html)
        urls = [c.url for c in committees]
        assert not any("other-page" in u for u in urls)


class TestParseMeetings:
    def test_extracts_meetings(self, meetings_html):
        meetings = parse_meetings(meetings_html, committee_id=130, committee_name="Cabinet")
        assert len(meetings) == 2

    def test_meeting_ids(self, meetings_html):
        meetings = parse_meetings(meetings_html, committee_id=130, committee_name="Cabinet")
        assert meetings[0].meeting_id == 6439
        assert meetings[1].meeting_id == 6438

    def test_meeting_dates(self, meetings_html):
        meetings = parse_meetings(meetings_html, committee_id=130, committee_name="Cabinet")
        assert "31 Mar 2025" in meetings[0].date

    def test_meeting_committee_info(self, meetings_html):
        meetings = parse_meetings(meetings_html, committee_id=130, committee_name="Cabinet")
        assert meetings[0].committee_id == 130
        assert meetings[0].committee_name == "Cabinet"


class TestParseMeetingDocuments:
    def test_extracts_pdfs(self, meeting_detail_html):
        docs = parse_meeting_documents(meeting_detail_html, meeting_id=6439)
        assert len(docs) == 3

    def test_classifies_minutes(self, meeting_detail_html):
        docs = parse_meeting_documents(meeting_detail_html, meeting_id=6439)
        minutes = [d for d in docs if d.doc_type == DocumentType.MINUTES]
        assert len(minutes) == 1

    def test_classifies_decisions(self, meeting_detail_html):
        docs = parse_meeting_documents(meeting_detail_html, meeting_id=6439)
        decisions = [d for d in docs if d.doc_type == DocumentType.DECISION]
        assert len(decisions) == 1

    def test_classifies_agenda(self, meeting_detail_html):
        docs = parse_meeting_documents(meeting_detail_html, meeting_id=6439)
        agendas = [d for d in docs if d.doc_type == DocumentType.AGENDA]
        assert len(agendas) == 1


class TestParseAgendaItems:
    def test_extracts_items(self, meeting_detail_html):
        items = parse_agenda_items(meeting_detail_html, meeting_id=6439)
        assert len(items) == 2

    def test_item_titles(self, meeting_detail_html):
        items = parse_agenda_items(meeting_detail_html, meeting_id=6439)
        titles = {i.title for i in items}
        assert "Pimlico District Heating" in titles
        assert "Homelessness Strategy 2025-2030" in titles

    def test_decision_urls(self, meeting_detail_html):
        items = parse_agenda_items(meeting_detail_html, meeting_id=6439)
        for item in items:
            assert item.decision_url is not None
            assert "ieDecisionDetails" in item.decision_url


class TestParseDecisionDetail:
    def test_extracts_title(self, decision_detail_html):
        result = parse_decision_detail(decision_detail_html)
        assert "Pimlico District Heating" in result["title"]

    def test_extracts_decision(self, decision_detail_html):
        result = parse_decision_detail(decision_detail_html)
        assert "£1.2m" in result["decision"]

    def test_extracts_reasons(self, decision_detail_html):
        result = parse_decision_detail(decision_detail_html)
        assert "infrastructure" in result["reasons"]

    def test_extracts_made_by(self, decision_detail_html):
        result = parse_decision_detail(decision_detail_html)
        assert result["made_by"] == "Cabinet"


class TestEmitEvent:
    def test_basic_event(self):
        event = emit_event("retriever", "started", "Begin fetch", url="https://example.com")
        assert event.agent_name == "retriever"
        assert event.metadata == {"url": "https://example.com"}

    def test_no_metadata(self):
        event = emit_event("retriever", "completed", "Done")
        assert event.metadata is None
