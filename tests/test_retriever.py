"""Tests for the retriever agent parsers."""

import asyncio

from src.agents.retriever import (
    emit_event,
    extract_councillors_from_text,
    fetch_meeting_detail,
    parse_agenda_items,
    parse_attendance,
    parse_committees,
    parse_decision_detail,
    parse_meeting_documents,
    parse_meetings,
)
from src.models.documents import DocumentType, Meeting


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
        assert len(meetings) == 3

    def test_meeting_ids(self, meetings_html):
        meetings = parse_meetings(meetings_html, committee_id=130, committee_name="Cabinet")
        assert meetings[0].meeting_id == 7115
        assert meetings[1].meeting_id == 6439
        assert meetings[2].meeting_id == 6438

    def test_meeting_dates(self, meetings_html):
        meetings = parse_meetings(meetings_html, committee_id=130, committee_name="Cabinet")
        assert "1 Jun 2099" in meetings[0].date

    def test_meeting_committee_info(self, meetings_html):
        meetings = parse_meetings(meetings_html, committee_id=130, committee_name="Cabinet")
        assert meetings[0].committee_id == 130
        assert meetings[0].committee_name == "Cabinet"

    def test_upcoming_flag(self, meetings_html):
        meetings = parse_meetings(meetings_html, committee_id=130, committee_name="Cabinet")
        assert meetings[0].is_upcoming is True   # Jun 2099
        assert meetings[1].is_upcoming is False   # Mar 2020
        assert meetings[2].is_upcoming is False   # Feb 2020


class TestParseMeetingDocuments:
    def test_extracts_pdfs(self, meeting_detail_html):
        docs = parse_meeting_documents(meeting_detail_html, meeting_id=6439)
        assert len(docs) == 3

    def test_document_urls_absolute(self, meeting_detail_html):
        docs = parse_meeting_documents(meeting_detail_html, meeting_id=6439)
        assert all(doc.url.startswith("https://committees.westminster.gov.uk/") for doc in docs)

    def test_strips_pdf_suffix_from_titles(self, meeting_detail_html):
        docs = parse_meeting_documents(meeting_detail_html, meeting_id=6439)
        assert docs[0].title == "Agenda frontsheet"
        assert docs[1].title == "Printed minutes"
        assert docs[2].title == "Printed decisions"

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

    def test_fetch_meeting_detail_sets_fetched_at(self, monkeypatch, meeting_detail_html):
        async def fake_fetch_page(url):
            return meeting_detail_html

        monkeypatch.setattr("src.agents.retriever.fetch_page", fake_fetch_page)
        meeting = Meeting(
            committee_id=130,
            committee_name="Cabinet",
            meeting_id=6439,
            date="31 Mar 2020 6.30 pm",
            url="https://committees.westminster.gov.uk/ieListDocuments.aspx?CId=130&MId=6439",
        )

        docs, _ = asyncio.run(fetch_meeting_detail(meeting))

        assert docs
        assert all(doc.fetched_at is not None for doc in docs)


class TestParseAgendaItems:
    def test_extracts_items(self, meeting_detail_html):
        items = parse_agenda_items(meeting_detail_html, meeting_id=6439)
        assert len(items) == 2

    def test_item_numbers(self, meeting_detail_html):
        items = parse_agenda_items(meeting_detail_html, meeting_id=6439)
        assert items[0].item_number == "4"
        assert items[1].item_number == "5"

    def test_item_titles_strip_pdf_suffix(self, meeting_detail_html):
        items = parse_agenda_items(meeting_detail_html, meeting_id=6439)
        assert items[0].title == "Pimlico District Heating"
        assert items[1].title == "Homelessness Strategy 2025-2030"

    def test_description(self, meeting_detail_html):
        items = parse_agenda_items(meeting_detail_html, meeting_id=6439)
        assert "additional expenditure" in items[0].description
        assert "Homelessness Strategy" in items[1].description

    def test_decision_text(self, meeting_detail_html):
        items = parse_agenda_items(meeting_detail_html, meeting_id=6439)
        assert "£1.2m" in items[0].decision_text
        assert "adoption" in items[1].decision_text

    def test_minutes_text(self, meeting_detail_html):
        items = parse_agenda_items(meeting_detail_html, meeting_id=6439)
        assert "Councillor Smith" in items[0].minutes_text
        assert "rough sleeping" in items[1].minutes_text

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


class TestParseAttendance:
    def test_extracts_present_councillors(self, attendance_html):
        councillors = parse_attendance(attendance_html)
        assert len(councillors) == 2  # Sullivan has apologies, not present

    def test_councillor_names(self, attendance_html):
        councillors = parse_attendance(attendance_html)
        names = {c.name for c in councillors}
        assert "Adam Hug" in names
        assert "David Boothroyd" in names

    def test_excludes_apologies(self, attendance_html):
        councillors = parse_attendance(attendance_html)
        names = {c.name for c in councillors}
        assert "Max Sullivan" not in names

    def test_councillor_roles(self, attendance_html):
        councillors = parse_attendance(attendance_html)
        chair = next(c for c in councillors if c.name == "Adam Hug")
        assert chair.role == "Chair"

    def test_profile_urls(self, attendance_html):
        councillors = parse_attendance(attendance_html)
        for c in councillors:
            assert c.profile_url is not None
            assert "mgUserInfo" in c.profile_url


class TestExtractCouncillorsFromText:
    def test_extracts_single_councillor(self):
        text = "Councillor Boothroyd introduced the budget report."
        names = extract_councillors_from_text(text)
        assert names == ["Boothroyd"]

    def test_extracts_multiple_councillors(self):
        text = (
            "Councillor Hug opened the meeting. "
            "Councillor Sullivan presented the report. "
            "Councillor Boothroyd asked a question."
        )
        names = extract_councillors_from_text(text)
        assert names == ["Hug", "Sullivan", "Boothroyd"]

    def test_deduplicates(self):
        text = (
            "Councillor Hug opened the meeting. "
            "Councillor Hug also noted the budget."
        )
        names = extract_councillors_from_text(text)
        assert names == ["Hug"]

    def test_handles_two_word_names(self):
        text = "Councillor Butler Thalassis spoke about climate."
        names = extract_councillors_from_text(text)
        assert names == ["Butler Thalassis"]

    def test_empty_text(self):
        assert extract_councillors_from_text("") == []

    def test_no_councillors(self):
        text = "The committee approved the motion."
        assert extract_councillors_from_text(text) == []


class TestEmitEvent:
    def test_basic_event(self):
        event = emit_event("retriever", "started", "Begin fetch", url="https://example.com")
        assert event.agent_name == "retriever"
        assert event.metadata == {"url": "https://example.com"}

    def test_no_metadata(self):
        event = emit_event("retriever", "completed", "Done")
        assert event.metadata is None
