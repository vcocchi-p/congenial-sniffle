"""Tests for the summariser (no API calls — unit tests only)."""

from src.models.documents import AgendaItem
from src.parser.summariser import _build_prompt, extract_text


class TestExtractText:
    def test_strips_tags(self, sample_html):
        text = extract_text(sample_html)
        assert "<html>" not in text
        assert "<nav>" not in text
        assert "Planning Committee Meeting" in text

    def test_removes_nav_and_footer(self, sample_html):
        text = extract_text(sample_html)
        assert "Nav" not in text
        assert "Footer" not in text

    def test_preserves_content(self, sample_html):
        text = extract_text(sample_html)
        assert "approve the new housing development" in text
        assert "Councillors Smith, Jones, and Patel" in text


class TestBuildPrompt:
    def test_includes_title(self):
        item = AgendaItem(
            meeting_id=1, item_number="4", title="Housing Strategy 2026-2031"
        )
        prompt = _build_prompt(item, is_upcoming=False)
        assert "Housing Strategy 2026-2031" in prompt

    def test_includes_description(self):
        item = AgendaItem(
            meeting_id=1,
            item_number="4",
            title="Housing Strategy",
            description="To approve the new housing strategy.",
        )
        prompt = _build_prompt(item, is_upcoming=False)
        assert "To approve the new housing strategy." in prompt

    def test_includes_decision_and_minutes(self):
        item = AgendaItem(
            meeting_id=1,
            item_number="4",
            title="Budget",
            decision_text="Cabinet approved the budget.",
            minutes_text="Councillor Smith presented the report.",
        )
        prompt = _build_prompt(item, is_upcoming=False)
        assert "Cabinet approved the budget." in prompt
        assert "Councillor Smith presented the report." in prompt

    def test_upcoming_flag_when_no_decision(self):
        item = AgendaItem(
            meeting_id=1,
            item_number="4",
            title="Housing Strategy",
            description="To approve the strategy.",
        )
        prompt = _build_prompt(item, is_upcoming=True)
        assert "NOT yet been decided" in prompt

    def test_upcoming_flag_not_added_when_decision_exists(self):
        item = AgendaItem(
            meeting_id=1,
            item_number="4",
            title="Housing Strategy",
            decision_text="Cabinet approved the strategy.",
        )
        prompt = _build_prompt(item, is_upcoming=True)
        assert "NOT yet been decided" not in prompt
