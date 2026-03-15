"""Tests for the summariser (no API calls — unit tests only)."""

from src.parser.summariser import extract_text


def test_extract_text_strips_tags(sample_html):
    text = extract_text(sample_html)
    assert "<html>" not in text
    assert "<nav>" not in text
    assert "Planning Committee Meeting" in text


def test_extract_text_removes_nav_and_footer(sample_html):
    text = extract_text(sample_html)
    assert "Nav" not in text
    assert "Footer" not in text


def test_extract_text_preserves_content(sample_html):
    text = extract_text(sample_html)
    assert "approve the new housing development" in text
    assert "Councillors Smith, Jones, and Patel" in text
