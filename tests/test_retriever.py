"""Tests for the retriever agent."""

from src.agents.retriever import emit_event, parse_meeting_links


def test_parse_meeting_links(sample_html):
    # sample_html doesn't have ieListDocuments links, so should return empty
    links = parse_meeting_links(sample_html)
    assert isinstance(links, list)


def test_parse_meeting_links_with_matches():
    html = """
    <html><body>
    <a href="/ieListDocuments.aspx?MId=123">March Planning Meeting</a>
    <a href="https://committees.westminster.gov.uk/ieListDocuments.aspx?MId=456">April Budget</a>
    <a href="/other-page">Not a meeting</a>
    </body></html>
    """
    links = parse_meeting_links(html)
    assert len(links) == 2
    assert "March Planning Meeting" in links[0]["title"]
    assert links[1]["url"].startswith("https://")


def test_emit_event():
    event = emit_event("retriever", "started", "Begin fetch", url="https://example.com")
    assert event.agent_name == "retriever"
    assert event.metadata == {"url": "https://example.com"}


def test_emit_event_no_metadata():
    event = emit_event("retriever", "completed", "Done")
    assert event.metadata is None
