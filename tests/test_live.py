"""Integration tests that hit the live Westminster Council site.

Run with: pytest tests/test_live.py -v
"""

import pytest

from src.agents.retriever import (
    fetch_all_committees,
    fetch_decision,
    fetch_meeting_detail,
    fetch_meetings,
)

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_fetch_committees():
    committees = await fetch_all_committees()
    assert len(committees) > 10
    names = {c.name for c in committees}
    assert "Cabinet" in names


@pytest.mark.asyncio
async def test_fetch_cabinet_meetings():
    committees = await fetch_all_committees()
    cabinet = next(c for c in committees if c.name == "Cabinet")
    meetings = await fetch_meetings(cabinet)
    assert len(meetings) > 0
    assert meetings[0].committee_name == "Cabinet"


@pytest.mark.asyncio
async def test_fetch_meeting_detail():
    committees = await fetch_all_committees()
    cabinet = next(c for c in committees if c.name == "Cabinet")
    meetings = await fetch_meetings(cabinet)
    # Get the most recent meeting with docs (skip future ones)
    for meeting in meetings:
        docs, items = await fetch_meeting_detail(meeting)
        if docs:
            assert any("pdf" in d.url.lower() for d in docs)
            return
    pytest.skip("No meetings with documents found")


@pytest.mark.asyncio
async def test_fetch_decision_detail():
    committees = await fetch_all_committees()
    cabinet = next(c for c in committees if c.name == "Cabinet")
    meetings = await fetch_meetings(cabinet)
    for meeting in meetings:
        _, items = await fetch_meeting_detail(meeting)
        items_with_decisions = [i for i in items if i.decision_url]
        if items_with_decisions:
            detail = await fetch_decision(items_with_decisions[0].decision_url)
            assert detail["title"]
            return
    pytest.skip("No decisions found")
