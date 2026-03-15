"""Multi-agent document retrieval from Westminster Council public sources.

Site structure (committees.westminster.gov.uk):
  - mgListCommittees.aspx                       → list of all committees
  - ieListMeetings.aspx?CId={id}&Year=0         → meetings for a committee
  - ieListDocuments.aspx?CId={cid}&MId={mid}    → agenda, minutes, docs for a meeting
  - ieDecisionDetails.aspx?AIId={id}            → individual decision detail
  - mgMemberIndex.aspx                          → all councillors
  - mgUserInfo.aspx?UID={id}                    → councillor detail
  - mgMeetingAttendance.aspx?ID={mid}           → who attended a meeting
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup

from src.models.documents import (
    AgendaItem,
    AgentEvent,
    Committee,
    CouncilDocument,
    DocumentType,
    Meeting,
    MeetingDocument,
)

WESTMINSTER_BASE = "https://committees.westminster.gov.uk"


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

async def fetch_page(url: str) -> str:
    """Fetch a single page and return its HTML content."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, follow_redirects=True, timeout=30)
        resp.raise_for_status()
        return resp.text


def _absolute(href: str) -> str:
    """Convert a relative href to an absolute URL."""
    if href.startswith("http"):
        return href
    return f"{WESTMINSTER_BASE}/{href.lstrip('/')}"


# ---------------------------------------------------------------------------
# Parsing: Committees
# ---------------------------------------------------------------------------

def parse_committees(html: str) -> list[Committee]:
    """Parse the committee listing page (mgListCommittees.aspx)."""
    soup = BeautifulSoup(html, "html.parser")
    committees = []
    for link in soup.select("a[href*='mgCommitteeDetails']"):
        href = link.get("href", "")
        match = re.search(r"ID=(\d+)", href)
        if match:
            committees.append(
                Committee(
                    id=int(match.group(1)),
                    name=link.get_text(strip=True),
                    url=_absolute(href),
                )
            )
    return committees


# ---------------------------------------------------------------------------
# Parsing: Meetings
# ---------------------------------------------------------------------------

def parse_meetings(html: str, committee_id: int, committee_name: str) -> list[Meeting]:
    """Parse a committee's meeting list page (ieListMeetings.aspx)."""
    soup = BeautifulSoup(html, "html.parser")
    meetings = []
    for link in soup.select("a[href*='ieListDocuments']"):
        href = link.get("href", "")
        mid_match = re.search(r"MId=(\d+)", href)
        if mid_match:
            meetings.append(
                Meeting(
                    committee_id=committee_id,
                    committee_name=committee_name,
                    meeting_id=int(mid_match.group(1)),
                    date=link.get_text(strip=True),
                    url=_absolute(href),
                )
            )
    return meetings


# ---------------------------------------------------------------------------
# Parsing: Meeting documents & agenda items
# ---------------------------------------------------------------------------

def parse_meeting_documents(html: str, meeting_id: int) -> list[MeetingDocument]:
    """Parse document links (PDFs) from a meeting detail page (ieListDocuments.aspx)."""
    soup = BeautifulSoup(html, "html.parser")
    docs = []
    for link in soup.select("a[href$='.pdf']"):
        href = link.get("href", "")
        title = link.get_text(strip=True)
        title_lower = title.lower()

        if "minute" in title_lower:
            doc_type = DocumentType.MINUTES
        elif "decision" in title_lower:
            doc_type = DocumentType.DECISION
        else:
            doc_type = DocumentType.AGENDA

        docs.append(
            MeetingDocument(
                meeting_id=meeting_id,
                title=title,
                doc_type=doc_type,
                url=_absolute(href),
            )
        )
    return docs


def parse_agenda_items(html: str, meeting_id: int) -> list[AgendaItem]:
    """Parse agenda items from a meeting detail page."""
    soup = BeautifulSoup(html, "html.parser")
    items = []

    # Agenda items are typically in rows with class containing "mgItemTitle" or
    # in links to ieDecisionDetails
    for link in soup.select("a[href*='ieDecisionDetails']"):
        href = link.get("href", "")
        text = link.get_text(strip=True)
        if text:
            # Try to extract item number from preceding text
            parent = link.find_parent("td") or link.find_parent("div")
            item_num = ""
            if parent:
                full_text = parent.get_text(strip=True)
                num_match = re.match(r"^(\d+\.?)", full_text)
                if num_match:
                    item_num = num_match.group(1)

            items.append(
                AgendaItem(
                    meeting_id=meeting_id,
                    item_number=item_num,
                    title=text,
                    decision_url=_absolute(href),
                )
            )
    return items


# ---------------------------------------------------------------------------
# Parsing: Decision detail
# ---------------------------------------------------------------------------

def parse_decision_detail(html: str) -> dict:
    """Parse an individual decision page (ieDecisionDetails.aspx).

    Returns a dict with keys: title, decision, reasons, made_by, date.
    """
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else ""

    result = {"title": title, "decision": "", "reasons": "", "made_by": "", "date": ""}

    # Look for common patterns in the decision page
    for header in soup.find_all(["h2", "h3", "strong"]):
        header_text = header.get_text(strip=True).lower()
        next_elem = header.find_next_sibling()
        if next_elem:
            content = next_elem.get_text(strip=True)
        else:
            content = ""

        if "decision" in header_text and "reason" not in header_text:
            result["decision"] = content
        elif "reason" in header_text:
            result["reasons"] = content
        elif "made by" in header_text or "decision maker" in header_text:
            result["made_by"] = content
        elif "date" in header_text:
            result["date"] = content

    return result


# ---------------------------------------------------------------------------
# High-level retrieval flows
# ---------------------------------------------------------------------------

async def fetch_all_committees() -> list[Committee]:
    """Fetch and parse all Westminster Council committees."""
    html = await fetch_page(f"{WESTMINSTER_BASE}/mgListCommittees.aspx")
    return parse_committees(html)


async def fetch_meetings(committee: Committee) -> list[Meeting]:
    """Fetch all meetings for a committee."""
    url = f"{WESTMINSTER_BASE}/ieListMeetings.aspx?CId={committee.id}&Year=0"
    html = await fetch_page(url)
    return parse_meetings(html, committee.id, committee.name)


async def fetch_meeting_detail(meeting: Meeting) -> tuple[list[MeetingDocument], list[AgendaItem]]:
    """Fetch documents and agenda items for a specific meeting."""
    html = await fetch_page(meeting.url)
    docs = parse_meeting_documents(html, meeting.meeting_id)
    items = parse_agenda_items(html, meeting.meeting_id)
    return docs, items


async def fetch_decision(url: str) -> dict:
    """Fetch and parse an individual decision detail page."""
    html = await fetch_page(url)
    return parse_decision_detail(html)


# ---------------------------------------------------------------------------
# Monitoring helpers
# ---------------------------------------------------------------------------

def emit_event(agent_name: str, event_type: str, message: str, **meta) -> AgentEvent:
    """Helper to create monitoring events."""
    return AgentEvent(
        agent_name=agent_name,
        event_type=event_type,
        message=message,
        timestamp=datetime.now(timezone.utc),
        metadata=meta or None,
    )
