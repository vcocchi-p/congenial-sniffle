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
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from src.models.documents import (
    AgendaItem,
    AgentEvent,
    Committee,
    Councillor,
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
    return urljoin(f"{WESTMINSTER_BASE}/", href)


def _clean_document_title(title: str) -> str:
    """Strip trailing PDF size markers from meeting document link text."""
    cleaned = re.sub(r"\s*\(PDF,\s*[\d.]+\s*[KMG]B\)\s*$", "", title, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*PDF\s*[\d.]+\s*[KMG]B\s*$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


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
    """Parse a committee's meeting list page (ieListMeetings.aspx).

    Determines whether each meeting is upcoming by parsing the date text
    and comparing to today's date.
    """
    soup = BeautifulSoup(html, "html.parser")
    today = datetime.now(timezone.utc).date()
    meetings = []
    for link in soup.select("a[href*='ieListDocuments']"):
        href = link.get("href", "")
        mid_match = re.search(r"MId=(\d+)", href)
        if mid_match:
            date_text = link.get_text(strip=True)
            is_upcoming = _is_upcoming(date_text, today)
            meetings.append(
                Meeting(
                    committee_id=committee_id,
                    committee_name=committee_name,
                    meeting_id=int(mid_match.group(1)),
                    date=date_text,
                    url=_absolute(href),
                    is_upcoming=is_upcoming,
                )
            )
    return meetings


def _is_upcoming(date_text: str, today) -> bool:
    """Check if a meeting date string is in the future.

    Date text looks like "16 Mar 2026 6.30 pm" or "Constitution".
    """
    # Try to parse the date portion (e.g. "16 Mar 2026")
    match = re.match(r"(\d{1,2}\s+\w+\s+\d{4})", date_text)
    if not match:
        return False
    try:
        meeting_date = datetime.strptime(match.group(1), "%d %b %Y").date()
        return meeting_date >= today
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Parsing: Meeting documents & agenda items
# ---------------------------------------------------------------------------

def parse_meeting_documents(html: str, meeting_id: int) -> list[MeetingDocument]:
    """Parse document links (PDFs) from a meeting detail page (ieListDocuments.aspx)."""
    soup = BeautifulSoup(html, "html.parser")
    docs = []
    for link in soup.select("a[href$='.pdf']"):
        href = link.get("href", "")
        raw_title = link.get_text(strip=True)
        title_lower = raw_title.lower()

        if "minute" in title_lower:
            doc_type = DocumentType.MINUTES
        elif "decision" in title_lower:
            doc_type = DocumentType.DECISION
        else:
            doc_type = DocumentType.AGENDA

        docs.append(
            MeetingDocument(
                meeting_id=meeting_id,
                title=_clean_document_title(raw_title),
                doc_type=doc_type,
                url=_absolute(href),
            )
        )
    return docs


def parse_agenda_items(html: str, meeting_id: int) -> list[AgendaItem]:
    """Parse agenda items with full inline content from a meeting detail page.

    Each agenda item is a <tr> containing:
      - p.mgAiTitleTxt            → item number + title
      - div.mgWordPara (first)    → description of the item
      - p.mgSubItemTitleTxt "Decision:" + div.mgWordPara → decision text
      - p.mgSubItemTitleTxt "Minutes:"  + div.mgWordPara → minutes text
    """
    soup = BeautifulSoup(html, "html.parser")
    items = []

    for row in soup.find_all("tr"):
        # Each agenda item row has a cell with class mgItemNumberCell
        num_cell = row.find("td", class_="mgItemNumberCell")
        if not num_cell:
            continue

        item_number = num_cell.get_text(strip=True).rstrip(".")

        # The second cell has the title + all content
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        content_cell = cells[1]

        # Title from first p.mgAiTitleTxt
        title_tag = content_cell.find("p", class_="mgAiTitleTxt")
        title = title_tag.get_text(strip=True) if title_tag else ""
        # Strip trailing "PDF xxx KB" from title
        title = re.sub(r"PDF\s+[\d.]+ [KMG]B\s*$", "", title).strip()

        # Decision URL if present
        decision_url = None
        for link in content_cell.select("a[href*='ieDecisionDetails']"):
            decision_url = _absolute(link.get("href", ""))
            break

        # Walk through mgSubItemTitleTxt headers to find Decision/Minutes sections
        description = ""
        decision_text = ""
        minutes_text = ""

        # First div.mgWordPara before any mgSubItemTitleTxt is the description
        sub_headers = content_cell.find_all("p", class_="mgSubItemTitleTxt")
        first_desc_div = content_cell.find("div", class_="mgWordPara")
        if first_desc_div:
            # Only use as description if it comes before the first sub-header
            if not sub_headers or first_desc_div.sourceline < sub_headers[0].sourceline:
                description = first_desc_div.get_text(separator="\n", strip=True)

        for header in sub_headers:
            header_text = header.get_text(strip=True).lower().rstrip(":")
            # The content is in the next div.mgWordPara sibling
            next_div = header.find_next_sibling("div", class_="mgWordPara")
            if not next_div:
                continue
            content = next_div.get_text(separator="\n", strip=True)

            if header_text == "decision":
                decision_text = content
            elif header_text == "minutes":
                minutes_text = content

        if title:
            items.append(
                AgendaItem(
                    meeting_id=meeting_id,
                    item_number=item_number,
                    title=title,
                    description=description,
                    decision_text=decision_text,
                    minutes_text=minutes_text,
                    decision_url=decision_url,
                )
            )

    return items


# ---------------------------------------------------------------------------
# Parsing: Attendance & councillors
# ---------------------------------------------------------------------------

def parse_attendance(html: str) -> list[Councillor]:
    """Parse the meeting attendance page (mgMeetingAttendance.aspx).

    Returns a list of councillors who attended the meeting.
    """
    soup = BeautifulSoup(html, "html.parser")
    councillors = []
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        name_cell = cells[0]
        role = cells[1].get_text(strip=True)
        attendance = cells[2].get_text(strip=True)
        if attendance.lower() != "present":
            continue
        link = name_cell.find("a")
        name = name_cell.get_text(strip=True)
        profile_url = None
        if link and link.get("href"):
            profile_url = _absolute(link["href"])
        if name:
            councillors.append(
                Councillor(name=name, role=role, profile_url=profile_url)
            )
    return councillors


def extract_councillors_from_text(text: str) -> list[str]:
    """Extract councillor names from minutes/decision text.

    Looks for the pattern "Councillor Surname" or "Councillors X, Y and Z".
    """
    # Match "Councillor(s) Name" patterns
    matches = re.findall(r"Councillor\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)", text)
    # Deduplicate while preserving order
    seen = set()
    result = []
    for name in matches:
        if name not in seen:
            seen.add(name)
            result.append(name)
    return result


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
    fetched_at = datetime.now(timezone.utc)
    docs = [
        document.model_copy(update={"fetched_at": fetched_at})
        for document in parse_meeting_documents(html, meeting.meeting_id)
    ]
    items = parse_agenda_items(html, meeting.meeting_id)
    return docs, items


async def fetch_attendance(meeting: Meeting) -> list[Councillor]:
    """Fetch and parse the attendance list for a meeting."""
    url = f"{WESTMINSTER_BASE}/mgMeetingAttendance.aspx?ID={meeting.meeting_id}"
    html = await fetch_page(url)
    return parse_attendance(html)


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
