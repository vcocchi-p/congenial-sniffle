"""Multi-agent document retrieval from Westminster Council public sources."""

from __future__ import annotations

from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup

from src.models.documents import AgentEvent, CouncilDocument, DocumentType

# Westminster Council committee meetings & decisions base URLs
WESTMINSTER_BASE = "https://committees.westminster.gov.uk"


async def fetch_page(url: str) -> str:
    """Fetch a single page and return its HTML content."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, follow_redirects=True, timeout=30)
        resp.raise_for_status()
        return resp.text


def parse_meeting_links(html: str) -> list[dict]:
    """Extract meeting links and titles from a committee listing page."""
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for link in soup.select("a[href*='ieListDocuments']"):
        href = link.get("href", "")
        if not href.startswith("http"):
            href = f"{WESTMINSTER_BASE}/{href.lstrip('/')}"
        results.append({"url": href, "title": link.get_text(strip=True)})
    return results


async def retrieve_document(url: str, doc_type: DocumentType) -> CouncilDocument:
    """Fetch and wrap a single council document."""
    html = await fetch_page(url)
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else url
    return CouncilDocument(
        url=url,
        title=title,
        doc_type=doc_type,
        fetched_at=datetime.now(timezone.utc),
        raw_content=html,
    )


def emit_event(agent_name: str, event_type: str, message: str, **meta) -> AgentEvent:
    """Helper to create monitoring events."""
    return AgentEvent(
        agent_name=agent_name,
        event_type=event_type,
        message=message,
        timestamp=datetime.now(timezone.utc),
        metadata=meta or None,
    )
