"""Summarise council agenda items into plain-language voter-friendly text via OpenAI."""

from __future__ import annotations

import json

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

from src.models.documents import AgendaItem, VoterSummary

UPCOMING_SYSTEM_PROMPT = """\
You are a civic information assistant for Westminster Council voters.
You will receive details of an UPCOMING agenda item that has NOT yet been decided.
Your job is to explain what is being proposed and why it matters to residents.

Respond with JSON containing:
- "summary": A clear 2-3 sentence plain-English summary a non-expert can understand.
- "key_points": A list of 3-5 bullet points covering: what is proposed, who it affects, \
and what residents should know.
- "councillors": A list of any councillor names mentioned (empty list if none).
- "what_to_watch": One sentence on what the outcome could mean for residents."""

DECIDED_SYSTEM_PROMPT = """\
You are a civic information assistant for Westminster Council voters.
You will receive details of a council agenda item that HAS been decided.
Your job is to explain what was decided and what it means for residents.

Respond with JSON containing:
- "summary": A clear 2-3 sentence plain-English summary a non-expert can understand.
- "key_points": A list of 3-5 bullet points covering: what was decided, who it affects, \
and what it means in practice.
- "councillors": A list of any councillor names mentioned (empty list if none)."""


def extract_text(html: str) -> str:
    """Strip HTML tags and return readable text."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)


def _build_prompt(item: AgendaItem, is_upcoming: bool) -> str:
    """Build the user prompt from an agenda item's fields."""
    parts = [f"Title: {item.title}"]

    if item.description:
        parts.append(f"Description: {item.description}")

    if item.decision_text:
        parts.append(f"Decision: {item.decision_text}")

    if item.minutes_text:
        parts.append(f"Discussion/Minutes: {item.minutes_text}")

    if is_upcoming and not item.decision_text:
        parts.append("Status: This item has NOT yet been decided.")

    return "\n\n".join(parts)


def summarise_item(
    item: AgendaItem,
    is_upcoming: bool = False,
    client: OpenAI | None = None,
    model: str = "gpt-4o",
) -> VoterSummary:
    """Summarise a single agenda item for voters.

    Args:
        item: The agenda item to summarise.
        is_upcoming: Whether this is an upcoming (not yet decided) item.
        client: OpenAI client (created if not provided).
        model: OpenAI model to use.
    """
    client = client or OpenAI()

    system_prompt = UPCOMING_SYSTEM_PROMPT if is_upcoming else DECIDED_SYSTEM_PROMPT
    user_prompt = _build_prompt(item, is_upcoming)

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        response_format={"type": "json_object"},
    )

    result = json.loads(response.choices[0].message.content)

    return VoterSummary(
        document_id=item.decision_url or f"item-{item.meeting_id}-{item.item_number}",
        title=item.title,
        plain_summary=result.get("summary", ""),
        key_points=result.get("key_points", []),
        councillors_involved=result.get("councillors", []),
    )
