"""Summarise council agenda items into plain-language voter-friendly text via OpenAI."""

from __future__ import annotations

import json
import os
from pathlib import Path

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI

from src.models.documents import AgendaItem, VoterSummary

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

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


PROS_CONS_SYSTEM_PROMPT = """\
You are a civic information assistant for Westminster Council voters.
You will receive details of a council agenda item (upcoming or decided).
Your job is to present a balanced view of the arguments for and against the proposal.

Respond with JSON containing:
- "summary": A clear 1-2 sentence plain-English summary of what is being proposed or decided.
- "pros": A list of 2-4 arguments IN FAVOUR of this proposal (from a resident's perspective).
- "cons": A list of 2-4 arguments AGAINST this proposal (from a resident's perspective).
- "councillors": A list of any councillor names mentioned (empty list if none).
- "status": Either "upcoming" or "decided"."""


VOTER_BRIEF_SYSTEM_PROMPT = """\
You are a civic information assistant for Westminster Council voters.
You will receive details of a council agenda item. Explain it for residents in plain English.

Respond with JSON containing:
- "summary": A clear 2-3 sentence plain-English summary.
- "why_it_matters": One short paragraph explaining why residents should care.
- "pros": A list of 2-4 arguments in favour from a resident's perspective.
- "cons": A list of 2-4 arguments against from a resident's perspective.
- "what_to_watch": One sentence on what residents should watch for next.
- "councillors": A list of any councillor names mentioned (empty list if none).
- "notify_voters": true if this item should be surfaced prominently to voters, else false."""


def _resolve_openai_api_key() -> str | None:
    return os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_KEY")


def _get_client(client: OpenAI | None) -> OpenAI:
    if client is not None:
        return client
    api_key = _resolve_openai_api_key()
    if api_key:
        return OpenAI(api_key=api_key)
    return OpenAI()


def generate_pros_cons(
    item: AgendaItem,
    is_upcoming: bool = False,
    client: OpenAI | None = None,
    model: str = "gpt-4o",
) -> dict:
    """Generate pros and cons for a single agenda item."""
    client = _get_client(client)
    user_prompt = _build_prompt(item, is_upcoming)

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": PROS_CONS_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        response_format={"type": "json_object"},
    )

    result = json.loads(response.choices[0].message.content)
    return {
        "item": item,
        "is_upcoming": is_upcoming,
        "summary": result.get("summary", ""),
        "pros": result.get("pros", []),
        "cons": result.get("cons", []),
        "councillors": result.get("councillors", []),
        "status": result.get("status", "upcoming" if is_upcoming else "decided"),
    }


def generate_voter_brief(
    item: AgendaItem,
    is_upcoming: bool = False,
    client: OpenAI | None = None,
    model: str = "gpt-4o",
) -> dict:
    """Generate a voter-facing brief for one agenda item."""
    client = _get_client(client)
    user_prompt = _build_prompt(item, is_upcoming)

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": VOTER_BRIEF_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        response_format={"type": "json_object"},
    )

    result = json.loads(response.choices[0].message.content)
    return {
        "summary": result.get("summary", ""),
        "why_it_matters": result.get("why_it_matters", ""),
        "pros": result.get("pros", []),
        "cons": result.get("cons", []),
        "what_to_watch": result.get("what_to_watch", ""),
        "councillors": result.get("councillors", []),
        "notify_voters": result.get("notify_voters", True),
        "status": "upcoming" if is_upcoming else "decided",
    }


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
    client = _get_client(client)

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
