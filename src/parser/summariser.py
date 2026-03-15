"""Parse council documents and produce plain-language summaries via OpenAI."""

from __future__ import annotations

from bs4 import BeautifulSoup
from openai import OpenAI

from src.models.documents import CouncilDocument, VoterSummary

SYSTEM_PROMPT = """\
You are a civic information assistant. Given raw HTML from a Westminster Council \
document, produce a clear, jargon-free summary that any voter can understand. \
Extract the key points, who is involved, and what the decision means for residents."""


def extract_text(html: str) -> str:
    """Strip HTML tags and return readable text."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)


def summarise_document(
    doc: CouncilDocument,
    client: OpenAI | None = None,
    model: str = "gpt-4o",
) -> VoterSummary:
    """Call OpenAI to summarise a council document for voters."""
    client = client or OpenAI()
    text = extract_text(doc.raw_content)

    # Truncate to fit context window
    max_chars = 80_000
    if len(text) > max_chars:
        text = text[:max_chars] + "\n...[truncated]"

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Document title: {doc.title}\n\n{text}"},
        ],
        response_format={"type": "json_object"},
    )

    import json

    result = json.loads(response.choices[0].message.content)

    return VoterSummary(
        document_id=doc.url,
        title=doc.title,
        plain_summary=result.get("summary", ""),
        key_points=result.get("key_points", []),
        councillors_involved=result.get("councillors", []),
        decision_date=doc.fetched_at,
    )
