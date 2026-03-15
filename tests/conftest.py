"""Shared fixtures for tests."""

import pytest

from src.models.documents import CouncilDocument, DocumentType
from datetime import datetime, timezone

SAMPLE_HTML = """\
<html>
<head><title>Test Council Meeting</title></head>
<body>
<nav>Nav</nav>
<div>
  <h1>Planning Committee Meeting</h1>
  <p>The committee decided to approve the new housing development on Elm Street.</p>
  <p>Councillors Smith, Jones, and Patel voted in favour.</p>
</div>
<footer>Footer</footer>
</body>
</html>
"""


@pytest.fixture
def sample_html():
    return SAMPLE_HTML


@pytest.fixture
def sample_document():
    return CouncilDocument(
        url="https://example.com/meeting/123",
        title="Planning Committee Meeting",
        doc_type=DocumentType.MINUTES,
        fetched_at=datetime(2026, 3, 15, tzinfo=timezone.utc),
        raw_content=SAMPLE_HTML,
    )
