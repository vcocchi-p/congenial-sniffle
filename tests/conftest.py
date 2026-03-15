"""Shared fixtures for tests."""

from datetime import datetime, timezone

import pytest

from src.models.documents import CouncilDocument, DocumentType

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

COMMITTEES_HTML = """\
<html><body>
<a href="mgCommitteeDetails.aspx?ID=130">Cabinet</a>
<a href="mgCommitteeDetails.aspx?ID=175">Council</a>
<a href="mgCommitteeDetails.aspx?ID=565">Strategic Planning Committee</a>
<a href="/other-page">Not a committee</a>
</body></html>
"""

MEETINGS_HTML = """\
<html><body>
<a href="ieListDocuments.aspx?CId=130&MId=6439&Ver=4">31 Mar 2025 6.30 pm</a>
<a href="ieListDocuments.aspx?CId=130&MId=6438&Ver=4">17 Feb 2025 6.30 pm</a>
<a href="/other-page">Not a meeting</a>
</body></html>
"""

MEETING_DETAIL_HTML = """\
<html><body>
<a href="documents/g6439/Agenda frontsheet 31st-Mar-2025.pdf">Agenda frontsheet (PDF, 132 KB)</a>
<a href="documents/g6439/Printed minutes 31st-Mar-2025.pdf">Printed minutes (PDF, 97 KB)</a>
<a href="documents/g6439/Decisions 31st-Mar-2025.pdf">Printed decisions (PDF, 88 KB)</a>
<div>
  <td>4. <a href="ieDecisionDetails.aspx?AIId=3001">Pimlico District Heating</a></td>
  <td>5. <a href="ieDecisionDetails.aspx?AIId=3002">Homelessness Strategy 2025-2030</a></td>
</div>
</body></html>
"""

DECISION_DETAIL_HTML = """\
<html>
<head><title>Decision - Pimlico District Heating</title></head>
<body>
<h2>Decision</h2>
<p>Cabinet approved additional expenditure of £1.2m.</p>
<h2>Reasons for Decision</h2>
<p>Essential infrastructure maintenance required to ensure continued service delivery.</p>
<h2>Made by</h2>
<p>Cabinet</p>
</body></html>
"""


@pytest.fixture
def sample_html():
    return SAMPLE_HTML


@pytest.fixture
def committees_html():
    return COMMITTEES_HTML


@pytest.fixture
def meetings_html():
    return MEETINGS_HTML


@pytest.fixture
def meeting_detail_html():
    return MEETING_DETAIL_HTML


@pytest.fixture
def decision_detail_html():
    return DECISION_DETAIL_HTML


@pytest.fixture
def sample_document():
    return CouncilDocument(
        url="https://example.com/meeting/123",
        title="Planning Committee Meeting",
        doc_type=DocumentType.MINUTES,
        fetched_at=datetime(2026, 3, 15, tzinfo=timezone.utc),
        raw_content=SAMPLE_HTML,
    )
