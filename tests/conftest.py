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
<a href="ieListDocuments.aspx?CId=130&MId=7115&Ver=4">1 Jun 2099 6.30 pm</a>
<a href="ieListDocuments.aspx?CId=130&MId=6439&Ver=4">31 Mar 2020 6.30 pm</a>
<a href="ieListDocuments.aspx?CId=130&MId=6438&Ver=4">17 Feb 2020 6.30 pm</a>
<a href="/other-page">Not a meeting</a>
</body></html>
"""

MEETING_DETAIL_HTML = """\
<html><body>
<a href="documents/g6439/Agenda frontsheet 31st-Mar-2025.pdf">Agenda frontsheet (PDF, 132 KB)</a>
<a href="documents/g6439/Printed minutes 31st-Mar-2025.pdf">Printed minutes (PDF, 97 KB)</a>
<a href="documents/g6439/Decisions 31st-Mar-2025.pdf">Printed decisions (PDF, 88 KB)</a>
<table>
<tr>
  <td class="mgItemNumberCell"><p class="mgAiTitleTxt">4.</p></td>
  <td>
    <p class="mgAiTitleTxt">Pimlico District HeatingPDF 500 KB</p>
    <ul class="mgActionList"><li><a href="ieDecisionDetails.aspx?AIId=3001">View the decision for item 4.</a></li></ul>
    <div class="mgWordPara">To consider additional expenditure for Pimlico District Heating.</div>
    <p class="mgSubItemTitleTxt">Decision:</p>
    <div class="mgWordPara">Cabinet approved additional expenditure of £1.2m for essential infrastructure maintenance.</div>
    <p class="mgSubItemTitleTxt">Minutes:</p>
    <div class="mgWordPara">Councillor Smith introduced the report. The committee discussed the urgent need for repairs.</div>
  </td>
</tr>
<tr>
  <td class="mgItemNumberCell"><p class="mgAiTitleTxt">5.</p></td>
  <td>
    <p class="mgAiTitleTxt">Homelessness Strategy 2025-2030PDF 1 MB</p>
    <ul class="mgActionList"><li><a href="ieDecisionDetails.aspx?AIId=3002">View the decision for item 5.</a></li></ul>
    <div class="mgWordPara">To consider the new Homelessness Strategy.</div>
    <p class="mgSubItemTitleTxt">Decision:</p>
    <div class="mgWordPara">Cabinet approved the Homelessness Strategy 2025-2030 for adoption.</div>
    <p class="mgSubItemTitleTxt">Minutes:</p>
    <div class="mgWordPara">Councillor Jones outlined the five-year plan to reduce rough sleeping by 50%.</div>
  </td>
</tr>
</table>
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


ATTENDANCE_HTML = """\
<html><body>
<table>
<tr><td>Attendee</td><td>Role</td><td>Attendance</td></tr>
<tr><td><a href="mgUserInfo.aspx?UID=158">Adam Hug</a></td><td>Chair</td><td>Present</td></tr>
<tr><td><a href="mgUserInfo.aspx?UID=200">David Boothroyd</a></td><td>Member</td><td>Present</td></tr>
<tr><td><a href="mgUserInfo.aspx?UID=201">Max Sullivan</a></td><td>Member</td><td>Apologies</td></tr>
</table>
</body></html>
"""


@pytest.fixture
def attendance_html():
    return ATTENDANCE_HTML


@pytest.fixture
def sample_document():
    return CouncilDocument(
        url="https://example.com/meeting/123",
        title="Planning Committee Meeting",
        doc_type=DocumentType.MINUTES,
        fetched_at=datetime(2026, 3, 15, tzinfo=timezone.utc),
        raw_content=SAMPLE_HTML,
    )
