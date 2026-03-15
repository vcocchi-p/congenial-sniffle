"""Tests for dashboard rendering helpers."""

from datetime import datetime, timezone

from src.dashboard.components import (
    build_analysis_rows,
    build_committee_rows,
    build_document_rows,
    build_trace_rows,
)
from src.dashboard.state import RetrievalTraceStep
from src.models.analysis import AgendaItemAnalysis
from src.models.documents import Committee, DocumentType, MeetingDocument, RetrievalBundle


def test_build_document_rows_preserves_full_url_and_timestamp():
    fetched_at = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)
    documents = [
        MeetingDocument(
            meeting_id=4776,
            title="Budget report",
            doc_type=DocumentType.AGENDA,
            url="https://committees.westminster.gov.uk/documents/4776/budget.pdf",
            fetched_at=fetched_at,
        )
    ]

    rows = build_document_rows(documents)

    assert rows == [
        {
            "Fetched At": "2026-03-15 12:00:00 UTC",
            "Title": "Budget report",
            "Type": "agenda",
            "Source": "Meeting 4776",
            "URL": "https://committees.westminster.gov.uk/documents/4776/budget.pdf",
        }
    ]


def test_build_trace_rows_uses_meeting_context_when_document_is_missing():
    trace = [
        RetrievalTraceStep(
            timestamp=datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc),
            step_name="meeting detail",
            event_type="progress",
            message="Fetched meeting detail",
            source_url="https://committees.westminster.gov.uk/ieListDocuments.aspx?CId=130&MId=4776",
            document_url=None,
            document_title=None,
            document_type=None,
            detail="Fetched documents and agenda items.",
            meeting_id=4776,
            committee=None,
        )
    ]

    rows = build_trace_rows(trace)

    assert rows[0]["Document / Context"] == (
        "https://committees.westminster.gov.uk/ieListDocuments.aspx?CId=130&MId=4776"
        "#trace-label=Meeting 4776"
    )


def test_build_trace_rows_preserves_document_link():
    trace = [
        RetrievalTraceStep(
            timestamp=datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc),
            step_name="document discovery",
            event_type="progress",
            message="Discovered Budget report",
            source_url="https://committees.westminster.gov.uk/ieListDocuments.aspx?CId=130&MId=4776",
            document_url="https://committees.westminster.gov.uk/documents/4776/budget.pdf",
            document_title="Budget report",
            document_type="agenda",
            detail="Captured a meeting document.",
            meeting_id=4776,
            committee="Cabinet",
        )
    ]

    rows = build_trace_rows(trace)

    assert rows[0]["Document / Context"] == (
        "https://committees.westminster.gov.uk/documents/4776/budget.pdf"
        "#trace-label=Budget report"
    )


def test_build_committee_rows_preserves_full_url():
    bundle = RetrievalBundle(
        committees=[
            Committee(
                id=130,
                name="Cabinet",
                url="https://committees.westminster.gov.uk/mgCommitteeDetails.aspx?ID=130",
            )
        ]
    )

    rows = build_committee_rows(bundle)

    assert rows == [
        {
            "ID": 130,
            "Name": "Cabinet",
            "URL": "https://committees.westminster.gov.uk/mgCommitteeDetails.aspx?ID=130",
        }
    ]


def test_build_analysis_rows_preserves_primary_source_link():
    items = [
        AgendaItemAnalysis(
            analysis_run_id="analysis-run-001",
            retrieval_run_id="run-005",
            meeting_id=6718,
            item_key="6718-4",
            item_number="4",
            title="Budget",
            plain_summary="Budget summary",
            why_it_matters="Budget matters",
            pros=["Protects services"],
            cons=["Could raise costs"],
            what_to_watch="Watch council tax",
            councillors_involved=["Leader"],
            source_urls=[
                "https://committees.westminster.gov.uk/ieListDocuments.aspx?CId=130&MId=6718&Ver=4"
            ],
            notify_voters=True,
            analysis_mode="demo_upcoming",
            created_at=datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc),
        )
    ]

    rows = build_analysis_rows(items)

    assert rows == [
        {
            "Item": "4",
            "Title": "Budget",
            "Summary": "Budget summary",
            "Why It Matters": "Budget matters",
            "Watch": "Watch council tax",
            "Notify": "Yes",
            "Source": "https://committees.westminster.gov.uk/ieListDocuments.aspx?CId=130&MId=6718&Ver=4",
        }
    ]
