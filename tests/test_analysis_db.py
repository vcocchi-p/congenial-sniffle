from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

import src.analysis.db as analysis_db
from src.analysis.db import (
    get_latest_analysis_run_sequence,
    init_analysis_db,
    load_item_analysis,
    load_latest_meeting_selection,
    load_meeting_analyses,
    record_analysis_run_result,
    record_analysis_run_started,
    record_meeting_selection,
)
from src.models.analysis import AgendaItemAnalysis, AnalysisRun, MeetingSelection


@pytest.fixture(autouse=True)
def tmp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    test_db = tmp_path / "analysis_test.db"
    monkeypatch.setattr(analysis_db, "DB_PATH", test_db)
    init_analysis_db()
    return test_db


def _sample_selection() -> MeetingSelection:
    return MeetingSelection(
        retrieval_run_id="run-005",
        meeting_id=6718,
        committee_name="Cabinet",
        meeting_date="23 Feb 2026 6.30 pm",
        analysis_mode="demo_upcoming",
        priority_score=999.0,
        reason_selected="Pinned for demo",
        selected_at=datetime(2026, 3, 15, 16, 0, tzinfo=timezone.utc),
        item_keys=["6718-4"],
    )


def _sample_run() -> AnalysisRun:
    return AnalysisRun(
        analysis_run_id="analysis-run-001",
        retrieval_run_id="run-005",
        meeting_id=6718,
        analysis_mode="demo_upcoming",
        status="running",
        model="gpt-4o",
        selected_reason="Pinned for demo",
        started_at=datetime(2026, 3, 15, 16, 0, tzinfo=timezone.utc),
    )


def _sample_item() -> AgendaItemAnalysis:
    return AgendaItemAnalysis(
        analysis_run_id="analysis-run-001",
        retrieval_run_id="run-005",
        meeting_id=6718,
        item_key="6718-4",
        item_number="4",
        title="Business and Financial Planning 2026/27 to 2028/29",
        plain_summary="Cabinet is being asked to approve the annual budget framework.",
        why_it_matters="This affects council tax, service funding, and local priorities.",
        pros=["Could protect essential services."],
        cons=["Could increase financial pressure on residents."],
        what_to_watch="Watch for changes to council tax and spending priorities.",
        councillors_involved=["Leader of the Council"],
        source_urls=["https://committees.westminster.gov.uk/ieListDocuments.aspx?MId=6718"],
        notify_voters=True,
        analysis_mode="demo_upcoming",
        created_at=datetime(2026, 3, 15, 16, 0, tzinfo=timezone.utc),
    )


def test_selection_and_analysis_round_trip():
    selection = _sample_selection()
    run = _sample_run()
    item = _sample_item()

    record_meeting_selection(selection)
    record_analysis_run_started(run)
    record_analysis_run_result(
        run.analysis_run_id,
        status="completed",
        completed_at=datetime(2026, 3, 15, 16, 5, tzinfo=timezone.utc),
        items=[item],
    )

    stored_selection = load_latest_meeting_selection()
    meeting_items = load_meeting_analyses(6718)
    stored_item = load_item_analysis("6718-4")

    assert stored_selection is not None
    assert stored_selection.meeting_id == 6718
    assert meeting_items[0].title == item.title
    assert stored_item is not None
    assert stored_item.why_it_matters == item.why_it_matters


def test_identical_analysis_reuses_item_version(tmp_db: Path):
    run = _sample_run()
    item = _sample_item()

    record_analysis_run_started(run)
    record_analysis_run_result(
        run.analysis_run_id,
        status="completed",
        completed_at=datetime(2026, 3, 15, 16, 5, tzinfo=timezone.utc),
        items=[item],
    )

    second_run = run.model_copy(
        update={
            "analysis_run_id": "analysis-run-002",
            "started_at": datetime(2026, 3, 15, 17, 0, tzinfo=timezone.utc),
        }
    )
    second_item = item.model_copy(update={"analysis_run_id": "analysis-run-002"})
    record_analysis_run_started(second_run)
    record_analysis_run_result(
        second_run.analysis_run_id,
        status="completed",
        completed_at=datetime(2026, 3, 15, 17, 5, tzinfo=timezone.utc),
        items=[second_item],
    )

    import sqlite3

    conn = sqlite3.connect(tmp_db)
    try:
        version_count = conn.execute("SELECT COUNT(*) FROM analysis_item_versions").fetchone()[0]
        run_link_count = conn.execute("SELECT COUNT(*) FROM analysis_run_items").fetchone()[0]
    finally:
        conn.close()

    assert version_count == 1
    assert run_link_count == 2


def test_analysis_sequence_reads_from_persisted_runs():
    run = _sample_run()
    record_analysis_run_started(run)
    assert get_latest_analysis_run_sequence() == 1
