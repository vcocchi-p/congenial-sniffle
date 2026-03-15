"""Tests for retrieval run simulation."""

from datetime import datetime, timezone

from src.dashboard.simulation import create_retrieval_run
from src.dashboard.state import summarize_run


def test_completed_retrieval_run_has_expected_event_sequence():
    events, documents = create_retrieval_run(
        "run-010", datetime(2026, 3, 15, 13, 0, tzinfo=timezone.utc), outcome="completed"
    )

    assert events[0].event_type == "started"
    assert events[-1].event_type == "completed"
    assert len(documents) == 3


def test_error_retrieval_run_surfaces_error_status():
    events, documents = create_retrieval_run(
        "run-011", datetime(2026, 3, 15, 14, 0, tzinfo=timezone.utc), outcome="error"
    )

    summary = summarize_run("run-011", events, documents)

    assert summary is not None
    assert summary.status == "error"
    assert summary.documents_discovered == 3
    assert summary.documents_fetched == 1
    assert summary.errors == 1
