"""Tests for dashboard state derivation."""

import pytest

from src.dashboard.mock_data import create_seeded_dashboard_data
from src.dashboard.state import (
    get_current_run_id,
    get_selected_run_id,
    initialize_state,
    load_global_metrics,
    load_pipeline_requests,
    load_recent_runs,
    load_retrieval_overview,
    load_retrieval_trace,
    load_stage_snapshots,
    select_run,
    submit_pipeline_request,
)


def test_initialize_state_populates_empty_mapping():
    state = {}

    initialize_state(state)

    assert get_current_run_id(state) is None
    assert get_selected_run_id(state) == "run-003"
    assert len(load_recent_runs(state)) == 3


def test_load_stage_snapshots_marks_non_retrieval_stages_as_placeholders():
    state = create_seeded_dashboard_data()

    stages = load_stage_snapshots(state)

    assert stages[0].stage == "retrieval"
    assert stages[0].status == "idle"
    assert all(stage.status == "placeholder" for stage in stages[1:])


def test_load_recent_runs_orders_newest_first():
    state = create_seeded_dashboard_data()

    runs = load_recent_runs(state)

    assert [run.run_id for run in runs] == ["run-003", "run-002", "run-001"]
    assert runs[1].status == "error"


def test_load_global_metrics_uses_current_run_and_recent_errors():
    state = create_seeded_dashboard_data()

    metrics = load_global_metrics(state)

    assert metrics["active_run_status"] == "idle"
    assert metrics["documents_discovered"] == 0
    assert metrics["documents_fetched"] == 0
    assert metrics["queued_requests"] == 0
    assert metrics["recent_errors"] == 1
    assert metrics["last_run_at"] is not None


def test_selecting_historical_run_updates_retrieval_overview():
    state = create_seeded_dashboard_data()
    select_run(state, "run-002")

    overview = load_retrieval_overview(state, get_selected_run_id(state))
    trace = load_retrieval_trace(state, "run-002")

    assert overview["run_id"] == "run-002"
    assert overview["status"] == "error"
    assert overview["latest_error"] is not None
    assert trace[-1].event_type == "error"


def test_submit_pipeline_request_adds_queued_request():
    state = create_seeded_dashboard_data()

    request = submit_pipeline_request(state, "https://committees.westminster.gov.uk/custom-source")

    requests = load_pipeline_requests(state)
    overview = load_retrieval_overview(state, None)
    stages = load_stage_snapshots(state)

    assert request.request_id == "request-001"
    assert requests[0].source_url == "https://committees.westminster.gov.uk/custom-source"
    assert overview["status"] == "queued"
    assert overview["active_request"] is not None
    assert stages[0].status == "queued"


def test_submit_pipeline_request_rejects_invalid_url():
    state = create_seeded_dashboard_data()

    with pytest.raises(ValueError):
        submit_pipeline_request(state, "not-a-url")
