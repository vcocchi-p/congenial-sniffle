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
    load_run_bundle,
    load_stage_snapshots,
    select_run,
    start_retrieval_run,
)
from src.models.documents import (
    AgendaItem,
    AgentEvent,
    Committee,
    DecisionDetail,
    DocumentType,
    Meeting,
    MeetingDocument,
    RetrievalBundle,
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
    assert metrics["manual_requests"] == 0
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


def test_start_retrieval_run_stores_bundle_and_updates_history():
    state = create_seeded_dashboard_data()

    async def fake_pipeline_runner(*, run_id, source_url, trigger_type, on_event):
        on_event(
            AgentEvent(
                agent_name="retriever",
                event_type="started",
                message="Started fake retrieval",
                timestamp=state["dashboard_run_events"]["run-003"][-1].timestamp,
                metadata={
                    "run_id": run_id,
                    "stage": "retrieval",
                    "step_name": "source discovery",
                    "source_url": source_url,
                    "detail": "Fake run for unit tests.",
                    "trigger_type": trigger_type,
                },
            )
        )
        on_event(
            AgentEvent(
                agent_name="retriever",
                event_type="completed",
                message="Completed fake retrieval",
                timestamp=state["dashboard_run_events"]["run-003"][-1].timestamp,
                metadata={
                    "run_id": run_id,
                    "stage": "retrieval",
                    "step_name": "completed",
                    "source_url": source_url,
                    "detail": "Fake run completed for unit tests.",
                    "trigger_type": trigger_type,
                },
            )
        )
        return RetrievalBundle(
            source_url=source_url,
            committees=[Committee(id=130, name="Cabinet", url=source_url)],
            meetings=[
                Meeting(
                    committee_id=130,
                    committee_name="Cabinet",
                    meeting_id=9001,
                    date="15 Mar 2026 6.30 pm",
                    url=source_url,
                )
            ],
            documents=[
                MeetingDocument(
                    meeting_id=9001,
                    title="Budget Report",
                    doc_type=DocumentType.AGENDA,
                    url=f"{source_url}/budget.pdf",
                )
            ],
            agenda_items=[
                AgendaItem(
                    meeting_id=9001,
                    item_number="4",
                    title="Budget",
                    description="Approve the annual budget",
                )
            ],
            decisions=[
                DecisionDetail(
                    agenda_title="Budget",
                    title="Decision - Budget",
                    decision="Approved",
                    made_by="Cabinet",
                )
            ],
        )

    request = start_retrieval_run(
        state,
        "https://committees.westminster.gov.uk/ieListMeetings.aspx?CId=130&Year=0",
        pipeline_runner=fake_pipeline_runner,
    )

    requests = load_pipeline_requests(state)
    overview = load_retrieval_overview(state, request.run_id)
    stages = load_stage_snapshots(state)
    bundle = load_run_bundle(state, request.run_id)

    assert request.request_id == "request-001"
    assert request.status == "completed"
    assert requests[0].run_id == request.run_id
    assert get_current_run_id(state) == request.run_id
    assert overview["status"] == "completed"
    assert bundle is not None
    assert len(bundle.documents) == 1
    assert stages[0].status == "completed"


def test_start_retrieval_run_rejects_invalid_url():
    state = create_seeded_dashboard_data()

    with pytest.raises(ValueError):
        start_retrieval_run(state, "https://example.com/not-westminster")
