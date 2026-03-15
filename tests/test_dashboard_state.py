"""Tests for dashboard state derivation."""

from datetime import timezone
from pathlib import Path

import pytest

from src.analysis import db as analysis_db
from src.analysis.db import (
    init_analysis_db,
    load_analysis_result_for_retrieval_run,
    record_analysis_run_result,
    record_analysis_run_started,
    record_meeting_selection,
)
from src.dashboard.mock_data import create_seeded_dashboard_data
from src.dashboard.state import (
    get_current_run_id,
    get_selected_run_id,
    initialize_state,
    load_analysis_overview,
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
from src.models.analysis import (
    AgendaItemAnalysis,
    AnalysisRun,
    MeetingAnalysisResult,
    MeetingSelection,
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
from src.retrieval import db as retrieval_db
from src.retrieval.db import (
    get_latest_run_sequence,
    list_retrieval_runs,
    record_retrieval_run_started,
)
from src.retrieval.db import (
    load_retrieval_bundle as load_persisted_bundle,
)
from src.retrieval.db import (
    load_retrieval_events as load_persisted_events,
)


@pytest.fixture(autouse=True)
def tmp_retrieval_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    test_db = tmp_path / "test_quorum.db"
    monkeypatch.setattr(retrieval_db, "DB_PATH", test_db)
    monkeypatch.setattr(analysis_db, "DB_PATH", test_db)
    retrieval_db.init_retrieval_db()
    init_analysis_db()
    return test_db


def test_initialize_state_populates_empty_mapping():
    state = {}

    initialize_state(state)

    assert get_current_run_id(state) is None
    assert get_selected_run_id(state) == "run-003"
    assert len(load_recent_runs(state)) == 3


def test_load_stage_snapshots_marks_analysis_as_idle_without_runs():
    state = create_seeded_dashboard_data()

    stages = load_stage_snapshots(state)

    assert stages[0].stage == "retrieval"
    assert stages[0].status == "idle"
    assert stages[1].stage == "analysis"
    assert stages[1].status == "idle"
    assert all(stage.status == "placeholder" for stage in stages[2:])


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
    run_timestamp = state["dashboard_run_events"]["run-003"][-1].timestamp

    async def fake_pipeline_runner(*, run_id, source_url, trigger_type, on_event):
        on_event(
            AgentEvent(
                agent_name="retriever",
                event_type="started",
                message="Started fake retrieval",
                timestamp=run_timestamp,
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
                timestamp=run_timestamp,
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

    def fake_analysis_runner(*, run_id, preferred_meeting_id, analysis_mode, on_event):
        on_event(
            AgentEvent(
                agent_name="analysis",
                event_type="started",
                message="Selected Cabinet meeting 6718 for voter analysis",
                timestamp=run_timestamp,
                metadata={
                    "run_id": run_id,
                    "stage": "analysis",
                    "step_name": "meeting selection",
                    "source_url": "https://committees.westminster.gov.uk/ieListDocuments.aspx?CId=130&MId=6718&Ver=4",
                    "detail": "Pinned for the demo.",
                    "trigger_type": "manual",
                    "meeting_id": preferred_meeting_id,
                    "committee": "Cabinet",
                },
            )
        )
        on_event(
            AgentEvent(
                agent_name="analysis",
                event_type="completed",
                message="Generated 1 voter briefs for meeting 6718",
                timestamp=run_timestamp.replace(tzinfo=timezone.utc),
                metadata={
                    "run_id": run_id,
                    "stage": "analysis",
                    "step_name": "completed",
                    "source_url": "https://committees.westminster.gov.uk/ieListDocuments.aspx?CId=130&MId=6718&Ver=4",
                    "detail": "Persisted analysis output.",
                    "trigger_type": "manual",
                    "meeting_id": preferred_meeting_id,
                    "committee": "Cabinet",
                },
            )
        )

        selection = MeetingSelection(
            retrieval_run_id=run_id,
            meeting_id=preferred_meeting_id,
            committee_name="Cabinet",
            meeting_date="23 Feb 2026 6.30 pm",
            analysis_mode=analysis_mode,
            priority_score=999.0,
            reason_selected="Pinned for the demo.",
            selected_at=run_timestamp,
            item_keys=["6718-4"],
        )
        run = AnalysisRun(
            analysis_run_id="analysis-run-001",
            retrieval_run_id=run_id,
            meeting_id=preferred_meeting_id,
            analysis_mode=analysis_mode,
            status="running",
            model="gpt-4o",
            selected_reason=selection.reason_selected,
            started_at=run_timestamp,
        )
        item = AgendaItemAnalysis(
            analysis_run_id=run.analysis_run_id,
            retrieval_run_id=run_id,
            meeting_id=preferred_meeting_id,
            item_key="6718-4",
            item_number="4",
            title="Budget",
            plain_summary="Budget summary",
            why_it_matters="Budget matters",
            pros=["Protects services"],
            cons=["Could raise costs"],
            what_to_watch="Watch council tax",
            councillors_involved=["Leader"],
            source_urls=["https://committees.westminster.gov.uk/ieListDocuments.aspx?CId=130&MId=6718&Ver=4"],
            notify_voters=True,
            analysis_mode=analysis_mode,
            created_at=run_timestamp,
        )
        record_meeting_selection(selection)
        record_analysis_run_started(run)
        record_analysis_run_result(
            run.analysis_run_id,
            status="completed",
            completed_at=run_timestamp,
            items=[item],
        )
        return MeetingAnalysisResult(
            selection=selection,
            run=run.model_copy(update={"status": "completed", "completed_at": run_timestamp}),
            items=[item],
        )

    request = start_retrieval_run(
        state,
        "https://committees.westminster.gov.uk/ieListMeetings.aspx?CId=130&Year=0",
        pipeline_runner=fake_pipeline_runner,
        analysis_runner=fake_analysis_runner,
    )

    requests = load_pipeline_requests(state)
    overview = load_retrieval_overview(state, request.run_id)
    stages = load_stage_snapshots(state)
    bundle = load_run_bundle(state, request.run_id)
    analysis_overview = load_analysis_overview(request.run_id)
    metrics = load_global_metrics(state)
    persisted_runs = list_retrieval_runs()
    persisted_events = load_persisted_events(request.run_id)
    persisted_bundle = load_persisted_bundle(request.run_id)
    persisted_analysis = load_analysis_result_for_retrieval_run(request.run_id)

    assert request.request_id == "request-001"
    assert request.status == "completed"
    assert requests[0].run_id == request.run_id
    assert get_current_run_id(state) == request.run_id
    assert overview["status"] == "completed"
    assert bundle is not None
    assert len(bundle.documents) == 1
    assert stages[0].status == "completed"
    assert stages[1].status == "completed"
    assert analysis_overview is not None
    assert analysis_overview.items_generated == 1
    assert metrics["summaries_generated"] == 1
    assert persisted_runs[0]["run_id"] == request.run_id
    assert persisted_events[-1].event_type == "completed"
    assert persisted_bundle is not None
    assert persisted_bundle.documents[0].title == "Budget Report"
    assert persisted_analysis is not None
    assert persisted_analysis.items[0].title == "Budget"


def test_start_retrieval_run_rejects_invalid_url():
    state = create_seeded_dashboard_data()

    with pytest.raises(ValueError):
        start_retrieval_run(state, "https://example.com/not-westminster")


def test_start_retrieval_run_uses_persisted_run_sequence_after_restart():
    state = {
        "dashboard_run_events": {},
        "dashboard_run_documents": {},
        "dashboard_run_bundles": {},
        "dashboard_run_order": [],
        "dashboard_current_run_id": None,
        "dashboard_selected_run_id": None,
        "dashboard_run_sequence": 0,
        "dashboard_summaries_generated": 0,
        "dashboard_pipeline_requests": [],
        "dashboard_request_sequence": 0,
    }
    requested_at = create_seeded_dashboard_data()["dashboard_run_events"]["run-003"][-1].timestamp
    record_retrieval_run_started(
        "run-007",
        source_url="https://committees.westminster.gov.uk/ieListMeetings.aspx?CId=130&Year=0",
        trigger_type="manual",
        requested_at=requested_at,
    )

    async def fake_pipeline_runner(*, run_id, source_url, trigger_type, on_event):
        on_event(
            AgentEvent(
                agent_name="retriever",
                event_type="completed",
                message="Completed fake retrieval",
                timestamp=requested_at,
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
        return RetrievalBundle(source_url=source_url)

    request = start_retrieval_run(
        state,
        "https://committees.westminster.gov.uk/ieListMeetings.aspx?CId=130&Year=0",
        pipeline_runner=fake_pipeline_runner,
    )

    assert get_latest_run_sequence() == 8
    assert request.run_id == "run-008"
