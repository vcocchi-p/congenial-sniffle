"""Seed deterministic dashboard state."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from src.dashboard.constants import (
    DASHBOARD_CURRENT_RUN_KEY,
    DASHBOARD_PIPELINE_REQUESTS_KEY,
    DASHBOARD_REQUEST_SEQUENCE_KEY,
    DASHBOARD_RUN_BUNDLES_KEY,
    DASHBOARD_RUN_DOCUMENTS_KEY,
    DASHBOARD_RUN_EVENTS_KEY,
    DASHBOARD_RUN_ORDER_KEY,
    DASHBOARD_RUN_SEQUENCE_KEY,
    DASHBOARD_SELECTED_RUN_KEY,
    DASHBOARD_SUMMARIES_GENERATED_KEY,
)
from src.dashboard.simulation import create_retrieval_run, next_run_id


def create_seeded_dashboard_data() -> dict[str, Any]:
    """Return deterministic mock state for the monitoring dashboard."""
    base_time = datetime(2026, 3, 15, 9, 0, tzinfo=timezone.utc)
    run_specs = (
        ("completed", base_time),
        ("error", base_time + timedelta(hours=1)),
        ("completed", base_time + timedelta(hours=2)),
    )

    run_events: dict[str, list] = {}
    run_documents: dict[str, list] = {}
    run_order: list[str] = []

    for sequence, (outcome, started_at) in enumerate(run_specs, start=1):
        run_id = next_run_id(sequence)
        events, documents = create_retrieval_run(run_id, started_at, outcome=outcome)
        run_order.append(run_id)
        run_events[run_id] = events
        run_documents[run_id] = documents

    current_run_id = run_order[-1]
    return {
        DASHBOARD_RUN_EVENTS_KEY: run_events,
        DASHBOARD_RUN_DOCUMENTS_KEY: run_documents,
        DASHBOARD_RUN_BUNDLES_KEY: {},
        DASHBOARD_RUN_ORDER_KEY: run_order,
        DASHBOARD_CURRENT_RUN_KEY: None,
        DASHBOARD_SELECTED_RUN_KEY: current_run_id,
        DASHBOARD_RUN_SEQUENCE_KEY: len(run_order),
        DASHBOARD_SUMMARIES_GENERATED_KEY: 0,
        DASHBOARD_PIPELINE_REQUESTS_KEY: [],
        DASHBOARD_REQUEST_SEQUENCE_KEY: 0,
    }
