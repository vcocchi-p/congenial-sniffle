"""Tests for seeded dashboard fixtures."""

from src.dashboard.constants import (
    DASHBOARD_CURRENT_RUN_KEY,
    DASHBOARD_PIPELINE_REQUESTS_KEY,
    DASHBOARD_RUN_BUNDLES_KEY,
    DASHBOARD_RUN_DOCUMENTS_KEY,
    DASHBOARD_RUN_EVENTS_KEY,
    DASHBOARD_RUN_ORDER_KEY,
    DASHBOARD_SELECTED_RUN_KEY,
)
from src.dashboard.mock_data import create_seeded_dashboard_data
from src.dashboard.simulation import metadata_keys_present


def test_seeded_dashboard_data_is_deterministic():
    first = create_seeded_dashboard_data()
    second = create_seeded_dashboard_data()

    assert first[DASHBOARD_RUN_ORDER_KEY] == ["run-001", "run-002", "run-003"]
    assert first[DASHBOARD_RUN_ORDER_KEY] == second[DASHBOARD_RUN_ORDER_KEY]
    assert first[DASHBOARD_CURRENT_RUN_KEY] is None
    assert first[DASHBOARD_SELECTED_RUN_KEY] == "run-003"
    assert first[DASHBOARD_PIPELINE_REQUESTS_KEY] == []
    assert first[DASHBOARD_RUN_BUNDLES_KEY] == {}
    assert (
        first[DASHBOARD_RUN_EVENTS_KEY]["run-003"][-1].timestamp
        == second[DASHBOARD_RUN_EVENTS_KEY]["run-003"][-1].timestamp
    )


def test_seeded_dashboard_data_has_expected_documents():
    seeded = create_seeded_dashboard_data()

    assert len(seeded[DASHBOARD_RUN_DOCUMENTS_KEY]["run-001"]) == 3
    assert len(seeded[DASHBOARD_RUN_DOCUMENTS_KEY]["run-002"]) == 1
    assert len(seeded[DASHBOARD_RUN_DOCUMENTS_KEY]["run-003"]) == 3


def test_seeded_events_use_standard_metadata_keys():
    seeded = create_seeded_dashboard_data()

    for events in seeded[DASHBOARD_RUN_EVENTS_KEY].values():
        assert all(metadata_keys_present(event) for event in events)
