from __future__ import annotations

PIPELINE_STAGES = ("retrieval", "analysis", "conversation", "app", "summary")

STAGE_LABELS = {
    "retrieval": "Retrieval",
    "analysis": "Analysis",
    "conversation": "Conversation",
    "app": "App",
    "summary": "Summary",
}

PLACEHOLDER_STAGE_MESSAGE = "Future stage placeholder for monitoring."

DASHBOARD_RUN_EVENTS_KEY = "dashboard_run_events"
DASHBOARD_RUN_DOCUMENTS_KEY = "dashboard_run_documents"
DASHBOARD_RUN_BUNDLES_KEY = "dashboard_run_bundles"
DASHBOARD_RUN_ORDER_KEY = "dashboard_run_order"
DASHBOARD_CURRENT_RUN_KEY = "dashboard_current_run_id"
DASHBOARD_SELECTED_RUN_KEY = "dashboard_selected_run_id"
DASHBOARD_RUN_SEQUENCE_KEY = "dashboard_run_sequence"
DASHBOARD_SUMMARIES_GENERATED_KEY = "dashboard_summaries_generated"
DASHBOARD_PIPELINE_REQUESTS_KEY = "dashboard_pipeline_requests"
DASHBOARD_REQUEST_SEQUENCE_KEY = "dashboard_request_sequence"

DEFAULT_SOURCE_URL = "https://committees.westminster.gov.uk/ieDocHome.aspx?Categories="
HISTORY_LIMIT = 5

RETRIEVAL_METADATA_KEYS = (
    "run_id",
    "stage",
    "step_name",
    "source_url",
    "document_url",
    "document_title",
    "document_type",
    "progress_current",
    "progress_total",
    "detail",
    "trigger_type",
)

RESOURCE_SECTIONS = ("committees", "meetings", "documents", "agenda_items", "decisions")
