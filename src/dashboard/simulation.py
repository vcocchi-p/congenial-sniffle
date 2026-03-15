"""Generate mock retrieval runs for the dashboard."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.dashboard.constants import DEFAULT_SOURCE_URL, RETRIEVAL_METADATA_KEYS
from src.models.documents import AgentEvent, CouncilDocument, DocumentType

DOCUMENT_BLUEPRINTS = (
    {
        "slug": "planning-committee-march-2026",
        "title": "Planning Committee March 2026 Minutes",
        "doc_type": DocumentType.MINUTES,
        "committee": "Planning Committee",
    },
    {
        "slug": "cabinet-housing-decision-elm-street",
        "title": "Cabinet Housing Decision: Elm Street",
        "doc_type": DocumentType.DECISION,
        "committee": "Cabinet Member Decisions",
    },
    {
        "slug": "transport-agenda-cycle-lanes",
        "title": "Transport Committee Agenda: Cycle Lanes",
        "doc_type": DocumentType.AGENDA,
        "committee": "Transport Committee",
    },
)


def next_run_id(sequence_number: int) -> str:
    return f"run-{sequence_number:03d}"


def _base_metadata(run_id: str, step_name: str) -> dict[str, object | None]:
    return {
        "run_id": run_id,
        "stage": "retrieval",
        "step_name": step_name,
        "source_url": None,
        "document_url": None,
        "document_title": None,
        "document_type": None,
        "progress_current": None,
        "progress_total": None,
        "detail": None,
        "trigger_type": "manual",
    }


def _build_event(
    *,
    run_id: str,
    event_type: str,
    message: str,
    timestamp: datetime,
    step_name: str,
    source_url: str | None = None,
    document_url: str | None = None,
    document_title: str | None = None,
    document_type: str | None = None,
    progress_current: int | None = None,
    progress_total: int | None = None,
    detail: str | None = None,
) -> AgentEvent:
    metadata = _base_metadata(run_id, step_name)
    metadata.update(
        {
            "source_url": source_url,
            "document_url": document_url,
            "document_title": document_title,
            "document_type": document_type,
            "progress_current": progress_current,
            "progress_total": progress_total,
            "detail": detail,
        }
    )
    return AgentEvent(
        agent_name="retriever",
        event_type=event_type,
        message=message,
        timestamp=timestamp.astimezone(timezone.utc),
        metadata=metadata,
    )


def _build_document(
    run_id: str,
    fetched_at: datetime,
    blueprint: dict[str, object],
) -> CouncilDocument:
    slug = str(blueprint["slug"])
    title = str(blueprint["title"])
    committee = str(blueprint["committee"])
    doc_type = blueprint["doc_type"]
    return CouncilDocument(
        url=f"https://committees.westminster.gov.uk/documents/{run_id}/{slug}.html",
        title=title,
        doc_type=doc_type,
        fetched_at=fetched_at.astimezone(timezone.utc),
        raw_content=(
            f"<html><head><title>{title}</title></head>"
            f"<body><h1>{title}</h1><p>{committee}</p></body></html>"
        ),
        committee=committee,
    )


def _document_events(
    *,
    run_id: str,
    source_url: str,
    document: CouncilDocument,
    index: int,
    total: int,
    started_at: datetime,
) -> list[AgentEvent]:
    fetch_time = started_at + timedelta(seconds=index * 20)
    store_time = fetch_time + timedelta(seconds=6)
    return [
        _build_event(
            run_id=run_id,
            event_type="progress",
            message=f"Fetched {document.title}",
            timestamp=fetch_time,
            step_name="document fetch",
            source_url=source_url,
            document_url=document.url,
            document_title=document.title,
            document_type=document.doc_type.value,
            progress_current=index,
            progress_total=total,
            detail="Downloaded council document HTML.",
        ),
        _build_event(
            run_id=run_id,
            event_type="progress",
            message=f"Stored {document.title}",
            timestamp=store_time,
            step_name="document wrap/store",
            source_url=source_url,
            document_url=document.url,
            document_title=document.title,
            document_type=document.doc_type.value,
            progress_current=index,
            progress_total=total,
            detail="Wrapped the document in a CouncilDocument model.",
        ),
    ]


def create_retrieval_run(
    run_id: str,
    started_at: datetime,
    *,
    source_url: str = DEFAULT_SOURCE_URL,
    outcome: str = "completed",
) -> tuple[list[AgentEvent], list[CouncilDocument]]:
    """Create a deterministic retrieval run for dashboard simulation."""
    if outcome not in {"completed", "error"}:
        raise ValueError("outcome must be 'completed' or 'error'")

    started_at = started_at.astimezone(timezone.utc)
    total_documents = len(DOCUMENT_BLUEPRINTS)
    events = [
        _build_event(
            run_id=run_id,
            event_type="started",
            message="Retrieval started",
            timestamp=started_at,
            step_name="source discovery",
            source_url=source_url,
            detail="Starting retrieval from Westminster committee listings.",
        ),
        _build_event(
            run_id=run_id,
            event_type="progress",
            message="Fetched committee source page",
            timestamp=started_at + timedelta(seconds=8),
            step_name="page fetch",
            source_url=source_url,
            detail="Downloaded the current committee landing page.",
        ),
        _build_event(
            run_id=run_id,
            event_type="progress",
            message=f"Parsed {total_documents} candidate documents",
            timestamp=started_at + timedelta(seconds=15),
            step_name="link extraction",
            source_url=source_url,
            progress_current=total_documents,
            progress_total=total_documents,
            detail="Extracted meeting and decision document links.",
        ),
    ]

    documents: list[CouncilDocument] = []
    if outcome == "completed":
        blueprints = DOCUMENT_BLUEPRINTS
    else:
        blueprints = DOCUMENT_BLUEPRINTS[:1]

    for index, blueprint in enumerate(blueprints, start=1):
        document_time = started_at + timedelta(seconds=20 + index * 20)
        document = _build_document(run_id, document_time, blueprint)
        documents.append(document)
        events.extend(
            _document_events(
                run_id=run_id,
                source_url=source_url,
                document=document,
                index=index,
                total=total_documents,
                started_at=started_at + timedelta(seconds=20),
            )
        )

    if outcome == "error":
        failed_blueprint = DOCUMENT_BLUEPRINTS[1]
        failed_url = f"https://committees.westminster.gov.uk/documents/{run_id}/{failed_blueprint['slug']}.html"
        events.append(
            _build_event(
                run_id=run_id,
                event_type="error",
                message="Document fetch failed for Cabinet Housing Decision: Elm Street",
                timestamp=started_at + timedelta(seconds=75),
                step_name="document fetch",
                source_url=source_url,
                document_url=failed_url,
                document_title=str(failed_blueprint["title"]),
                document_type=DocumentType.DECISION.value,
                progress_current=2,
                progress_total=total_documents,
                detail="The source page returned a 504 timeout.",
            )
        )
        return events, documents

    events.append(
        _build_event(
            run_id=run_id,
            event_type="completed",
            message="Retrieval completed successfully",
            timestamp=started_at + timedelta(seconds=110),
            step_name="completed",
            source_url=source_url,
            progress_current=total_documents,
            progress_total=total_documents,
            detail="Finished fetching and storing all documents for this run.",
        )
    )
    return events, documents


def metadata_keys_present(event: AgentEvent) -> bool:
    metadata = event.metadata or {}
    return all(key in metadata for key in RETRIEVAL_METADATA_KEYS)
