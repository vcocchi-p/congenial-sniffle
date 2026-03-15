"""SQLite persistence for retrieval runs, events, and normalized resources."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from src.models.documents import (
    AgendaItem,
    AgentEvent,
    Committee,
    Councillor,
    DecisionDetail,
    Meeting,
    MeetingDocument,
    RetrievalBundle,
)

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "quorum.db"


@dataclass(frozen=True)
class AnalysisInput:
    run_id: str
    item_key: str
    agenda_item: AgendaItem
    meeting: Meeting | None
    is_upcoming: bool


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_retrieval_db() -> None:
    """Create retrieval tables if they do not exist."""
    conn = _connect()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS retrieval_runs (
            run_id TEXT PRIMARY KEY,
            source_url TEXT,
            trigger_type TEXT NOT NULL,
            status TEXT NOT NULL,
            requested_at TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            error_message TEXT,
            latest_message TEXT,
            documents_discovered INTEGER NOT NULL DEFAULT 0,
            documents_fetched INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS retrieval_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            agent_name TEXT NOT NULL,
            event_type TEXT NOT NULL,
            message TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            metadata_json TEXT,
            FOREIGN KEY (run_id) REFERENCES retrieval_runs(run_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_retrieval_events_run_id
        ON retrieval_events(run_id, id);

        CREATE TABLE IF NOT EXISTS retrieval_committees (
            run_id TEXT NOT NULL,
            committee_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            PRIMARY KEY (run_id, committee_id),
            FOREIGN KEY (run_id) REFERENCES retrieval_runs(run_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS retrieval_meetings (
            run_id TEXT NOT NULL,
            meeting_id INTEGER NOT NULL,
            committee_id INTEGER NOT NULL,
            committee_name TEXT NOT NULL,
            date TEXT NOT NULL,
            url TEXT NOT NULL,
            is_upcoming INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (run_id, meeting_id),
            FOREIGN KEY (run_id) REFERENCES retrieval_runs(run_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS retrieval_attendees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            meeting_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            role TEXT NOT NULL,
            profile_url TEXT,
            FOREIGN KEY (run_id, meeting_id)
                REFERENCES retrieval_meetings(run_id, meeting_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS retrieval_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            meeting_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            doc_type TEXT NOT NULL,
            url TEXT NOT NULL,
            fetched_at TEXT,
            FOREIGN KEY (run_id, meeting_id)
                REFERENCES retrieval_meetings(run_id, meeting_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS retrieval_agenda_items (
            run_id TEXT NOT NULL,
            item_key TEXT NOT NULL,
            meeting_id INTEGER NOT NULL,
            item_number TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            decision_text TEXT NOT NULL,
            minutes_text TEXT NOT NULL,
            decision_url TEXT,
            PRIMARY KEY (run_id, item_key),
            FOREIGN KEY (run_id, meeting_id)
                REFERENCES retrieval_meetings(run_id, meeting_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_retrieval_agenda_items_run_id
        ON retrieval_agenda_items(run_id, meeting_id);

        CREATE TABLE IF NOT EXISTS retrieval_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            agenda_title TEXT NOT NULL,
            title TEXT NOT NULL,
            decision TEXT NOT NULL,
            reasons TEXT NOT NULL,
            made_by TEXT NOT NULL,
            date TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES retrieval_runs(run_id) ON DELETE CASCADE
        );
        """
    )
    conn.close()


def record_retrieval_run_started(
    run_id: str,
    *,
    source_url: str | None,
    trigger_type: str,
    requested_at: datetime,
) -> None:
    """Create or update a retrieval run record when execution begins."""
    init_retrieval_db()
    conn = _connect()
    timestamp = _serialize_datetime(requested_at)
    conn.execute(
        """
        INSERT INTO retrieval_runs (
            run_id,
            source_url,
            trigger_type,
            status,
            requested_at,
            started_at,
            completed_at,
            error_message,
            latest_message,
            documents_discovered,
            documents_fetched
        )
        VALUES (?, ?, ?, 'running', ?, ?, NULL, NULL, 'Retrieval run is in progress.', 0, 0)
        ON CONFLICT(run_id) DO UPDATE SET
            source_url = excluded.source_url,
            trigger_type = excluded.trigger_type,
            status = excluded.status,
            requested_at = excluded.requested_at,
            started_at = excluded.started_at,
            completed_at = NULL,
            error_message = NULL,
            latest_message = excluded.latest_message,
            documents_discovered = 0,
            documents_fetched = 0
        """,
        (run_id, source_url, trigger_type, timestamp, timestamp),
    )
    conn.commit()
    conn.close()


def record_retrieval_event(run_id: str, event: AgentEvent) -> None:
    """Persist a retrieval event for a run."""
    init_retrieval_db()
    conn = _connect()
    conn.execute(
        """
        INSERT INTO retrieval_events (
            run_id,
            agent_name,
            event_type,
            message,
            timestamp,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            event.agent_name,
            event.event_type,
            event.message,
            _serialize_datetime(event.timestamp),
            json.dumps(event.metadata or {}),
        ),
    )
    conn.execute(
        """
        UPDATE retrieval_runs
        SET latest_message = ?
        WHERE run_id = ?
        """,
        (event.message, run_id),
    )
    conn.commit()
    conn.close()


def record_retrieval_run_result(
    run_id: str,
    *,
    status: str,
    completed_at: datetime | None,
    latest_message: str,
    documents_discovered: int,
    documents_fetched: int,
    error_message: str | None = None,
    bundle: RetrievalBundle | None = None,
) -> None:
    """Persist the final run status and optionally the normalized bundle."""
    init_retrieval_db()
    conn = _connect()
    conn.execute(
        """
        UPDATE retrieval_runs
        SET status = ?,
            completed_at = ?,
            error_message = ?,
            latest_message = ?,
            documents_discovered = ?,
            documents_fetched = ?
        WHERE run_id = ?
        """,
        (
            status,
            _serialize_datetime(completed_at),
            error_message,
            latest_message,
            documents_discovered,
            documents_fetched,
            run_id,
        ),
    )
    if bundle is not None:
        _replace_bundle_rows(conn, run_id, bundle)
    conn.commit()
    conn.close()


def list_retrieval_runs(limit: int | None = None) -> list[dict]:
    """Return stored retrieval runs ordered newest first."""
    init_retrieval_db()
    conn = _connect()
    query = """
        SELECT
            run_id,
            source_url,
            trigger_type,
            status,
            requested_at,
            started_at,
            completed_at,
            error_message,
            latest_message,
            documents_discovered,
            documents_fetched
        FROM retrieval_runs
        ORDER BY started_at DESC
    """
    params: tuple[int, ...] | tuple[()] = ()
    if limit is not None:
        query += " LIMIT ?"
        params = (limit,)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [
        {
            "run_id": row["run_id"],
            "source_url": row["source_url"],
            "trigger_type": row["trigger_type"],
            "status": row["status"],
            "requested_at": _deserialize_datetime(row["requested_at"]),
            "started_at": _deserialize_datetime(row["started_at"]),
            "completed_at": _deserialize_datetime(row["completed_at"]),
            "error_message": row["error_message"],
            "latest_message": row["latest_message"],
            "documents_discovered": row["documents_discovered"],
            "documents_fetched": row["documents_fetched"],
        }
        for row in rows
    ]


def load_retrieval_events(run_id: str) -> list[AgentEvent]:
    """Load all persisted events for a retrieval run."""
    init_retrieval_db()
    conn = _connect()
    rows = conn.execute(
        """
        SELECT agent_name, event_type, message, timestamp, metadata_json
        FROM retrieval_events
        WHERE run_id = ?
        ORDER BY id ASC
        """,
        (run_id,),
    ).fetchall()
    conn.close()
    events: list[AgentEvent] = []
    for row in rows:
        metadata_json = row["metadata_json"]
        metadata = json.loads(metadata_json) if metadata_json else None
        events.append(
            AgentEvent(
                agent_name=row["agent_name"],
                event_type=row["event_type"],
                message=row["message"],
                timestamp=_deserialize_datetime(row["timestamp"]),
                metadata=metadata,
            )
        )
    return events


def load_retrieval_bundle(run_id: str) -> RetrievalBundle | None:
    """Reconstruct a stored retrieval bundle for a run."""
    init_retrieval_db()
    conn = _connect()
    run_row = conn.execute(
        "SELECT source_url FROM retrieval_runs WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    if run_row is None:
        conn.close()
        return None

    committee_rows = conn.execute(
        """
        SELECT committee_id, name, url
        FROM retrieval_committees
        WHERE run_id = ?
        ORDER BY rowid ASC
        """,
        (run_id,),
    ).fetchall()
    committees = [
        Committee(id=row["committee_id"], name=row["name"], url=row["url"])
        for row in committee_rows
    ]

    attendee_rows = conn.execute(
        """
        SELECT meeting_id, name, role, profile_url
        FROM retrieval_attendees
        WHERE run_id = ?
        ORDER BY id ASC
        """,
        (run_id,),
    ).fetchall()
    attendees_by_meeting: dict[int, list[Councillor]] = {}
    for row in attendee_rows:
        attendees_by_meeting.setdefault(row["meeting_id"], []).append(
            Councillor(
                name=row["name"],
                role=row["role"],
                profile_url=row["profile_url"],
            )
        )

    meeting_rows = conn.execute(
        """
        SELECT meeting_id, committee_id, committee_name, date, url, is_upcoming
        FROM retrieval_meetings
        WHERE run_id = ?
        ORDER BY rowid ASC
        """,
        (run_id,),
    ).fetchall()
    meetings = [
        Meeting(
            committee_id=row["committee_id"],
            committee_name=row["committee_name"],
            meeting_id=row["meeting_id"],
            date=row["date"],
            url=row["url"],
            is_upcoming=bool(row["is_upcoming"]),
            attendees=attendees_by_meeting.get(row["meeting_id"], []),
        )
        for row in meeting_rows
    ]

    document_rows = conn.execute(
        """
        SELECT meeting_id, title, doc_type, url, fetched_at
        FROM retrieval_documents
        WHERE run_id = ?
        ORDER BY id ASC
        """,
        (run_id,),
    ).fetchall()
    documents = [
        MeetingDocument(
            meeting_id=row["meeting_id"],
            title=row["title"],
            doc_type=row["doc_type"],
            url=row["url"],
            fetched_at=_deserialize_datetime(row["fetched_at"]),
        )
        for row in document_rows
    ]

    agenda_rows = conn.execute(
        """
        SELECT
            meeting_id,
            item_number,
            title,
            description,
            decision_text,
            minutes_text,
            decision_url
        FROM retrieval_agenda_items
        WHERE run_id = ?
        ORDER BY rowid ASC
        """,
        (run_id,),
    ).fetchall()
    agenda_items = [
        AgendaItem(
            meeting_id=row["meeting_id"],
            item_number=row["item_number"],
            title=row["title"],
            description=row["description"],
            decision_text=row["decision_text"],
            minutes_text=row["minutes_text"],
            decision_url=row["decision_url"],
        )
        for row in agenda_rows
    ]

    decision_rows = conn.execute(
        """
        SELECT agenda_title, title, decision, reasons, made_by, date
        FROM retrieval_decisions
        WHERE run_id = ?
        ORDER BY id ASC
        """,
        (run_id,),
    ).fetchall()
    decisions = [
        DecisionDetail(
            agenda_title=row["agenda_title"],
            title=row["title"],
            decision=row["decision"],
            reasons=row["reasons"],
            made_by=row["made_by"],
            date=row["date"],
        )
        for row in decision_rows
    ]
    conn.close()

    return RetrievalBundle(
        source_url=run_row["source_url"],
        committees=committees,
        meetings=meetings,
        documents=documents,
        agenda_items=agenda_items,
        decisions=decisions,
    )


def load_latest_retrieval_bundle() -> RetrievalBundle | None:
    """Load the most recently started retrieval bundle."""
    latest_run_id = get_latest_run_id()
    if latest_run_id is None:
        return None
    return load_retrieval_bundle(latest_run_id)


def get_latest_run_id() -> str | None:
    """Return the newest stored retrieval run ID."""
    init_retrieval_db()
    conn = _connect()
    row = conn.execute(
        """
        SELECT run_id
        FROM retrieval_runs
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()
    return row["run_id"] if row is not None else None


def load_analysis_inputs(run_id: str | None = None) -> list[AnalysisInput]:
    """Load normalized agenda-item inputs for the future Analysis agent."""
    resolved_run_id = run_id or get_latest_run_id()
    if resolved_run_id is None:
        return []
    bundle = load_retrieval_bundle(resolved_run_id)
    if bundle is None:
        return []

    meeting_lookup = {meeting.meeting_id: meeting for meeting in bundle.meetings}
    return [
        AnalysisInput(
            run_id=resolved_run_id,
            item_key=_item_key(item),
            agenda_item=item,
            meeting=meeting_lookup.get(item.meeting_id),
            is_upcoming=meeting_lookup.get(item.meeting_id).is_upcoming
            if meeting_lookup.get(item.meeting_id) is not None
            else False,
        )
        for item in bundle.agenda_items
    ]


def _replace_bundle_rows(conn: sqlite3.Connection, run_id: str, bundle: RetrievalBundle) -> None:
    conn.execute("DELETE FROM retrieval_committees WHERE run_id = ?", (run_id,))
    conn.execute("DELETE FROM retrieval_meetings WHERE run_id = ?", (run_id,))
    conn.execute("DELETE FROM retrieval_documents WHERE run_id = ?", (run_id,))
    conn.execute("DELETE FROM retrieval_agenda_items WHERE run_id = ?", (run_id,))
    conn.execute("DELETE FROM retrieval_decisions WHERE run_id = ?", (run_id,))

    for committee in bundle.committees:
        conn.execute(
            """
            INSERT INTO retrieval_committees (run_id, committee_id, name, url)
            VALUES (?, ?, ?, ?)
            """,
            (run_id, committee.id, committee.name, committee.url),
        )

    for meeting in bundle.meetings:
        conn.execute(
            """
            INSERT INTO retrieval_meetings (
                run_id,
                meeting_id,
                committee_id,
                committee_name,
                date,
                url,
                is_upcoming
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                meeting.meeting_id,
                meeting.committee_id,
                meeting.committee_name,
                meeting.date,
                meeting.url,
                int(meeting.is_upcoming),
            ),
        )
        for councillor in meeting.attendees:
            conn.execute(
                """
                INSERT INTO retrieval_attendees (
                    run_id,
                    meeting_id,
                    name,
                    role,
                    profile_url
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    meeting.meeting_id,
                    councillor.name,
                    councillor.role,
                    councillor.profile_url,
                ),
            )

    for document in bundle.documents:
        conn.execute(
            """
            INSERT INTO retrieval_documents (
                run_id,
                meeting_id,
                title,
                doc_type,
                url,
                fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                document.meeting_id,
                document.title,
                document.doc_type.value,
                document.url,
                _serialize_datetime(document.fetched_at),
            ),
        )

    for item in bundle.agenda_items:
        conn.execute(
            """
            INSERT INTO retrieval_agenda_items (
                run_id,
                item_key,
                meeting_id,
                item_number,
                title,
                description,
                decision_text,
                minutes_text,
                decision_url
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                _item_key(item),
                item.meeting_id,
                item.item_number,
                item.title,
                item.description,
                item.decision_text,
                item.minutes_text,
                item.decision_url,
            ),
        )

    for decision in bundle.decisions:
        conn.execute(
            """
            INSERT INTO retrieval_decisions (
                run_id,
                agenda_title,
                title,
                decision,
                reasons,
                made_by,
                date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                decision.agenda_title,
                decision.title,
                decision.decision,
                decision.reasons,
                decision.made_by,
                decision.date,
            ),
        )


def _item_key(item: AgendaItem) -> str:
    return f"{item.meeting_id}-{item.item_number}"


def _serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _deserialize_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None
