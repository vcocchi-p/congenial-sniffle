"""SQLite persistence for retrieval runs, events, and normalized resources."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

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

        CREATE TABLE IF NOT EXISTS retrieval_committee_versions (
            committee_version_id INTEGER PRIMARY KEY AUTOINCREMENT,
            committee_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_seen_run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            UNIQUE(committee_id, content_hash)
        );
        CREATE TABLE IF NOT EXISTS retrieval_run_committees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            committee_version_id INTEGER NOT NULL,
            UNIQUE(run_id, committee_version_id),
            FOREIGN KEY (run_id) REFERENCES retrieval_runs(run_id) ON DELETE CASCADE,
            FOREIGN KEY (committee_version_id)
                REFERENCES retrieval_committee_versions(committee_version_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS retrieval_meeting_versions (
            meeting_version_id INTEGER PRIMARY KEY AUTOINCREMENT,
            meeting_id INTEGER NOT NULL,
            committee_id INTEGER NOT NULL,
            committee_name TEXT NOT NULL,
            date TEXT NOT NULL,
            url TEXT NOT NULL,
            is_upcoming INTEGER NOT NULL DEFAULT 0,
            content_hash TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_seen_run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            UNIQUE(meeting_id, content_hash)
        );
        CREATE TABLE IF NOT EXISTS retrieval_run_meetings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            meeting_version_id INTEGER NOT NULL,
            UNIQUE(run_id, meeting_version_id),
            FOREIGN KEY (run_id) REFERENCES retrieval_runs(run_id) ON DELETE CASCADE,
            FOREIGN KEY (meeting_version_id)
                REFERENCES retrieval_meeting_versions(meeting_version_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS retrieval_attendee_versions (
            attendee_version_id INTEGER PRIMARY KEY AUTOINCREMENT,
            attendee_key TEXT NOT NULL,
            meeting_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            role TEXT NOT NULL,
            profile_url TEXT,
            content_hash TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_seen_run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            UNIQUE(attendee_key, content_hash)
        );
        CREATE TABLE IF NOT EXISTS retrieval_run_attendees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            attendee_version_id INTEGER NOT NULL,
            UNIQUE(run_id, attendee_version_id),
            FOREIGN KEY (run_id) REFERENCES retrieval_runs(run_id) ON DELETE CASCADE,
            FOREIGN KEY (attendee_version_id)
                REFERENCES retrieval_attendee_versions(attendee_version_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS retrieval_document_versions (
            document_version_id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_key TEXT NOT NULL,
            meeting_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            doc_type TEXT NOT NULL,
            url TEXT NOT NULL,
            fetched_at TEXT,
            content_hash TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_seen_run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            UNIQUE(document_key, content_hash)
        );
        CREATE TABLE IF NOT EXISTS retrieval_run_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            document_version_id INTEGER NOT NULL,
            UNIQUE(run_id, document_version_id),
            FOREIGN KEY (run_id) REFERENCES retrieval_runs(run_id) ON DELETE CASCADE,
            FOREIGN KEY (document_version_id)
                REFERENCES retrieval_document_versions(document_version_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS retrieval_agenda_item_versions (
            agenda_item_version_id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_key TEXT NOT NULL,
            meeting_id INTEGER NOT NULL,
            item_number TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            decision_text TEXT NOT NULL,
            minutes_text TEXT NOT NULL,
            decision_url TEXT,
            content_hash TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_seen_run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            UNIQUE(item_key, content_hash)
        );
        CREATE TABLE IF NOT EXISTS retrieval_run_agenda_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            agenda_item_version_id INTEGER NOT NULL,
            UNIQUE(run_id, agenda_item_version_id),
            FOREIGN KEY (run_id) REFERENCES retrieval_runs(run_id) ON DELETE CASCADE,
            FOREIGN KEY (agenda_item_version_id)
                REFERENCES retrieval_agenda_item_versions(agenda_item_version_id)
                ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS retrieval_decision_versions (
            decision_version_id INTEGER PRIMARY KEY AUTOINCREMENT,
            decision_key TEXT NOT NULL,
            agenda_title TEXT NOT NULL,
            title TEXT NOT NULL,
            decision TEXT NOT NULL,
            reasons TEXT NOT NULL,
            made_by TEXT NOT NULL,
            date TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_seen_run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            UNIQUE(decision_key, content_hash)
        );
        CREATE TABLE IF NOT EXISTS retrieval_run_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            decision_version_id INTEGER NOT NULL,
            UNIQUE(run_id, decision_version_id),
            FOREIGN KEY (run_id) REFERENCES retrieval_runs(run_id) ON DELETE CASCADE,
            FOREIGN KEY (decision_version_id)
                REFERENCES retrieval_decision_versions(decision_version_id) ON DELETE CASCADE
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
    conn.execute("DELETE FROM retrieval_events WHERE run_id = ?", (run_id,))
    _clear_run_links(conn, run_id)
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
        "UPDATE retrieval_runs SET latest_message = ? WHERE run_id = ?",
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
        _clear_run_links(conn, run_id)
        _link_bundle_rows(
            conn,
            run_id,
            bundle,
            seen_at=completed_at or datetime.utcnow(),
        )
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

    bundle = _load_versioned_bundle(conn, run_id, run_row["source_url"])
    if bundle is None:
        bundle = _load_legacy_bundle(conn, run_id, run_row["source_url"])
    conn.close()
    return bundle


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


def get_latest_run_sequence() -> int:
    """Return the highest numeric run sequence seen so far."""
    init_retrieval_db()
    conn = _connect()
    rows = conn.execute("SELECT run_id FROM retrieval_runs").fetchall()
    conn.close()
    sequences = []
    for row in rows:
        match = re.fullmatch(r"run-(\d+)", row["run_id"])
        if match:
            sequences.append(int(match.group(1)))
    return max(sequences, default=0)


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


def _load_versioned_bundle(
    conn: sqlite3.Connection,
    run_id: str,
    source_url: str | None,
) -> RetrievalBundle | None:
    committee_rows = conn.execute(
        """
        SELECT v.committee_id, v.name, v.url
        FROM retrieval_run_committees rc
        JOIN retrieval_committee_versions v
            ON rc.committee_version_id = v.committee_version_id
        WHERE rc.run_id = ?
        ORDER BY rc.id ASC
        """,
        (run_id,),
    ).fetchall()
    if not committee_rows:
        return None

    committees = [
        Committee(id=row["committee_id"], name=row["name"], url=row["url"])
        for row in committee_rows
    ]

    attendee_rows = conn.execute(
        """
        SELECT v.meeting_id, v.name, v.role, v.profile_url
        FROM retrieval_run_attendees ra
        JOIN retrieval_attendee_versions v
            ON ra.attendee_version_id = v.attendee_version_id
        WHERE ra.run_id = ?
        ORDER BY ra.id ASC
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
        SELECT
            v.meeting_id,
            v.committee_id,
            v.committee_name,
            v.date,
            v.url,
            v.is_upcoming
        FROM retrieval_run_meetings rm
        JOIN retrieval_meeting_versions v
            ON rm.meeting_version_id = v.meeting_version_id
        WHERE rm.run_id = ?
        ORDER BY rm.id ASC
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
        SELECT v.meeting_id, v.title, v.doc_type, v.url, v.fetched_at
        FROM retrieval_run_documents rd
        JOIN retrieval_document_versions v
            ON rd.document_version_id = v.document_version_id
        WHERE rd.run_id = ?
        ORDER BY rd.id ASC
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
            v.meeting_id,
            v.item_number,
            v.title,
            v.description,
            v.decision_text,
            v.minutes_text,
            v.decision_url
        FROM retrieval_run_agenda_items rai
        JOIN retrieval_agenda_item_versions v
            ON rai.agenda_item_version_id = v.agenda_item_version_id
        WHERE rai.run_id = ?
        ORDER BY rai.id ASC
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
        SELECT
            v.agenda_title,
            v.title,
            v.decision,
            v.reasons,
            v.made_by,
            v.date
        FROM retrieval_run_decisions rd
        JOIN retrieval_decision_versions v
            ON rd.decision_version_id = v.decision_version_id
        WHERE rd.run_id = ?
        ORDER BY rd.id ASC
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

    return RetrievalBundle(
        source_url=source_url,
        committees=committees,
        meetings=meetings,
        documents=documents,
        agenda_items=agenda_items,
        decisions=decisions,
    )


def _load_legacy_bundle(
    conn: sqlite3.Connection,
    run_id: str,
    source_url: str | None,
) -> RetrievalBundle:
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

    return RetrievalBundle(
        source_url=source_url,
        committees=committees,
        meetings=meetings,
        documents=documents,
        agenda_items=agenda_items,
        decisions=decisions,
    )


def _clear_run_links(conn: sqlite3.Connection, run_id: str) -> None:
    for table in (
        "retrieval_run_committees",
        "retrieval_run_meetings",
        "retrieval_run_attendees",
        "retrieval_run_documents",
        "retrieval_run_agenda_items",
        "retrieval_run_decisions",
    ):
        conn.execute(f"DELETE FROM {table} WHERE run_id = ?", (run_id,))


def _link_bundle_rows(
    conn: sqlite3.Connection,
    run_id: str,
    bundle: RetrievalBundle,
    *,
    seen_at: datetime,
) -> None:
    seen_at_text = _serialize_datetime(seen_at)

    for committee in bundle.committees:
        version_id = _upsert_committee_version(conn, committee, run_id, seen_at_text)
        _insert_run_link(
            conn,
            "retrieval_run_committees",
            "committee_version_id",
            run_id,
            version_id,
        )

    for meeting in bundle.meetings:
        version_id = _upsert_meeting_version(conn, meeting, run_id, seen_at_text)
        _insert_run_link(conn, "retrieval_run_meetings", "meeting_version_id", run_id, version_id)
        for councillor in meeting.attendees:
            attendee_id = _upsert_attendee_version(
                conn,
                meeting.meeting_id,
                councillor,
                run_id,
                seen_at_text,
            )
            _insert_run_link(
                conn,
                "retrieval_run_attendees",
                "attendee_version_id",
                run_id,
                attendee_id,
            )

    for document in bundle.documents:
        version_id = _upsert_document_version(conn, document, run_id, seen_at_text)
        _insert_run_link(conn, "retrieval_run_documents", "document_version_id", run_id, version_id)

    for item in bundle.agenda_items:
        version_id = _upsert_agenda_item_version(conn, item, run_id, seen_at_text)
        _insert_run_link(
            conn,
            "retrieval_run_agenda_items",
            "agenda_item_version_id",
            run_id,
            version_id,
        )

    for decision in bundle.decisions:
        version_id = _upsert_decision_version(conn, decision, run_id, seen_at_text)
        _insert_run_link(
            conn,
            "retrieval_run_decisions",
            "decision_version_id",
            run_id,
            version_id,
        )


def _insert_run_link(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    run_id: str,
    version_id: int,
) -> None:
    conn.execute(
        f"INSERT OR IGNORE INTO {table} (run_id, {column}) VALUES (?, ?)",
        (run_id, version_id),
    )


def _upsert_committee_version(
    conn: sqlite3.Connection,
    committee: Committee,
    run_id: str,
    seen_at_text: str,
) -> int:
    payload = {"name": committee.name, "url": committee.url}
    content_hash = _hash_payload(payload)
    row = conn.execute(
        """
        SELECT committee_version_id
        FROM retrieval_committee_versions
        WHERE committee_id = ? AND content_hash = ?
        """,
        (committee.id, content_hash),
    ).fetchone()
    if row is not None:
        version_id = row["committee_version_id"]
        conn.execute(
            """
            UPDATE retrieval_committee_versions
            SET last_seen_run_id = ?, last_seen_at = ?
            WHERE committee_version_id = ?
            """,
            (run_id, seen_at_text, version_id),
        )
        return version_id

    cursor = conn.execute(
        """
        INSERT INTO retrieval_committee_versions (
            committee_id,
            name,
            url,
            content_hash,
            first_seen_run_id,
            last_seen_run_id,
            created_at,
            last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            committee.id,
            committee.name,
            committee.url,
            content_hash,
            run_id,
            run_id,
            seen_at_text,
            seen_at_text,
        ),
    )
    return int(cursor.lastrowid)


def _upsert_meeting_version(
    conn: sqlite3.Connection,
    meeting: Meeting,
    run_id: str,
    seen_at_text: str,
) -> int:
    payload = {
        "committee_id": meeting.committee_id,
        "committee_name": meeting.committee_name,
        "date": meeting.date,
        "url": meeting.url,
        "is_upcoming": meeting.is_upcoming,
    }
    content_hash = _hash_payload(payload)
    row = conn.execute(
        """
        SELECT meeting_version_id
        FROM retrieval_meeting_versions
        WHERE meeting_id = ? AND content_hash = ?
        """,
        (meeting.meeting_id, content_hash),
    ).fetchone()
    if row is not None:
        version_id = row["meeting_version_id"]
        conn.execute(
            """
            UPDATE retrieval_meeting_versions
            SET last_seen_run_id = ?, last_seen_at = ?
            WHERE meeting_version_id = ?
            """,
            (run_id, seen_at_text, version_id),
        )
        return version_id

    cursor = conn.execute(
        """
        INSERT INTO retrieval_meeting_versions (
            meeting_id,
            committee_id,
            committee_name,
            date,
            url,
            is_upcoming,
            content_hash,
            first_seen_run_id,
            last_seen_run_id,
            created_at,
            last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            meeting.meeting_id,
            meeting.committee_id,
            meeting.committee_name,
            meeting.date,
            meeting.url,
            int(meeting.is_upcoming),
            content_hash,
            run_id,
            run_id,
            seen_at_text,
            seen_at_text,
        ),
    )
    return int(cursor.lastrowid)


def _upsert_attendee_version(
    conn: sqlite3.Connection,
    meeting_id: int,
    councillor: Councillor,
    run_id: str,
    seen_at_text: str,
) -> int:
    attendee_key = f"{meeting_id}:{councillor.name}"
    payload = {
        "name": councillor.name,
        "role": councillor.role,
        "profile_url": councillor.profile_url,
    }
    content_hash = _hash_payload(payload)
    row = conn.execute(
        """
        SELECT attendee_version_id
        FROM retrieval_attendee_versions
        WHERE attendee_key = ? AND content_hash = ?
        """,
        (attendee_key, content_hash),
    ).fetchone()
    if row is not None:
        version_id = row["attendee_version_id"]
        conn.execute(
            """
            UPDATE retrieval_attendee_versions
            SET last_seen_run_id = ?, last_seen_at = ?
            WHERE attendee_version_id = ?
            """,
            (run_id, seen_at_text, version_id),
        )
        return version_id

    cursor = conn.execute(
        """
        INSERT INTO retrieval_attendee_versions (
            attendee_key,
            meeting_id,
            name,
            role,
            profile_url,
            content_hash,
            first_seen_run_id,
            last_seen_run_id,
            created_at,
            last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            attendee_key,
            meeting_id,
            councillor.name,
            councillor.role,
            councillor.profile_url,
            content_hash,
            run_id,
            run_id,
            seen_at_text,
            seen_at_text,
        ),
    )
    return int(cursor.lastrowid)


def _upsert_document_version(
    conn: sqlite3.Connection,
    document: MeetingDocument,
    run_id: str,
    seen_at_text: str,
) -> int:
    document_key = document.url
    payload = {
        "meeting_id": document.meeting_id,
        "title": document.title,
        "doc_type": document.doc_type.value,
        "url": document.url,
        "fetched_at": _serialize_datetime(document.fetched_at),
    }
    content_hash = _hash_payload(payload)
    row = conn.execute(
        """
        SELECT document_version_id
        FROM retrieval_document_versions
        WHERE document_key = ? AND content_hash = ?
        """,
        (document_key, content_hash),
    ).fetchone()
    if row is not None:
        version_id = row["document_version_id"]
        conn.execute(
            """
            UPDATE retrieval_document_versions
            SET last_seen_run_id = ?, last_seen_at = ?
            WHERE document_version_id = ?
            """,
            (run_id, seen_at_text, version_id),
        )
        return version_id

    cursor = conn.execute(
        """
        INSERT INTO retrieval_document_versions (
            document_key,
            meeting_id,
            title,
            doc_type,
            url,
            fetched_at,
            content_hash,
            first_seen_run_id,
            last_seen_run_id,
            created_at,
            last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            document_key,
            document.meeting_id,
            document.title,
            document.doc_type.value,
            document.url,
            _serialize_datetime(document.fetched_at),
            content_hash,
            run_id,
            run_id,
            seen_at_text,
            seen_at_text,
        ),
    )
    return int(cursor.lastrowid)


def _upsert_agenda_item_version(
    conn: sqlite3.Connection,
    item: AgendaItem,
    run_id: str,
    seen_at_text: str,
) -> int:
    item_key = _item_key(item)
    payload = {
        "meeting_id": item.meeting_id,
        "item_number": item.item_number,
        "title": item.title,
        "description": item.description,
        "decision_text": item.decision_text,
        "minutes_text": item.minutes_text,
        "decision_url": item.decision_url,
    }
    content_hash = _hash_payload(payload)
    row = conn.execute(
        """
        SELECT agenda_item_version_id
        FROM retrieval_agenda_item_versions
        WHERE item_key = ? AND content_hash = ?
        """,
        (item_key, content_hash),
    ).fetchone()
    if row is not None:
        version_id = row["agenda_item_version_id"]
        conn.execute(
            """
            UPDATE retrieval_agenda_item_versions
            SET last_seen_run_id = ?, last_seen_at = ?
            WHERE agenda_item_version_id = ?
            """,
            (run_id, seen_at_text, version_id),
        )
        return version_id

    cursor = conn.execute(
        """
        INSERT INTO retrieval_agenda_item_versions (
            item_key,
            meeting_id,
            item_number,
            title,
            description,
            decision_text,
            minutes_text,
            decision_url,
            content_hash,
            first_seen_run_id,
            last_seen_run_id,
            created_at,
            last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            item_key,
            item.meeting_id,
            item.item_number,
            item.title,
            item.description,
            item.decision_text,
            item.minutes_text,
            item.decision_url,
            content_hash,
            run_id,
            run_id,
            seen_at_text,
            seen_at_text,
        ),
    )
    return int(cursor.lastrowid)


def _upsert_decision_version(
    conn: sqlite3.Connection,
    decision: DecisionDetail,
    run_id: str,
    seen_at_text: str,
) -> int:
    decision_key = _decision_key(decision)
    payload = {
        "agenda_title": decision.agenda_title,
        "title": decision.title,
        "decision": decision.decision,
        "reasons": decision.reasons,
        "made_by": decision.made_by,
        "date": decision.date,
    }
    content_hash = _hash_payload(payload)
    row = conn.execute(
        """
        SELECT decision_version_id
        FROM retrieval_decision_versions
        WHERE decision_key = ? AND content_hash = ?
        """,
        (decision_key, content_hash),
    ).fetchone()
    if row is not None:
        version_id = row["decision_version_id"]
        conn.execute(
            """
            UPDATE retrieval_decision_versions
            SET last_seen_run_id = ?, last_seen_at = ?
            WHERE decision_version_id = ?
            """,
            (run_id, seen_at_text, version_id),
        )
        return version_id

    cursor = conn.execute(
        """
        INSERT INTO retrieval_decision_versions (
            decision_key,
            agenda_title,
            title,
            decision,
            reasons,
            made_by,
            date,
            content_hash,
            first_seen_run_id,
            last_seen_run_id,
            created_at,
            last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            decision_key,
            decision.agenda_title,
            decision.title,
            decision.decision,
            decision.reasons,
            decision.made_by,
            decision.date,
            content_hash,
            run_id,
            run_id,
            seen_at_text,
            seen_at_text,
        ),
    )
    return int(cursor.lastrowid)


def _decision_key(decision: DecisionDetail) -> str:
    payload = {
        "agenda_title": decision.agenda_title,
        "title": decision.title,
        "made_by": decision.made_by,
        "date": decision.date,
    }
    return json.dumps(payload, sort_keys=True)


def _item_key(item: AgendaItem) -> str:
    return f"{item.meeting_id}-{item.item_number}"


def _hash_payload(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _deserialize_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None
