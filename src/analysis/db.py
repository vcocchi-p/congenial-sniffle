"""SQLite persistence for meeting selection and voter-facing analysis outputs."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path

from src.models.analysis import (
    AgendaItemAnalysis,
    AnalysisRun,
    MeetingAnalysisResult,
    MeetingSelection,
)

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "quorum.db"


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_analysis_db() -> None:
    """Create analysis tables if they do not exist."""
    conn = _connect()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS analysis_meeting_selections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            retrieval_run_id TEXT NOT NULL,
            meeting_id INTEGER NOT NULL,
            committee_name TEXT NOT NULL,
            meeting_date TEXT NOT NULL,
            analysis_mode TEXT NOT NULL,
            priority_score REAL NOT NULL,
            reason_selected TEXT NOT NULL,
            item_keys_json TEXT NOT NULL,
            selected_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_analysis_meeting_selections_meeting
        ON analysis_meeting_selections(meeting_id, id DESC);

        CREATE TABLE IF NOT EXISTS analysis_runs (
            analysis_run_id TEXT PRIMARY KEY,
            retrieval_run_id TEXT NOT NULL,
            meeting_id INTEGER NOT NULL,
            analysis_mode TEXT NOT NULL,
            status TEXT NOT NULL,
            model TEXT NOT NULL,
            selected_reason TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            error_message TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_analysis_runs_meeting
        ON analysis_runs(meeting_id, started_at DESC);

        CREATE TABLE IF NOT EXISTS analysis_item_versions (
            analysis_item_version_id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_key TEXT NOT NULL,
            meeting_id INTEGER NOT NULL,
            item_number TEXT NOT NULL,
            title TEXT NOT NULL,
            plain_summary TEXT NOT NULL,
            why_it_matters TEXT NOT NULL,
            pros_json TEXT NOT NULL,
            cons_json TEXT NOT NULL,
            what_to_watch TEXT NOT NULL,
            councillors_json TEXT NOT NULL,
            source_urls_json TEXT NOT NULL,
            notify_voters INTEGER NOT NULL DEFAULT 1,
            analysis_mode TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            first_seen_analysis_run_id TEXT NOT NULL,
            last_seen_analysis_run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            UNIQUE(item_key, analysis_mode, content_hash)
        );

        CREATE TABLE IF NOT EXISTS analysis_run_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analysis_run_id TEXT NOT NULL,
            analysis_item_version_id INTEGER NOT NULL,
            UNIQUE(analysis_run_id, analysis_item_version_id),
            FOREIGN KEY (analysis_run_id) REFERENCES analysis_runs(analysis_run_id)
                ON DELETE CASCADE,
            FOREIGN KEY (analysis_item_version_id)
                REFERENCES analysis_item_versions(analysis_item_version_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_analysis_run_items_run
        ON analysis_run_items(analysis_run_id, id);
        """
    )
    conn.close()


def get_latest_analysis_run_sequence() -> int:
    """Return the highest numeric analysis run sequence seen so far."""
    init_analysis_db()
    conn = _connect()
    rows = conn.execute("SELECT analysis_run_id FROM analysis_runs").fetchall()
    conn.close()
    sequences = []
    for row in rows:
        match = re.fullmatch(r"analysis-run-(\d+)", row["analysis_run_id"])
        if match:
            sequences.append(int(match.group(1)))
    return max(sequences, default=0)


def record_meeting_selection(selection: MeetingSelection) -> None:
    """Persist the chosen meeting for analysis."""
    init_analysis_db()
    conn = _connect()
    conn.execute(
        """
        INSERT INTO analysis_meeting_selections (
            retrieval_run_id,
            meeting_id,
            committee_name,
            meeting_date,
            analysis_mode,
            priority_score,
            reason_selected,
            item_keys_json,
            selected_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            selection.retrieval_run_id,
            selection.meeting_id,
            selection.committee_name,
            selection.meeting_date,
            selection.analysis_mode,
            selection.priority_score,
            selection.reason_selected,
            json.dumps(selection.item_keys),
            _serialize_datetime(selection.selected_at),
        ),
    )
    conn.commit()
    conn.close()


def load_latest_meeting_selection() -> MeetingSelection | None:
    """Return the most recent meeting selection, if one exists."""
    init_analysis_db()
    conn = _connect()
    row = conn.execute(
        """
        SELECT retrieval_run_id,
               meeting_id,
               committee_name,
               meeting_date,
               analysis_mode,
               priority_score,
               reason_selected,
               item_keys_json,
               selected_at
        FROM analysis_meeting_selections
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()
    if row is None:
        return None
    return MeetingSelection(
        retrieval_run_id=row["retrieval_run_id"],
        meeting_id=row["meeting_id"],
        committee_name=row["committee_name"],
        meeting_date=row["meeting_date"],
        analysis_mode=row["analysis_mode"],
        priority_score=row["priority_score"],
        reason_selected=row["reason_selected"],
        selected_at=_deserialize_datetime(row["selected_at"]),
        item_keys=json.loads(row["item_keys_json"]),
    )


def record_analysis_run_started(run: AnalysisRun) -> None:
    """Create or update an analysis run row when execution begins."""
    init_analysis_db()
    conn = _connect()
    conn.execute(
        """
        INSERT INTO analysis_runs (
            analysis_run_id,
            retrieval_run_id,
            meeting_id,
            analysis_mode,
            status,
            model,
            selected_reason,
            started_at,
            completed_at,
            error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(analysis_run_id) DO UPDATE SET
            retrieval_run_id = excluded.retrieval_run_id,
            meeting_id = excluded.meeting_id,
            analysis_mode = excluded.analysis_mode,
            status = excluded.status,
            model = excluded.model,
            selected_reason = excluded.selected_reason,
            started_at = excluded.started_at,
            completed_at = excluded.completed_at,
            error_message = excluded.error_message
        """,
        (
            run.analysis_run_id,
            run.retrieval_run_id,
            run.meeting_id,
            run.analysis_mode,
            run.status,
            run.model,
            run.selected_reason,
            _serialize_datetime(run.started_at),
            _serialize_datetime(run.completed_at) if run.completed_at else None,
            run.error_message,
        ),
    )
    conn.commit()
    conn.close()


def record_analysis_run_result(
    analysis_run_id: str,
    *,
    status: str,
    completed_at: datetime,
    items: list[AgendaItemAnalysis] | None = None,
    error_message: str | None = None,
) -> None:
    """Persist the final state of an analysis run and its item outputs."""
    init_analysis_db()
    conn = _connect()
    conn.execute(
        """
        UPDATE analysis_runs
        SET status = ?, completed_at = ?, error_message = ?
        WHERE analysis_run_id = ?
        """,
        (status, _serialize_datetime(completed_at), error_message, analysis_run_id),
    )

    for item in items or []:
        version_id = _upsert_analysis_item_version(conn, item)
        conn.execute(
            """
            INSERT INTO analysis_run_items (analysis_run_id, analysis_item_version_id)
            VALUES (?, ?)
            ON CONFLICT(analysis_run_id, analysis_item_version_id) DO NOTHING
            """,
            (analysis_run_id, version_id),
        )

    conn.commit()
    conn.close()


def load_meeting_analyses(
    meeting_id: int,
    *,
    analysis_mode: str | None = None,
    analysis_run_id: str | None = None,
) -> list[AgendaItemAnalysis]:
    """Load persisted analyses for a meeting from the latest matching run."""
    init_analysis_db()
    conn = _connect()
    resolved_run_id = analysis_run_id or _resolve_latest_analysis_run_id(
        conn, meeting_id=meeting_id, analysis_mode=analysis_mode
    )
    if resolved_run_id is None:
        conn.close()
        return []

    rows = conn.execute(
        """
        SELECT r.analysis_run_id,
               r.retrieval_run_id,
               v.meeting_id,
               v.item_key,
               v.item_number,
               v.title,
               v.plain_summary,
               v.why_it_matters,
               v.pros_json,
               v.cons_json,
               v.what_to_watch,
               v.councillors_json,
               v.source_urls_json,
               v.notify_voters,
               v.analysis_mode,
               v.last_seen_at
        FROM analysis_run_items ri
        JOIN analysis_item_versions v
            ON ri.analysis_item_version_id = v.analysis_item_version_id
        JOIN analysis_runs r
            ON ri.analysis_run_id = r.analysis_run_id
        WHERE ri.analysis_run_id = ?
        ORDER BY CAST(v.item_number AS INTEGER), v.item_number, ri.id
        """,
        (resolved_run_id,),
    ).fetchall()
    conn.close()
    return [_row_to_analysis(row) for row in rows]


def load_item_analysis(
    item_key: str,
    *,
    analysis_mode: str | None = None,
) -> AgendaItemAnalysis | None:
    """Return the latest persisted analysis for a single agenda item."""
    init_analysis_db()
    conn = _connect()
    row = conn.execute(
        """
        SELECT r.analysis_run_id,
               r.retrieval_run_id,
               v.meeting_id,
               v.item_key,
               v.item_number,
               v.title,
               v.plain_summary,
               v.why_it_matters,
               v.pros_json,
               v.cons_json,
               v.what_to_watch,
               v.councillors_json,
               v.source_urls_json,
               v.notify_voters,
               v.analysis_mode,
               v.last_seen_at
        FROM analysis_run_items ri
        JOIN analysis_item_versions v
            ON ri.analysis_item_version_id = v.analysis_item_version_id
        JOIN analysis_runs r
            ON ri.analysis_run_id = r.analysis_run_id
        WHERE v.item_key = ?
          AND (? IS NULL OR v.analysis_mode = ?)
          AND r.status = 'completed'
        ORDER BY COALESCE(r.completed_at, r.started_at) DESC, ri.id DESC
        LIMIT 1
        """,
        (item_key, analysis_mode, analysis_mode),
    ).fetchone()
    conn.close()
    return _row_to_analysis(row) if row is not None else None


def load_analysis_result_for_retrieval_run(
    retrieval_run_id: str,
    *,
    analysis_mode: str | None = None,
) -> MeetingAnalysisResult | None:
    """Load the latest persisted analysis result for a retrieval run."""
    init_analysis_db()
    conn = _connect()
    row = conn.execute(
        """
        SELECT analysis_run_id
        FROM analysis_runs
        WHERE retrieval_run_id = ?
          AND (? IS NULL OR analysis_mode = ?)
        ORDER BY COALESCE(completed_at, started_at) DESC
        LIMIT 1
        """,
        (retrieval_run_id, analysis_mode, analysis_mode),
    ).fetchone()
    if row is None:
        conn.close()
        return None

    run = _load_analysis_run(conn, row["analysis_run_id"])
    if run is None:
        conn.close()
        return None

    selection_row = conn.execute(
        """
        SELECT retrieval_run_id,
               meeting_id,
               committee_name,
               meeting_date,
               analysis_mode,
               priority_score,
               reason_selected,
               item_keys_json,
               selected_at
        FROM analysis_meeting_selections
        WHERE retrieval_run_id = ? AND meeting_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (retrieval_run_id, run.meeting_id),
    ).fetchone()
    conn.close()
    if selection_row is None:
        return None

    selection = MeetingSelection(
        retrieval_run_id=selection_row["retrieval_run_id"],
        meeting_id=selection_row["meeting_id"],
        committee_name=selection_row["committee_name"],
        meeting_date=selection_row["meeting_date"],
        analysis_mode=selection_row["analysis_mode"],
        priority_score=selection_row["priority_score"],
        reason_selected=selection_row["reason_selected"],
        selected_at=_deserialize_datetime(selection_row["selected_at"]),
        item_keys=json.loads(selection_row["item_keys_json"]),
    )
    items = load_meeting_analyses(
        run.meeting_id,
        analysis_mode=run.analysis_mode,
        analysis_run_id=run.analysis_run_id,
    )
    return MeetingAnalysisResult(selection=selection, run=run, items=items)


def _resolve_latest_analysis_run_id(
    conn: sqlite3.Connection,
    *,
    meeting_id: int,
    analysis_mode: str | None,
) -> str | None:
    row = conn.execute(
        """
        SELECT analysis_run_id
        FROM analysis_runs
        WHERE meeting_id = ?
          AND status = 'completed'
          AND (? IS NULL OR analysis_mode = ?)
        ORDER BY COALESCE(completed_at, started_at) DESC
        LIMIT 1
        """,
        (meeting_id, analysis_mode, analysis_mode),
    ).fetchone()
    return row["analysis_run_id"] if row is not None else None


def _load_analysis_run(conn: sqlite3.Connection, analysis_run_id: str) -> AnalysisRun | None:
    row = conn.execute(
        """
        SELECT analysis_run_id,
               retrieval_run_id,
               meeting_id,
               analysis_mode,
               status,
               model,
               selected_reason,
               started_at,
               completed_at,
               error_message
        FROM analysis_runs
        WHERE analysis_run_id = ?
        LIMIT 1
        """,
        (analysis_run_id,),
    ).fetchone()
    if row is None:
        return None
    return AnalysisRun(
        analysis_run_id=row["analysis_run_id"],
        retrieval_run_id=row["retrieval_run_id"],
        meeting_id=row["meeting_id"],
        analysis_mode=row["analysis_mode"],
        status=row["status"],
        model=row["model"],
        selected_reason=row["selected_reason"],
        started_at=_deserialize_datetime(row["started_at"]),
        completed_at=(
            _deserialize_datetime(row["completed_at"]) if row["completed_at"] else None
        ),
        error_message=row["error_message"],
    )


def _upsert_analysis_item_version(
    conn: sqlite3.Connection,
    item: AgendaItemAnalysis,
) -> int:
    content_hash = _analysis_content_hash(item)
    now = _serialize_datetime(item.created_at)
    row = conn.execute(
        """
        SELECT analysis_item_version_id
        FROM analysis_item_versions
        WHERE item_key = ? AND analysis_mode = ? AND content_hash = ?
        """,
        (item.item_key, item.analysis_mode, content_hash),
    ).fetchone()
    if row is not None:
        conn.execute(
            """
            UPDATE analysis_item_versions
            SET last_seen_analysis_run_id = ?, last_seen_at = ?
            WHERE analysis_item_version_id = ?
            """,
            (item.analysis_run_id, now, row["analysis_item_version_id"]),
        )
        return int(row["analysis_item_version_id"])

    cursor = conn.execute(
        """
        INSERT INTO analysis_item_versions (
            item_key,
            meeting_id,
            item_number,
            title,
            plain_summary,
            why_it_matters,
            pros_json,
            cons_json,
            what_to_watch,
            councillors_json,
            source_urls_json,
            notify_voters,
            analysis_mode,
            content_hash,
            first_seen_analysis_run_id,
            last_seen_analysis_run_id,
            created_at,
            last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            item.item_key,
            item.meeting_id,
            item.item_number,
            item.title,
            item.plain_summary,
            item.why_it_matters,
            json.dumps(item.pros),
            json.dumps(item.cons),
            item.what_to_watch,
            json.dumps(item.councillors_involved),
            json.dumps(item.source_urls),
            int(item.notify_voters),
            item.analysis_mode,
            content_hash,
            item.analysis_run_id,
            item.analysis_run_id,
            now,
            now,
        ),
    )
    return int(cursor.lastrowid)


def _analysis_content_hash(item: AgendaItemAnalysis) -> str:
    payload = {
        "item_key": item.item_key,
        "meeting_id": item.meeting_id,
        "item_number": item.item_number,
        "title": item.title,
        "plain_summary": item.plain_summary,
        "why_it_matters": item.why_it_matters,
        "pros": item.pros,
        "cons": item.cons,
        "what_to_watch": item.what_to_watch,
        "councillors_involved": item.councillors_involved,
        "source_urls": item.source_urls,
        "notify_voters": item.notify_voters,
        "analysis_mode": item.analysis_mode,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=True).encode("utf-8")
    ).hexdigest()


def _row_to_analysis(row: sqlite3.Row) -> AgendaItemAnalysis:
    return AgendaItemAnalysis(
        analysis_run_id=row["analysis_run_id"],
        retrieval_run_id=row["retrieval_run_id"],
        meeting_id=row["meeting_id"],
        item_key=row["item_key"],
        item_number=row["item_number"],
        title=row["title"],
        plain_summary=row["plain_summary"],
        why_it_matters=row["why_it_matters"],
        pros=json.loads(row["pros_json"]),
        cons=json.loads(row["cons_json"]),
        what_to_watch=row["what_to_watch"],
        councillors_involved=json.loads(row["councillors_json"]),
        source_urls=json.loads(row["source_urls_json"]),
        notify_voters=bool(row["notify_voters"]),
        analysis_mode=row["analysis_mode"],
        created_at=_deserialize_datetime(row["last_seen_at"]),
    )


def _serialize_datetime(value: datetime) -> str:
    return value.isoformat()


def _deserialize_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)
