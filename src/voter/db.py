"""SQLite persistence for voter signups and votes."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import src.retrieval.db as retrieval_db

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "quorum.db"


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _votes_table(demo: bool, populated: bool = False) -> str:
    if populated:
        return "populated_votes"
    return "demo_votes" if demo else "votes"


def _users_table(demo: bool, populated: bool = False) -> str:
    if populated:
        return "populated_users"
    return "demo_users" if demo else "users"


def init_db():
    """Create voter tables and ensure retrieval tables exist for empty DB startup."""
    conn = _connect()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            item_key TEXT NOT NULL,
            item_title TEXT NOT NULL,
            vote TEXT NOT NULL CHECK(vote IN ('for', 'against', 'abstain')),
            submitted_at TEXT NOT NULL,
            FOREIGN KEY (username) REFERENCES users(username),
            UNIQUE(username, item_key)
        );

        CREATE TABLE IF NOT EXISTS demo_users (
            username TEXT PRIMARY KEY,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS demo_votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            item_key TEXT NOT NULL,
            item_title TEXT NOT NULL,
            vote TEXT NOT NULL CHECK(vote IN ('for', 'against', 'abstain')),
            submitted_at TEXT NOT NULL,
            FOREIGN KEY (username) REFERENCES demo_users(username),
            UNIQUE(username, item_key)
        );

        CREATE TABLE IF NOT EXISTS populated_users (
            username TEXT PRIMARY KEY,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS populated_votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            item_key TEXT NOT NULL,
            item_title TEXT NOT NULL,
            vote TEXT NOT NULL CHECK(vote IN ('for', 'against', 'abstain')),
            submitted_at TEXT NOT NULL,
            FOREIGN KEY (username) REFERENCES populated_users(username),
            UNIQUE(username, item_key)
        );
        """
    )
    conn.close()
    retrieval_db.init_retrieval_db()


def register_user(username: str) -> str:
    """Register a new user, auto-disambiguating on conflict.

    Returns the final username that was registered (may differ from input
    if there was a conflict, e.g. "user" → "user2" → "user3" etc.).
    """
    conn = _connect()
    try:
        candidate = username
        suffix = 2
        while True:
            try:
                conn.execute(
                    "INSERT INTO users (username, created_at) VALUES (?, ?)",
                    (candidate, datetime.now(timezone.utc).isoformat()),
                )
                conn.commit()
                return candidate
            except sqlite3.IntegrityError:
                candidate = f"{username}{suffix}"
                suffix += 1
    finally:
        conn.close()


def user_exists(username: str) -> bool:
    conn = _connect()
    row = conn.execute("SELECT 1 FROM users WHERE username = ?", (username,)).fetchone()
    conn.close()
    return row is not None


def submit_votes(username: str, votes: dict[str, dict]) -> int:
    """Save votes to the database. Returns number of votes saved.

    votes: {item_key: {"vote": "for"|"against"|"abstain", "title": str}}
    """
    conn = _connect()
    count = 0
    now = datetime.now(timezone.utc).isoformat()
    for item_key, vote_data in votes.items():
        conn.execute(
            """
            INSERT INTO votes (username, item_key, item_title, vote, submitted_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(username, item_key)
            DO UPDATE SET vote = excluded.vote, submitted_at = excluded.submitted_at
            """,
            (username, item_key, vote_data.get("title", item_key), vote_data["vote"], now),
        )
        count += 1
    conn.commit()
    conn.close()
    return count


def get_vote_tallies(
    item_key: str, use_demo: bool = False, use_populated: bool = False
) -> dict[str, int]:
    """Get vote counts for an item. Returns {"for": N, "against": N, "abstain": N}."""
    table = _votes_table(use_demo, populated=use_populated)
    conn = _connect()
    rows = conn.execute(
        f"SELECT vote, COUNT(*) as cnt FROM {table} WHERE item_key = ? GROUP BY vote",
        (item_key,),
    ).fetchall()
    conn.close()
    tallies = {"for": 0, "against": 0, "abstain": 0}
    for row in rows:
        tallies[row["vote"]] = row["cnt"]
    return tallies


def get_meetings_with_votes(use_demo: bool = False) -> list[dict]:
    """Return all meeting IDs that have at least one vote, with engagement stats."""
    table = _votes_table(use_demo)
    conn = _connect()
    rows = conn.execute(
        f"""
        SELECT item_key, COUNT(DISTINCT username) as unique_voters, COUNT(*) as total_votes
        FROM {table}
        GROUP BY item_key
        """
    ).fetchall()

    # Group by meeting_id (everything before the last "-N" suffix)
    meetings: dict[str, dict] = {}
    for row in rows:
        parts = row["item_key"].rsplit("-", 1)
        meeting_id = parts[0] if len(parts) == 2 else row["item_key"]
        if meeting_id not in meetings:
            meetings[meeting_id] = {
                "meeting_id": meeting_id,
                "total_votes": 0,
                "items_voted_on": 0,
            }
        meetings[meeting_id]["total_votes"] += row["total_votes"]
        meetings[meeting_id]["items_voted_on"] += 1

    # Count unique voters per meeting across all its items
    result = []
    for meeting_id, data in meetings.items():
        voter_row = conn.execute(
            f"SELECT COUNT(DISTINCT username) as uv FROM {table} WHERE item_key LIKE ?",
            (f"{meeting_id}-%",),
        ).fetchone()
        result.append({
            "meeting_id": meeting_id,
            "unique_voters": voter_row["uv"] if voter_row else 0,
            "total_votes": data["total_votes"],
            "items_voted_on": data["items_voted_on"],
        })
    conn.close()
    return sorted(result, key=lambda x: x["total_votes"], reverse=True)


def get_item_tallies_for_meeting(meeting_id: str, use_demo: bool = False) -> list[dict]:
    """Return per-item vote tallies for a given meeting.

    Returns list of dicts with keys: item_key, item_title, for, against, abstain, total.
    Sorted by total votes descending.
    """
    table = _votes_table(use_demo)
    conn = _connect()
    rows = conn.execute(
        f"""
        SELECT item_key, item_title, vote, COUNT(*) as cnt
        FROM {table}
        WHERE item_key LIKE ?
        GROUP BY item_key, vote
        """,
        (f"{meeting_id}-%",),
    ).fetchall()
    conn.close()

    items: dict[str, dict] = {}
    for row in rows:
        key = row["item_key"]
        if key not in items:
            items[key] = {
                "item_key": key,
                "item_title": row["item_title"],
                "for": 0,
                "against": 0,
                "abstain": 0,
            }
        items[key][row["vote"]] = row["cnt"]

    for item in items.values():
        item["total"] = item["for"] + item["against"] + item["abstain"]

    return sorted(items.values(), key=lambda x: x["total"], reverse=True)


def get_user_votes(username: str) -> dict[str, str]:
    """Get all votes by a user. Returns {item_key: vote}."""
    conn = _connect()
    rows = conn.execute(
        "SELECT item_key, vote FROM votes WHERE username = ?", (username,)
    ).fetchall()
    conn.close()
    return {row["item_key"]: row["vote"] for row in rows}


def get_latest_run_id() -> str | None:
    """Return the run_id of the most recently completed pipeline run, or None."""
    conn = _connect()
    row = conn.execute(
        """
        SELECT run_id FROM retrieval_runs
        WHERE status = 'completed'
        ORDER BY completed_at DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()
    return row["run_id"] if row else None


def get_meetings(run_id: str) -> list[dict]:
    """Return all meetings for a retrieval run from the persisted SQLite store."""
    conn = _connect()
    rows = conn.execute(
        """
        SELECT mv.*
        FROM retrieval_meeting_versions mv
        JOIN retrieval_run_meetings rm ON rm.meeting_version_id = mv.meeting_version_id
        WHERE rm.run_id = ?
        ORDER BY rm.id ASC
        """,
        (run_id,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_agenda_items(run_id: str) -> list[dict]:
    """Return all agenda items for a retrieval run from the persisted SQLite store."""
    conn = _connect()
    rows = conn.execute(
        """
        SELECT av.*
        FROM retrieval_agenda_item_versions av
        JOIN retrieval_run_agenda_items ra ON ra.agenda_item_version_id = av.agenda_item_version_id
        WHERE ra.run_id = ?
        ORDER BY ra.id ASC
        """,
        (run_id,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_recent_votes(
    limit: int = 30, use_demo: bool = False, use_populated: bool = False
) -> list[dict]:
    """Return the most recent votes cast, newest first.

    Returns list of dicts with keys: username, item_key, item_title, vote, submitted_at.
    """
    table = _votes_table(use_demo, populated=use_populated)
    conn = _connect()
    rows = conn.execute(
        f"""
        SELECT username, item_key, item_title, vote, submitted_at
        FROM {table}
        ORDER BY submitted_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]
