"""SQLite persistence for voter signups and votes."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "quorum.db"


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create tables if they don't exist."""
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
        """
    )
    conn.close()


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


def get_vote_tallies(item_key: str) -> dict[str, int]:
    """Get vote counts for an item. Returns {"for": N, "against": N, "abstain": N}."""
    conn = _connect()
    rows = conn.execute(
        "SELECT vote, COUNT(*) as cnt FROM votes WHERE item_key = ? GROUP BY vote",
        (item_key,),
    ).fetchall()
    conn.close()
    tallies = {"for": 0, "against": 0, "abstain": 0}
    for row in rows:
        tallies[row["vote"]] = row["cnt"]
    return tallies


def get_all_tallies() -> dict[str, dict]:
    """Get vote tallies for all items. Returns {item_key: {title, for, against, abstain}}."""
    conn = _connect()
    rows = conn.execute(
        """
        SELECT item_key, item_title, vote, COUNT(*) as cnt
        FROM votes GROUP BY item_key, vote
        """
    ).fetchall()
    conn.close()
    result: dict[str, dict] = {}
    for row in rows:
        key = row["item_key"]
        if key not in result:
            result[key] = {"title": row["item_title"], "for": 0, "against": 0, "abstain": 0}
        result[key][row["vote"]] = row["cnt"]
    return result


def get_user_votes(username: str) -> dict[str, str]:
    """Get all votes by a user. Returns {item_key: vote}."""
    conn = _connect()
    rows = conn.execute(
        "SELECT item_key, vote FROM votes WHERE username = ?", (username,)
    ).fetchall()
    conn.close()
    return {row["item_key"]: row["vote"] for row in rows}
