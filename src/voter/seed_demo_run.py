"""Seed a demo pipeline run for the Feb 23 Cabinet meeting.

Pretends it's Feb 15 2026 — the meeting is upcoming and no decisions
have been made yet. Only substantive agenda items are included (items 4-12).

Run once:
    python -m src.voter.seed_demo_run
"""

from __future__ import annotations

import hashlib
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.voter.db import DB_PATH

RUN_ID = "run-demo-feb23"
MEETING_ID = 6718
# Substantive item version IDs (items 4-12, skipping Welcome/Declarations/Minutes)
SUBSTANTIVE_ITEM_VERSION_IDS = [57, 58, 59, 60, 61, 62, 63, 64, 65]
# Simulated date: pretend it's Feb 15 2026
NOW = "2026-02-15T10:00:00+00:00"


def _hash(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def seed():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    # Remove existing demo run if re-running
    conn.execute("DELETE FROM retrieval_runs WHERE run_id = ?", (RUN_ID,))
    conn.commit()

    # 1. Create the run
    conn.execute(
        """
        INSERT INTO retrieval_runs
            (run_id, source_url, trigger_type, status, requested_at, started_at,
             completed_at, latest_message, documents_discovered, documents_fetched)
        VALUES (?, ?, 'manual', 'completed', ?, ?, ?, ?, 0, 0)
        """,
        (
            RUN_ID,
            "https://committees.westminster.gov.uk/ieListDocuments.aspx?CId=130&MId=6718",
            NOW,
            NOW,
            NOW,
            "Demo run: Cabinet 23 Feb 2026 (pre-decision view)",
        ),
    )
    conn.commit()
    print(f"✓ Created run {RUN_ID}")

    # 2. Insert a new meeting version with is_upcoming=1
    meeting_hash = _hash(str(MEETING_ID), "upcoming", RUN_ID)
    try:
        conn.execute(
            """
            INSERT INTO retrieval_meeting_versions
                (meeting_id, committee_id, committee_name, date, url, is_upcoming,
                 content_hash, first_seen_run_id, last_seen_run_id, created_at, last_seen_at)
            VALUES (?, 130, 'Cabinet', '23 Feb 2026 6.30 pm',
                    'https://committees.westminster.gov.uk/ieListDocuments.aspx?CId=130&MId=6718',
                    1, ?, ?, ?, ?, ?)
            """,
            (MEETING_ID, meeting_hash, RUN_ID, RUN_ID, NOW, NOW),
        )
        conn.commit()
        meeting_version_id = conn.execute(
            "SELECT meeting_version_id FROM retrieval_meeting_versions WHERE content_hash = ?",
            (meeting_hash,),
        ).fetchone()["meeting_version_id"]
    except sqlite3.IntegrityError:
        # Already exists from a previous seed — fetch it
        meeting_version_id = conn.execute(
            "SELECT meeting_version_id FROM retrieval_meeting_versions WHERE content_hash = ?",
            (meeting_hash,),
        ).fetchone()["meeting_version_id"]

    # 3. Link meeting version to this run
    conn.execute(
        "INSERT OR IGNORE INTO retrieval_run_meetings (run_id, meeting_version_id) VALUES (?, ?)",
        (RUN_ID, meeting_version_id),
    )
    conn.commit()
    print(f"✓ Linked meeting {MEETING_ID} (is_upcoming=1) to run")

    # 4. Link substantive agenda items to this run
    for version_id in SUBSTANTIVE_ITEM_VERSION_IDS:
        row = conn.execute(
            "SELECT item_number, title FROM retrieval_agenda_item_versions "
            "WHERE agenda_item_version_id = ?",
            (version_id,),
        ).fetchone()
        conn.execute(
            "INSERT OR IGNORE INTO retrieval_run_agenda_items "
            "(run_id, agenda_item_version_id) VALUES (?, ?)",
            (RUN_ID, version_id),
        )
        print(f"  ✓ Item {row['item_number']}: {row['title'][:60]}")

    conn.commit()
    conn.close()
    print(f"\nDone. Demo run '{RUN_ID}' ready — voter app will show 9 upcoming items.")


if __name__ == "__main__":
    seed()
