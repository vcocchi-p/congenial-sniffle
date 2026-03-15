"""Seed demo data: one Westminster meeting with 10 motions and ~10k fake voters.

Run once:
    python -m src.voter.seed
"""

from __future__ import annotations

import random
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.voter.db import DB_PATH, init_db

MEETING_ID = "WCC-2026-03"

# 10 realistic Westminster agenda items.
# Each has: title, and weights [for, against, abstain] that drive vote distribution.
# Also participation: fraction of 10k voters who bother to vote on this item.
AGENDA_ITEMS = [
    {
        "number": "1",
        "title": "Proposed cycle lane on Baker Street (Marylebone to Paddington)",
        "weights": [0.55, 0.35, 0.10],
        "participation": 0.82,
    },
    {
        "number": "2",
        "title": "Affordable housing development on Former Paddington Gas Works site",
        "weights": [0.70, 0.18, 0.12],
        "participation": 0.91,
    },
    {
        "number": "3",
        "title": "Temporary pedestrianisation of Oxford Street (pilot, 12 months)",
        "weights": [0.48, 0.44, 0.08],  # contentious
        "participation": 0.95,
    },
    {
        "number": "4",
        "title": "Extended late-night licensing hours for hospitality venues in Soho",
        "weights": [0.38, 0.50, 0.12],
        "participation": 0.74,
    },
    {
        "number": "5",
        "title": "Expansion of ULEZ enforcement to all Westminster borough roads",
        "weights": [0.46, 0.46, 0.08],  # contentious
        "participation": 0.88,
    },
    {
        "number": "6",
        "title": "Redevelopment of Paddington Recreation Ground (new sports facilities)",
        "weights": [0.75, 0.12, 0.13],
        "participation": 0.63,
    },
    {
        "number": "7",
        "title": "Installation of CCTV cameras in Church Street residential area",
        "weights": [0.52, 0.36, 0.12],
        "participation": 0.57,
    },
    {
        "number": "8",
        "title": "New primary school places: expansion of St Peter's Eaton Square",
        "weights": [0.83, 0.08, 0.09],
        "participation": 0.69,
    },
    {
        "number": "9",
        "title": "Planning permission: mixed-use tower on Victoria Street (32 storeys)",
        "weights": [0.30, 0.58, 0.12],
        "participation": 0.78,
    },
    {
        "number": "10",
        "title": "Revised parking charges across Westminster (increase of 15%)",
        "weights": [0.22, 0.65, 0.13],
        "participation": 0.85,
    },
]

NUM_VOTERS = 10_000
VOTES = ["for", "against", "abstain"]
SEED = 42


def seed():
    init_db()
    rng = random.Random(SEED)

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")  # faster bulk insert

    # Clear existing demo data
    conn.execute("DELETE FROM demo_votes")
    conn.execute("DELETE FROM demo_users")
    conn.commit()

    print(f"Seeding {NUM_VOTERS:,} demo voters across {len(AGENDA_ITEMS)} agenda items...")

    # Insert all users in one batch
    base_time = datetime(2026, 3, 10, 9, 0, 0, tzinfo=timezone.utc)
    users = [
        (f"voter_{i:05d}", (base_time + timedelta(seconds=i * 3)).isoformat())
        for i in range(1, NUM_VOTERS + 1)
    ]
    conn.executemany("INSERT INTO demo_users (username, created_at) VALUES (?, ?)", users)
    conn.commit()
    print(f"  ✓ {NUM_VOTERS:,} users inserted")

    # Build vote rows
    vote_rows = []
    for item in AGENDA_ITEMS:
        item_key = f"{MEETING_ID}-{item['number']}"
        title = item["title"]
        participating = [
            f"voter_{i:05d}"
            for i in range(1, NUM_VOTERS + 1)
            if rng.random() < item["participation"]
        ]
        for username in participating:
            vote = rng.choices(VOTES, weights=item["weights"])[0]
            submitted_at = (base_time + timedelta(seconds=rng.randint(0, 86400))).isoformat()
            vote_rows.append((username, item_key, title, vote, submitted_at))

        n = len(participating)
        print(f"  ✓ Item {item['number']:>2}: {n:,} votes — {title[:55]}…")

    conn.executemany(
        "INSERT INTO demo_votes (username, item_key, item_title, vote, submitted_at) "
        "VALUES (?, ?, ?, ?, ?)",
        vote_rows,
    )
    conn.commit()
    conn.close()
    print(f"\nDone. {len(vote_rows):,} total votes seeded into demo_votes.")


if __name__ == "__main__":
    seed()
