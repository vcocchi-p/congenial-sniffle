"""Seed realistic fake votes for the Cabinet 23 Feb 2026 agenda items.

Populates populated_users / populated_votes tables — a parallel dataset
to demo_votes, but keyed to the real item_keys from the pipeline
(6718-4 through 6718-12).

Run once:
    python -m src.voter.seed_populated_votes
"""

from __future__ import annotations

import random
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.voter.db import DB_PATH, init_db

NUM_VOTERS = 500
SEED = 99
VOTES = ["for", "against", "abstain"]

# Item keys + titles + voting weights [for, against, abstain] + participation rate
AGENDA_ITEMS = [
    {
        "item_key": "6718-4",
        "title": "Business and Financial Planning 2026/27 to 2028/29",
        "weights": [0.52, 0.38, 0.10],
        "participation": 0.80,
    },
    {
        "item_key": "6718-5",
        "title": "Capital Strategy 2026-27 to 2030-31",
        "weights": [0.60, 0.28, 0.12],
        "participation": 0.55,
    },
    {
        "item_key": "6718-6",
        "title": "Integrated Investment Framework 2026/27",
        "weights": [0.58, 0.28, 0.14],
        "participation": 0.48,
    },
    {
        "item_key": "6718-7",
        "title": "Treasury Management Strategy Statement for 2026/27 to 2030/31",
        "weights": [0.60, 0.25, 0.15],
        "participation": 0.42,
    },
    {
        "item_key": "6718-8",
        "title": "Housing Revenue Account Business Plan",
        "weights": [0.38, 0.52, 0.10],  # contentious — 4.8% rent increase
        "participation": 0.88,
    },
    {
        "item_key": "6718-9",
        "title": "Westminster City Council Pay Policy 2026-2027",
        "weights": [0.55, 0.30, 0.15],
        "participation": 0.60,
    },
    {
        "item_key": "6718-10",
        "title": "Contract Award - Highways",
        "weights": [0.50, 0.38, 0.12],  # £1.25bn contract — divided opinion
        "participation": 0.72,
    },
    {
        "item_key": "6718-11",
        "title": "Sustainable Transport Strategy 2026 - 2036",
        "weights": [0.54, 0.36, 0.10],  # close split on cycling/roads
        "participation": 0.85,
    },
    {
        "item_key": "6718-12",
        "title": "Affordable Housing Pipeline - Strategic Outline Case for Multiple Sites",
        "weights": [0.74, 0.16, 0.10],  # popular
        "participation": 0.90,
    },
]


def seed():
    init_db()
    rng = random.Random(SEED)

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")

    conn.execute("DELETE FROM populated_votes")
    conn.execute("DELETE FROM populated_users")
    conn.commit()

    # Simulated voting window: Feb 15–22 2026 (week before the meeting)
    base_time = datetime(2026, 2, 15, 9, 0, 0, tzinfo=timezone.utc)

    users = [
        (f"cab_voter_{i:04d}", (base_time + timedelta(seconds=i * 60)).isoformat())
        for i in range(1, NUM_VOTERS + 1)
    ]
    conn.executemany(
        "INSERT INTO populated_users (username, created_at) VALUES (?, ?)", users
    )
    conn.commit()
    print(f"✓ {NUM_VOTERS:,} users inserted")

    vote_rows = []
    for item in AGENDA_ITEMS:
        participating = [
            f"cab_voter_{i:04d}"
            for i in range(1, NUM_VOTERS + 1)
            if rng.random() < item["participation"]
        ]
        for username in participating:
            vote = rng.choices(VOTES, weights=item["weights"])[0]
            submitted_at = (base_time + timedelta(seconds=rng.randint(0, 7 * 86400))).isoformat()
            vote_rows.append((username, item["item_key"], item["title"], vote, submitted_at))

        n = len(participating)
        print(f"  ✓ {item['item_key']}: {n:,} votes — {item['title'][:55]}…")

    conn.executemany(
        "INSERT INTO populated_votes (username, item_key, item_title, vote, submitted_at) "
        "VALUES (?, ?, ?, ?, ?)",
        vote_rows,
    )
    conn.commit()
    conn.close()
    print(f"\nDone. {len(vote_rows):,} total votes in populated_votes.")


if __name__ == "__main__":
    seed()
