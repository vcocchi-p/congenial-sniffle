"""Councillor-facing Streamlit dashboard — voter sentiment by meeting and agenda item."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

from src.voter.db import get_item_tallies_for_meeting, get_meetings_with_votes, init_db

load_dotenv()
init_db()

st.set_page_config(
    page_title="The Quorum — Councillor View",
    page_icon="🏛️",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("🏛️ The Quorum — Councillor Dashboard")
st.caption("Voter sentiment across council meetings and agenda items.")
st.markdown("---")

# ---------------------------------------------------------------------------
# Sidebar — demo toggle + meeting selector
# ---------------------------------------------------------------------------
with st.sidebar:
    use_demo = st.toggle("🎭 Demo data", value=False, help="Switch between live and demo data")
    if use_demo:
        st.caption("Showing 10k simulated voters — Westminster Council meeting March 2026.")
    else:
        st.caption("Showing live voter submissions.")
    st.markdown("---")

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
meetings = get_meetings_with_votes(use_demo=use_demo)

if not meetings:
    if use_demo:
        st.warning("Demo data not seeded yet. Run: `python -m src.voter.seed`")
    else:
        st.info("No votes have been submitted yet. Share the voter app and ask people to vote!")
    st.stop()

with st.sidebar:
    st.header("📅 Select Meeting")
    meeting_labels = {
        m["meeting_id"]: f"{m['meeting_id']} — {m['unique_voters']:,} voters"
        for m in meetings
    }
    selected_id = st.radio(
        "Meeting",
        options=[m["meeting_id"] for m in meetings],
        format_func=lambda x: meeting_labels[x],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.caption(f"{len(meetings)} meeting(s) with votes")

# ---------------------------------------------------------------------------
# Meeting-level headline metrics
# ---------------------------------------------------------------------------
selected = next(m for m in meetings if m["meeting_id"] == selected_id)
items = get_item_tallies_for_meeting(selected_id, use_demo=use_demo)

col1, col2, col3 = st.columns(3)
col1.metric("👥 Unique Voters", selected["unique_voters"])
col2.metric("🗳️ Total Votes Cast", selected["total_votes"])
col3.metric("📋 Agenda Items Voted On", selected["items_voted_on"])

st.markdown("---")

if not items:
    st.info("No items found for this meeting.")
    st.stop()

# ---------------------------------------------------------------------------
# Engagement overview — horizontal bar chart
# ---------------------------------------------------------------------------
st.subheader("Engagement by Agenda Item")

titles = [i["item_title"][:60] + ("…" if len(i["item_title"]) > 60 else "") for i in items]

fig_engagement = go.Figure()
fig_engagement.add_trace(go.Bar(
    name="For",
    y=titles,
    x=[i["for"] for i in items],
    orientation="h",
    marker_color="#2ecc71",
))
fig_engagement.add_trace(go.Bar(
    name="Against",
    y=titles,
    x=[i["against"] for i in items],
    orientation="h",
    marker_color="#e74c3c",
))
fig_engagement.add_trace(go.Bar(
    name="Abstain",
    y=titles,
    x=[i["abstain"] for i in items],
    orientation="h",
    marker_color="#95a5a6",
))
fig_engagement.update_layout(
    barmode="stack",
    height=max(300, len(items) * 50),
    margin=dict(l=20, r=20, t=20, b=20),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    xaxis_title="Votes",
    yaxis=dict(autorange="reversed"),
)
st.plotly_chart(fig_engagement, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Per-item detail cards
# ---------------------------------------------------------------------------
st.subheader("Item Breakdown")

for item in items:
    total = item["total"]
    pct_for = item["for"] / total * 100 if total else 0
    pct_against = item["against"] / total * 100 if total else 0
    pct_abstain = item["abstain"] / total * 100 if total else 0

    # Flag contentious items (within 10% of each other, for vs against)
    spread = abs(pct_for - pct_against)
    is_contentious = spread < 10 and total >= 5
    label = " 🔥 Contentious" if is_contentious else ""

    with st.expander(f"**{item['item_title'][:80]}**{label} — {total} votes", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Votes", total)
        c2.metric("👍 For", f"{item['for']} ({pct_for:.0f}%)")
        c3.metric("👎 Against", f"{item['against']} ({pct_against:.0f}%)")
        c4.metric("🤷 Abstain", f"{item['abstain']} ({pct_abstain:.0f}%)")

        # Donut chart
        fig_donut = go.Figure(data=[go.Pie(
            labels=["For", "Against", "Abstain"],
            values=[item["for"], item["against"], item["abstain"]],
            hole=0.55,
            marker_colors=["#2ecc71", "#e74c3c", "#95a5a6"],
            textinfo="label+percent",
        )])
        fig_donut.update_layout(
            margin=dict(l=20, r=20, t=20, b=20),
            height=280,
            showlegend=False,
        )
        st.plotly_chart(fig_donut, use_container_width=True)

        if is_contentious:
            st.warning(
                "Voter opinion is closely divided on this item. "
                "Consider further public consultation."
            )
