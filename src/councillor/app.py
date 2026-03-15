"""Councillor-facing Streamlit dashboard — voter sentiment by meeting and agenda item."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

from src.voter.db import (
    get_agenda_items,
    get_item_tallies_for_meeting,
    get_latest_run_id,
    get_meetings,
    get_meetings_with_votes,
    get_recent_votes,
    get_vote_tallies,
    init_db,
)

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
# Load meetings + items — two separate paths depending on demo toggle
# ---------------------------------------------------------------------------
if use_demo:
    # Demo mode: derive meetings from the seeded demo_votes table (WCC-2026-03)
    demo_meetings = get_meetings_with_votes(use_demo=True)
    if not demo_meetings:
        st.warning("Demo data not seeded yet. Run: `python -m src.voter.seed`")
        st.stop()

    with st.sidebar:
        st.header("📅 Select Meeting")
        meeting_labels = {
            m["meeting_id"]: f"{m['meeting_id']} — {m['unique_voters']:,} voters"
            for m in demo_meetings
        }
        selected_id = st.radio(
            "Meeting",
            options=[m["meeting_id"] for m in demo_meetings],
            format_func=lambda x: meeting_labels[x],
            label_visibility="collapsed",
        )
        st.markdown("---")
        st.caption(f"{len(demo_meetings)} meeting(s) with votes")

    selected_meta = next(m for m in demo_meetings if m["meeting_id"] == selected_id)
    items = get_item_tallies_for_meeting(selected_id, use_demo=True)

    col1, col2, col3 = st.columns(3)
    col1.metric("👥 Unique Voters", selected_meta["unique_voters"])
    col2.metric("🗳️ Total Votes Cast", selected_meta["total_votes"])
    col3.metric("📋 Items Voted On", selected_meta["items_voted_on"])

else:
    # Live mode: meetings from retrieval DB, vote tallies overlaid from votes table
    run_id = get_latest_run_id()
    if run_id is None:
        st.info("No pipeline run found. Run the retrieval pipeline first.")
        st.stop()

    all_meetings = get_meetings(run_id)
    if not all_meetings:
        st.info("No meetings found in the latest pipeline run.")
        st.stop()

    with st.sidebar:
        st.header("📅 Select Meeting")
        meeting_labels = {
            m["meeting_id"]: f"{m['committee_name']} — {m['date']}"
            for m in all_meetings
        }
        selected_id = st.radio(
            "Meeting",
            options=[m["meeting_id"] for m in all_meetings],
            format_func=lambda x: meeting_labels[x],
            label_visibility="collapsed",
        )
        st.markdown("---")
        st.caption(f"{len(all_meetings)} meeting(s) loaded")

    all_items = get_agenda_items(run_id)
    meeting_items = [i for i in all_items if i["meeting_id"] == selected_id]
    items = []
    for i in meeting_items:
        tallies = get_vote_tallies(i["item_key"], use_demo=False)
        items.append({
            "item_key": i["item_key"],
            "item_title": i["title"],
            "for": tallies["for"],
            "against": tallies["against"],
            "abstain": tallies["abstain"],
            "total": tallies["for"] + tallies["against"] + tallies["abstain"],
        })

    total_votes = sum(i["total"] for i in items)
    items_with_votes = sum(1 for i in items if i["total"] > 0)
    col1, col2, col3 = st.columns(3)
    col1.metric("🗳️ Total Votes Cast", total_votes)
    col2.metric("📋 Agenda Items", len(items))
    col3.metric("📊 Items With Votes", items_with_votes)

st.markdown("---")

if not items:
    st.info("No items found for this meeting.")
    st.stop()

# ---------------------------------------------------------------------------
# Main content + live feed side by side
# ---------------------------------------------------------------------------
main_col, feed_col = st.columns([3, 1])

with feed_col:
    st.subheader("🔴 Live Feed")

    _VOTE_ICON = {"for": "✅", "against": "❌", "abstain": "🤷"}
    _VOTE_COLOR = {"for": "#2ecc71", "against": "#e74c3c", "abstain": "#95a5a6"}

    @st.fragment(run_every=2)
    def live_vote_feed():
        recent = get_recent_votes(limit=30, use_demo=use_demo)
        if not recent:
            st.caption("No votes yet.")
            return

        rows_html = ""
        for v in recent:
            icon = _VOTE_ICON.get(v["vote"], "❓")
            color = _VOTE_COLOR.get(v["vote"], "#ccc")
            title = v["item_title"][:35] + ("…" if len(v["item_title"]) > 35 else "")
            username = v["username"][:12]
            rows_html += f"""
            <div style="
                display:flex; align-items:center; gap:8px;
                padding:6px 8px; margin-bottom:4px;
                border-left: 3px solid {color};
                background: rgba(255,255,255,0.03);
                border-radius: 4px;
                font-size: 0.78rem;
                animation: fadeIn 0.4s ease;
            ">
                <span style="font-size:1.1rem">{icon}</span>
                <div>
                    <div style="font-weight:600; color:#eee">{username}</div>
                    <div style="color:#aaa">{title}</div>
                </div>
            </div>
            """

        st.markdown(
            f"""
            <style>
            @keyframes fadeIn {{
                from {{ opacity:0; transform:translateY(-4px); }}
                to {{ opacity:1; transform:translateY(0); }}
            }}
            .feed-container {{ max-height: 600px; overflow-y: auto; }}
            </style>
            <div class="feed-container">{rows_html}</div>
            """,
            unsafe_allow_html=True,
        )

    live_vote_feed()

with main_col:
    # ---------------------------------------------------------------------------
    # Engagement overview — horizontal bar chart (only when votes exist)
    # ---------------------------------------------------------------------------
    st.subheader("Engagement by Agenda Item")

    titles = [i["item_title"][:60] + ("…" if len(i["item_title"]) > 60 else "") for i in items]

    def _pct_labels(counts: list[int], totals: list[int]) -> list[str]:
        return [f"{c / t * 100:.0f}%" if t else "" for c, t in zip(counts, totals)]

    totals = [i["total"] for i in items]
    any_votes = sum(totals) > 0

    if not any_votes:
        st.info(f"⏳ {len(items)} agenda items loaded — waiting for votes to come in.")

    if any_votes:
        fig_engagement = go.Figure()
        fig_engagement.add_trace(go.Bar(
            name="For",
            y=titles,
            x=[i["for"] for i in items],
            orientation="h",
            marker_color="#2ecc71",
            text=_pct_labels([i["for"] for i in items], totals),
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(color="white", size=12),
        ))
        fig_engagement.add_trace(go.Bar(
            name="Against",
            y=titles,
            x=[i["against"] for i in items],
            orientation="h",
            marker_color="#e74c3c",
            text=_pct_labels([i["against"] for i in items], totals),
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(color="white", size=12),
        ))
        fig_engagement.add_trace(go.Bar(
            name="Abstain",
            y=titles,
            x=[i["abstain"] for i in items],
            orientation="h",
            marker_color="#95a5a6",
            text=_pct_labels([i["abstain"] for i in items], totals),
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(color="white", size=12),
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
