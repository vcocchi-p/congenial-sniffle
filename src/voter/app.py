"""Voter-facing Streamlit app — sign up, browse decisions, vote."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import asyncio
import io

import qrcode
import streamlit as st
from dotenv import load_dotenv

from src.agents.pipeline import run_pipeline  # noqa: E402
from src.models.documents import AgendaItem, Meeting  # noqa: E402
from src.parser.summariser import generate_pros_cons  # noqa: E402
from src.voter.db import get_vote_tallies, init_db, register_user, submit_votes, user_exists

load_dotenv()
init_db()

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(page_title="The Quorum — Have Your Say", page_icon="🗳️", layout="wide")

# ---------------------------------------------------------------------------
# Session state defaults
# ---------------------------------------------------------------------------
if "voter_username" not in st.session_state:
    st.session_state.voter_username = None
if "votes" not in st.session_state:
    st.session_state.votes = {}  # {item_key: {"vote": "for"|"against"|"abstain", "user": str}}
if "pipeline_data" not in st.session_state:
    st.session_state.pipeline_data = None
if "pros_cons_cache" not in st.session_state:
    st.session_state.pros_cons_cache = {}  # {item_key: pros_cons_dict}
if "fetching" not in st.session_state:
    st.session_state.fetching = False
if "submitted_votes" not in st.session_state:
    st.session_state.submitted_votes = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _item_key(item: AgendaItem) -> str:
    return f"{item.meeting_id}-{item.item_number}"


def _generate_qr(url: str) -> bytes:
    """Generate a QR code PNG for the given URL."""
    img = qrcode.make(url)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _fetch_pipeline_data():
    """Run the retrieval pipeline and cache results."""
    with st.spinner("Fetching latest council data..."):
        loop = asyncio.new_event_loop()
        try:
            data = loop.run_until_complete(run_pipeline(max_meetings_per_committee=3))
        finally:
            loop.close()
    st.session_state.pipeline_data = data
    st.session_state.fetching = False


def _get_pros_cons(item: AgendaItem, is_upcoming: bool) -> dict:
    """Get pros/cons for an item, using cache if available."""
    key = _item_key(item)
    if key not in st.session_state.pros_cons_cache:
        with st.spinner(f"Analysing: {item.title}..."):
            result = generate_pros_cons(item, is_upcoming=is_upcoming)
            st.session_state.pros_cons_cache[key] = result
    return st.session_state.pros_cons_cache[key]


# ---------------------------------------------------------------------------
# Signup page
# ---------------------------------------------------------------------------
def show_signup():
    st.markdown(
        """
        <style>
        .signup-container {
            max-width: 500px;
            margin: 0 auto;
            padding: 2rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.title("🗳️ The Quorum")
        st.subheader("Have Your Say on Westminster Council Decisions")
        st.markdown("---")

        st.markdown(
            "**Welcome!** Sign up to browse upcoming council decisions, "
            "see the pros and cons, and cast your vote."
        )

        username = st.text_input(
            "Choose a username",
            placeholder="e.g. westminster_voter_42",
            max_chars=30,
        )

        if st.button("Sign Up & Enter", type="primary", use_container_width=True):
            name = username.strip()
            if not name:
                st.error("Please enter a username.")
            elif user_exists(name):
                # Returning user — just log them in
                st.session_state.voter_username = name
                st.rerun()
            else:
                register_user(name)
                st.session_state.voter_username = name
                st.rerun()

        st.markdown("---")
        st.caption("Share this page with others:")

        # QR code for sharing
        qr_bytes = _generate_qr("http://localhost:8502")
        st.image(qr_bytes, width=200, caption="Scan to join")


# ---------------------------------------------------------------------------
# Main content page
# ---------------------------------------------------------------------------
def show_content():
    # Header
    col_title, col_user = st.columns([4, 1])
    with col_title:
        st.title("🗳️ The Quorum")
    with col_user:
        st.markdown(f"**{st.session_state.voter_username}**")
        if st.button("Sign out", key="signout"):
            st.session_state.voter_username = None
            st.rerun()

    # Submit all votes button
    if st.session_state.votes:
        col_submit, col_count = st.columns([3, 1])
        with col_submit:
            if st.button(
                "📨 Submit All Votes", type="primary", use_container_width=True, key="submit_top"
            ):
                count = submit_votes(st.session_state.voter_username, st.session_state.votes)
                st.session_state.submitted_votes = dict(st.session_state.votes)
                st.session_state.votes = {}
                st.success(
                    f"✅ {count} vote(s) submitted and saved. "
                    "Thank you for having your say!"
                )
                st.balloons()
        with col_count:
            st.metric("Votes ready", len(st.session_state.votes))

    st.markdown("---")

    # Fetch data if not cached
    if st.session_state.pipeline_data is None:
        st.info("We need to fetch the latest council data. This may take a minute.")
        if st.button("🔄 Load Council Decisions", type="primary"):
            _fetch_pipeline_data()
            st.rerun()
        return

    data = st.session_state.pipeline_data
    meetings: list[Meeting] = data.get("meetings", [])
    agenda_items: list[AgendaItem] = data.get("agenda_items", [])

    if not agenda_items:
        st.warning("No agenda items found. Try refreshing the data.")
        if st.button("🔄 Refresh"):
            st.session_state.pipeline_data = None
            st.rerun()
        return

    # Build a meeting lookup
    meeting_lookup: dict[int, Meeting] = {m.meeting_id: m for m in meetings}

    # Separate upcoming and past items
    upcoming_items = []
    past_items = []
    for item in agenda_items:
        meeting = meeting_lookup.get(item.meeting_id)
        if meeting and meeting.is_upcoming:
            upcoming_items.append((item, meeting))
        else:
            past_items.append((item, meeting))

    # Sidebar with vote tally
    with st.sidebar:
        st.header("📊 Your Votes")
        vote_count = len(st.session_state.votes)
        st.metric("Votes Cast", vote_count)
        st.markdown("---")

        if st.session_state.votes:
            for key, vote_data in st.session_state.votes.items():
                vote_label = {"for": "👍 For", "against": "👎 Against", "abstain": "🤷 Abstain"}
                title = vote_data.get("title", key)[:40]
                st.markdown(f"- **{title}**: {vote_label[vote_data['vote']]}")

        st.markdown("---")
        if st.button("🔄 Refresh Data"):
            st.session_state.pipeline_data = None
            st.session_state.pros_cons_cache = {}
            st.rerun()

    # Tabs for upcoming vs past
    tab_upcoming, tab_past = st.tabs(
        [f"📋 Upcoming ({len(upcoming_items)})", f"📁 Past Decisions ({len(past_items)})"]
    )

    with tab_upcoming:
        if not upcoming_items:
            st.info("No upcoming decisions found.")
        else:
            _render_items(upcoming_items, is_upcoming=True)

    with tab_past:
        if not past_items:
            st.info("No past decisions found.")
        else:
            _render_items(past_items, is_upcoming=False)


def _render_items(items: list[tuple[AgendaItem, Meeting | None]], is_upcoming: bool):
    """Render agenda items with pros/cons and vote buttons."""
    for item, meeting in items:
        key = _item_key(item)
        committee_name = meeting.committee_name if meeting else "Unknown Committee"
        meeting_date = meeting.date if meeting else ""

        with st.expander(f"**{item.title}** — {committee_name} ({meeting_date})", expanded=False):
            # Show basic info
            if item.description:
                st.markdown(f"*{item.description}*")

            # Pros & cons (generated on demand)
            if st.button("📊 Show Analysis", key=f"analyse-{key}"):
                st.session_state[f"show_analysis_{key}"] = True

            if st.session_state.get(f"show_analysis_{key}", False):
                analysis = _get_pros_cons(item, is_upcoming)

                st.markdown(f"**Summary:** {analysis['summary']}")

                col_pro, col_con = st.columns(2)
                with col_pro:
                    st.markdown("#### ✅ Arguments For")
                    for pro in analysis["pros"]:
                        st.markdown(f"- {pro}")
                with col_con:
                    st.markdown("#### ❌ Arguments Against")
                    for con in analysis["cons"]:
                        st.markdown(f"- {con}")

                if analysis.get("councillors"):
                    st.markdown(f"**Councillors mentioned:** {', '.join(analysis['councillors'])}")

            st.markdown("---")

            # Show current tallies from DB
            tallies = get_vote_tallies(key)
            total = tallies["for"] + tallies["against"] + tallies["abstain"]
            if total > 0:
                t_for, t_against, t_abstain = st.columns(3)
                with t_for:
                    st.metric("👍 For", tallies["for"])
                with t_against:
                    st.metric("👎 Against", tallies["against"])
                with t_abstain:
                    st.metric("🤷 Abstain", tallies["abstain"])

            # Vote buttons
            existing_vote = st.session_state.votes.get(key)

            if existing_vote:
                vote_label = {"for": "👍 For", "against": "👎 Against", "abstain": "🤷 Abstain"}
                st.success(f"You voted: **{vote_label[existing_vote['vote']]}**")
                if st.button("Change vote", key=f"change-{key}"):
                    del st.session_state.votes[key]
                    st.rerun()
            else:
                st.markdown("**Cast your vote:**")
                col_for, col_against, col_abstain = st.columns(3)

                with col_for:
                    if st.button("👍 Vote For", key=f"for-{key}", use_container_width=True):
                        st.session_state.votes[key] = {
                            "vote": "for",
                            "user": st.session_state.voter_username,
                            "title": item.title,
                        }
                        st.rerun()
                with col_against:
                    if st.button("👎 Vote Against", key=f"against-{key}", use_container_width=True):
                        st.session_state.votes[key] = {
                            "vote": "against",
                            "user": st.session_state.voter_username,
                            "title": item.title,
                        }
                        st.rerun()
                with col_abstain:
                    if st.button("🤷 Abstain", key=f"abstain-{key}", use_container_width=True):
                        st.session_state.votes[key] = {
                            "vote": "abstain",
                            "user": st.session_state.voter_username,
                            "title": item.title,
                        }
                        st.rerun()


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------
if st.session_state.voter_username is None:
    show_signup()
else:
    show_content()
