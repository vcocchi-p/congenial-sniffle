"""Voter-facing Streamlit app — sign up, browse decisions, vote."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import io

import qrcode
import streamlit as st
from dotenv import load_dotenv

from src.analysis.db import load_item_analysis  # noqa: E402
from src.analysis.relevance import is_voter_relevant_agenda_item  # noqa: E402
from src.models.documents import AgendaItem, Meeting  # noqa: E402
from src.voter.db import (  # noqa: E402
    get_agenda_items,
    get_latest_run_id,
    get_meetings,
    get_vote_tallies,
    init_db,
    register_user,
    submit_votes,
    user_exists,
)
from src.voter.presentation import is_demo_mode_active, is_demo_upcoming  # noqa: E402

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
if "pros_cons_cache" not in st.session_state:
    st.session_state.pros_cons_cache = {}  # {item_key: pros_cons_dict}
if "submitted_votes" not in st.session_state:
    st.session_state.submitted_votes = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _item_key(item: AgendaItem) -> str:
    return f"{item.meeting_id}-{item.item_number}"


def _get_public_url() -> str:
    """Get the public URL from ngrok, falling back to localhost."""
    try:
        import urllib.request

        resp = urllib.request.urlopen("http://localhost:4040/api/tunnels", timeout=2)
        import json

        data = json.loads(resp.read())
        for tunnel in data.get("tunnels", []):
            if tunnel.get("proto") == "https":
                return tunnel["public_url"]
    except Exception:
        pass
    return "http://localhost:8502"


def _generate_qr(url: str) -> bytes:
    """Generate a QR code PNG for the given URL."""
    img = qrcode.make(url)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _load_council_data() -> tuple[list[Meeting], list[AgendaItem]] | None:
    """Load meetings and agenda items from the latest completed pipeline run."""
    run_id = get_latest_run_id()
    if run_id is None:
        return None

    meeting_rows = get_meetings(run_id)
    item_rows = get_agenda_items(run_id)

    meetings = [
        Meeting(
            committee_id=row["committee_id"],
            committee_name=row["committee_name"],
            meeting_id=row["meeting_id"],
            date=row["date"],
            url=row["url"],
            is_upcoming=bool(row["is_upcoming"]),
        )
        for row in meeting_rows
    ]
    agenda_items = [
        AgendaItem(
            meeting_id=row["meeting_id"],
            item_number=row["item_number"],
            title=row["title"],
            description=row["description"],
            decision_text=row["decision_text"],
            minutes_text=row["minutes_text"],
            decision_url=row.get("decision_url"),
        )
        for row in item_rows
    ]
    return meetings, agenda_items


def _get_persisted_analysis(item: AgendaItem, is_upcoming: bool) -> dict | None:
    """Return persisted analysis for an item, using cache if available."""
    key = _item_key(item)
    if key not in st.session_state.pros_cons_cache:
        analysis_mode = "demo_upcoming" if is_demo_mode_active() and is_upcoming else None
        persisted = load_item_analysis(key, analysis_mode=analysis_mode)
        if persisted is not None:
            st.session_state.pros_cons_cache[key] = {
                "item": item,
                "is_upcoming": is_upcoming,
                "summary": persisted.plain_summary,
                "pros": persisted.pros,
                "cons": persisted.cons,
                "councillors": persisted.councillors_involved,
                "status": "upcoming" if is_upcoming else "decided",
                "why_it_matters": persisted.why_it_matters,
                "what_to_watch": persisted.what_to_watch,
                "notify_voters": persisted.notify_voters,
            }
        else:
            st.session_state.pros_cons_cache[key] = None
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
                final_name = register_user(name)
                st.session_state.voter_username = final_name
                if final_name != name:
                    st.info(f'"{name}" was taken — you\'ve been signed up as "{final_name}".')
                st.rerun()

        st.markdown("---")
        st.caption("Share this page with others:")

        # QR code for sharing — auto-detects ngrok URL
        public_url = _get_public_url()
        qr_bytes = _generate_qr(public_url)
        st.image(qr_bytes, width=200, caption=public_url)


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
                    f"✅ {count} vote(s) submitted and saved. Thank you for having your say!"
                )
                st.balloons()
        with col_count:
            st.metric("Votes ready", len(st.session_state.votes))

    st.markdown("---")

    if is_demo_mode_active():
        st.info(
            "Demo mode is active: the selected meeting is being presented as upcoming so "
            "voters can review the pre-meeting analysis."
        )

    result = _load_council_data()
    if result is None:
        st.info("We need to fetch the latest council data. This may take a minute.")
        return

    meetings, agenda_items = result

    if not agenda_items:
        st.warning("No agenda items found in the latest pipeline run.")
        return

    # Build a meeting lookup
    meeting_lookup: dict[int, Meeting] = {m.meeting_id: m for m in meetings}

    demo_mode_active = is_demo_mode_active()

    # Separate upcoming and past items, deduplicating by item key
    upcoming_items = []
    past_items = []
    seen: set[str] = set()
    for item in agenda_items:
        key = _item_key(item)
        if key in seen:
            continue
        seen.add(key)
        if not is_voter_relevant_agenda_item(item):
            continue
        meeting = meeting_lookup.get(item.meeting_id)
        is_upcoming_item = is_demo_upcoming(item, meeting)
        persisted_analysis = _get_persisted_analysis(item, is_upcoming_item)
        if persisted_analysis is not None and not persisted_analysis.get("notify_voters", True):
            continue
        if demo_mode_active and persisted_analysis is None:
            continue
        if is_upcoming_item:
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

    if demo_mode_active:
        st.subheader(f"Upcoming ({len(upcoming_items)})")
        if not upcoming_items:
            st.info("No upcoming decisions found.")
        else:
            _render_items(upcoming_items, is_upcoming=True, prefix="up")
        return

    tab_upcoming, tab_past = st.tabs(
        [f"📋 Upcoming ({len(upcoming_items)})", f"📁 Past Decisions ({len(past_items)})"]
    )

    with tab_upcoming:
        if not upcoming_items:
            st.info("No upcoming decisions found.")
        else:
            _render_items(upcoming_items, is_upcoming=True, prefix="up")

    with tab_past:
        if not past_items:
            st.info("No past decisions found.")
        else:
            _render_items(past_items, is_upcoming=False, prefix="past")


def _render_items(
    items: list[tuple[AgendaItem, Meeting | None]], is_upcoming: bool, prefix: str = ""
):
    """Render agenda items with pros/cons and vote buttons."""
    for item, meeting in items:
        key = _item_key(item)
        committee_name = meeting.committee_name if meeting else "Unknown Committee"
        meeting_date = meeting.date if meeting else ""

        with st.expander(f"**{item.title}** — {committee_name} ({meeting_date})", expanded=False):
            # Show basic info
            if item.description:
                st.markdown(f"*{item.description}*")

            analysis = _get_persisted_analysis(item, is_upcoming)
            if analysis is not None:
                st.markdown(f"**Summary:** {analysis['summary']}")
                if analysis.get("why_it_matters"):
                    st.markdown(f"**Why it matters:** {analysis['why_it_matters']}")

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
                if analysis.get("what_to_watch"):
                    st.markdown(f"**What to watch:** {analysis['what_to_watch']}")
            else:
                st.info("Analysis is not ready for this item yet.")
                continue

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
                if st.button("Change vote", key=f"change-{prefix}-{key}"):
                    del st.session_state.votes[key]
                    st.rerun()
            else:
                st.markdown("**Cast your vote:**")
                col_for, col_against, col_abstain = st.columns(3)

                with col_for:
                    if st.button(
                        "👍 Vote For", key=f"for-{prefix}-{key}", use_container_width=True
                    ):
                        st.session_state.votes[key] = {
                            "vote": "for",
                            "user": st.session_state.voter_username,
                            "title": item.title,
                        }
                        st.rerun()
                with col_against:
                    if st.button(
                        "👎 Vote Against", key=f"against-{prefix}-{key}", use_container_width=True
                    ):
                        st.session_state.votes[key] = {
                            "vote": "against",
                            "user": st.session_state.voter_username,
                            "title": item.title,
                        }
                        st.rerun()
                with col_abstain:
                    if st.button(
                        "🤷 Abstain", key=f"abstain-{prefix}-{key}", use_container_width=True
                    ):
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
