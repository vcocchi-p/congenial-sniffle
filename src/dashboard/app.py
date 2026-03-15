"""Streamlit monitoring dashboard for the pipeline."""

from __future__ import annotations

from datetime import datetime, timezone

import streamlit as st

from src.models.documents import AgentEvent

# --- State ---
if "events" not in st.session_state:
    st.session_state.events: list[AgentEvent] = []

if "documents_fetched" not in st.session_state:
    st.session_state.documents_fetched = 0

if "summaries_generated" not in st.session_state:
    st.session_state.summaries_generated = 0


def add_event(event: AgentEvent):
    st.session_state.events.append(event)


# --- Layout ---
st.set_page_config(page_title="Westminster Pipeline Monitor", layout="wide")
st.title("Westminster Council Pipeline Monitor")

col1, col2, col3 = st.columns(3)
col1.metric("Documents Fetched", st.session_state.documents_fetched)
col2.metric("Summaries Generated", st.session_state.summaries_generated)
col3.metric("Agent Events", len(st.session_state.events))

st.divider()

# --- Agent Event Log ---
st.subheader("Agent Event Log")

if st.session_state.events:
    for event in reversed(st.session_state.events):
        icon = {"started": "🟢", "progress": "🔵", "completed": "✅", "error": "🔴"}.get(
            event.event_type, "⚪"
        )
        st.markdown(
            f"{icon} **{event.agent_name}** — {event.message}  \n"
            f"<small>{event.timestamp:%H:%M:%S}</small>",
            unsafe_allow_html=True,
        )
else:
    st.info("No events yet. Run the pipeline to see activity here.")

st.divider()

# --- Pipeline Controls ---
st.subheader("Pipeline Controls")

if st.button("Run Retrieval"):
    add_event(
        AgentEvent(
            agent_name="retriever",
            event_type="started",
            message="Starting document retrieval...",
            timestamp=datetime.now(timezone.utc),
        )
    )
    st.rerun()
