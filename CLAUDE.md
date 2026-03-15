# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Hackathon project (Sequel 2026) built by two collaborators. Makes Westminster Council decisions accessible to voters and relays voter sentiment back to councillors.

## Commands

```bash
# Setup (requires Python 3.10+)
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Tests
pytest                        # all tests
pytest tests/test_models.py   # single file
pytest -k "test_name"         # single test by name
pytest --cov=src              # with coverage

# Lint
ruff check src/ tests/
ruff format src/ tests/

# Dashboard
streamlit run src/dashboard/app.py
```

## Architecture

Three-stage pipeline:

1. **Retrieval** (`src/agents/`) — Multi-agent framework using Claude Agent SDK to scrape/fetch Westminster Council documents (meetings, decisions, voting records) from `committees.westminster.gov.uk`.
2. **Parsing & Summarisation** (`src/parser/`) — Strips HTML, calls OpenAI (`gpt-4o`) to produce plain-language summaries voters can understand.
3. **Monitoring Dashboard** (`src/dashboard/`) — Streamlit app showing pipeline status, agent events, and metrics. Will also serve as the councillor-facing voter sentiment view.

## Key Conventions

- **Data models** live in `src/models/documents.py` using Pydantic — `CouncilDocument`, `VoterSummary`, `VotingIntention`, `AgentEvent`.
- **Agent events** (`AgentEvent`) are the shared contract between agents and the dashboard for monitoring.
- Tests that hit external APIs (OpenAI, Westminster site) should be kept separate from unit tests. Unit tests must not make network calls.
- Environment variables: `OPENAI_API_KEY` (required for summarisation), `ANTHROPIC_API_KEY` (required for Claude agents). Store in `.env` (gitignored).
