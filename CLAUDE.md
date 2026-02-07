# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RunBase is a personal running data pipeline that ingests workout data from multiple sources (Apple Watch via HealthFit, Strava, Garmin, Runalyze, historical spreadsheets), reconciles them into a canonical SQLite database, and provides a Flask-based review UI for conflict resolution and data browsing.

## Environment

- Python 3.14 with a virtual environment at `./venv`
- Activate with: `source venv/bin/activate`
- Remote: `https://github.com/michaelkurish-insta/runbot.git`

## Key Commands

- `pip install -r requirements.txt` — install dependencies
- `python -m runbase db init` — initialize the SQLite database with full schema
- `python -m runbase sync` — sync data from sources (not yet implemented)
- `python -m runbase import` — import historical data (not yet implemented)
- `python -m runbase reconcile` — reconcile activities across sources (not yet implemented)
- `python -m runbase review` — launch the Flask review UI (not yet implemented)
- `python -m runbase status` — show pipeline status (not yet implemented)

## Architecture

```
runbase/
├── config.py          # YAML config loader (~/runbase paths, env var expansion)
├── db.py              # SQLite connection helper, schema init
├── models.py          # Dataclasses: Activity, ActivitySource, Interval, Stream, Shoe, Conflict, etc.
├── cli.py             # argparse CLI with subcommands
├── ingest/            # Data ingestion modules (fit_parser, icloud_sync, strava, etc.)
├── reconcile/         # Cross-source matching, conflict detection, field resolution
└── review/            # Flask app for conflict resolution and data browsing
```

## Configuration

- Config lives at `config/config.yaml` (copy from `config/config.example.yaml`)
- Paths support `~` expansion and `$ENV_VAR` substitution
- Default DB path: `~/runbase/data/runbase.db`

## Build Phases

See `runbase_build_plan.md` for the full phased build plan. Currently at Phase 0 (project skeleton).
