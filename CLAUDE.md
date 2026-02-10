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
- `python -m runbase sync --icloud -v` — sync .fit files from iCloud HealthFit folder
- `python -m runbase sync --strava -v` — sync from Strava API (incremental)
- `python -m runbase sync --strava --full-history -v` — full Strava history sync
- `python -m runbase import --xlsx -v` — import historical XLSX spreadsheet
- `python -m runbase reconcile` — reconcile activities across sources (not yet implemented)
- `python -m runbase review` — launch the Flask review UI (not yet implemented)
- `python -m runbase status` — show pipeline status (not yet implemented)
- `python scripts/setup_strava_auth.py` — set up Strava OAuth tokens
- `python scripts/backfill_xlsx_fields.py -v` — one-time migration for strides + workout_category

## Architecture

```
runbase/
├── config.py              # YAML config loader (~/runbase paths, env var expansion)
├── db.py                  # SQLite connection helper, schema init
├── models.py              # Dataclasses: Activity, ActivitySource, Interval, Stream, Shoe, Conflict, etc.
├── cli.py                 # argparse CLI with subcommands
├── ingest/
│   ├── fit_parser.py      # .fit file parsing via fitparse
│   ├── icloud_sync.py     # iCloud HealthFit folder scanner + importer
│   ├── xlsx_import.py     # XLSX import with note parsing, interval splits, strides, categories
│   └── strava_sync.py     # Strava API sync with rate limiting, stream/lap fetch, shoe matching
├── reconcile/             # Cross-source matching, conflict detection, field resolution (planned)
└── review/                # Flask app for conflict resolution and data browsing (planned)
```

## Configuration

- Config lives at `config/config.yaml` (copy from `config/config.example.yaml`)
- Paths support `~` expansion and `$ENV_VAR` substitution
- Default DB path: `~/runbase/data/runbase.db`
- Strava credentials: set `STRAVA_CLIENT_ID` and `STRAVA_CLIENT_SECRET` env vars, or edit config directly

## Build Phases

See `runbase_build_plan.md` for the full phased build plan.

- Phase 0 (skeleton): Complete
- Phase 1 (FIT parser + iCloud sync): Complete
- Phase 2a (XLSX import): Complete
- Phase 3 (Strava API sync): Complete
- Phase 2b (XLSX backfill — strides + workout_category): Code done, pending migration run

## Key Patterns

- CLI uses lazy imports inside command handlers (keeps startup fast)
- Each sync source gets its own module in `runbase/ingest/`
- Single transaction per file import (activity + source + streams + laps + processed_files)
- Dedup via `processed_files` table (check by path or hash before importing)
- Strava sync matches existing activities by date + distance tolerance, fills missing fields
- XLSX note parsing uses a 5-pattern regex cascade (splits/full/pace+HR/pace-only/@pattern/fallback)
- FIT and Strava cadence for running is per-foot (half strides) — double for full strides/min
