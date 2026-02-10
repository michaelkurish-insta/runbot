# RunBase

Personal running data pipeline. Ingests workout data from multiple sources, reconciles them into a canonical SQLite database, and (eventually) provides a review UI for conflict resolution and browsing.

## Data Sources

| Source | Status | Description |
|--------|--------|-------------|
| Apple Watch / HealthFit | Done | `.fit` files synced via iCloud |
| Training log spreadsheet | Done | Historical XLSX with note parsing, interval splits |
| Strava API | Done | Full-history sync with streams, laps, and shoe matching |
| Garmin Connect | Planned | API export |
| Runalyze | Planned | CSV scrape for training metrics |

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Copy the example config and fill in your values:

```bash
cp config/config.example.yaml config/config.yaml
# Edit config/config.yaml with your paths and API credentials
```

Initialize the database:

```bash
python -m runbase db init
```

## Usage

### Import historical XLSX data

```bash
python -m runbase import --xlsx -v
```

Parses distance, duration, pace, HR, cadence, interval splits, strides count, and workout category from the spreadsheet. Deduplicates by file hash.

### Sync from iCloud (HealthFit .fit files)

```bash
python -m runbase sync --icloud -v
```

### Sync from Strava

First, set up OAuth tokens:

```bash
python scripts/setup_strava_auth.py
```

Then sync:

```bash
# Incremental sync (new activities since last run)
python -m runbase sync --strava -v

# Full history (re-fetch everything, skip already-imported)
python -m runbase sync --strava --full-history -v

# Skip per-second streams for faster sync
python -m runbase sync --strava --no-streams -v
```

Strava sync matches against existing activities (date + distance tolerance), fills in missing fields (HR, cadence, elevation, laps, streams), and creates shoes.

### One-time migrations

```bash
# Backfill strides + workout_category for existing XLSX rows
python scripts/backfill_xlsx_fields.py -v
```

## Architecture

```
runbase/
├── config.py              # YAML config loader (~/ expansion, $ENV_VAR substitution)
├── db.py                  # SQLite connection helper, schema DDL
├── models.py              # Dataclasses: Activity, ActivitySource, Interval, Stream, etc.
├── cli.py                 # argparse CLI with subcommands
└── ingest/
    ├── fit_parser.py      # .fit file parsing (fitparse)
    ├── icloud_sync.py     # iCloud HealthFit folder scanner
    ├── xlsx_import.py     # XLSX import with note parsing, strides, categories
    └── strava_sync.py     # Strava API sync with rate limiting

config/
├── config.example.yaml    # Template config (check into git)
└── config.yaml            # Your config (gitignored)

scripts/
├── setup_strava_auth.py   # Strava OAuth token setup
└── backfill_xlsx_fields.py # Migration: strides + workout_category
```

## Database

SQLite with WAL mode at `~/runbase/data/runbase.db`. Key tables:

- **activities** — canonical activity records (one per real-world run)
- **activity_sources** — per-source raw data for audit trail
- **intervals** — interval/rep-level splits
- **streams** — per-second time series from .fit files and Strava
- **shoes** — shoe tracking (populated from Strava gear)
- **processed_files** — dedup manifest to avoid re-importing
