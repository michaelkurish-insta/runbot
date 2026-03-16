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
- `python -m runbase pipeline -v` — full sync pipeline: iCloud → Strava → enrich (cron-friendly)
- `python -m runbase db init` — initialize the SQLite database with full schema
- `python -m runbase sync --icloud -v` — sync .fit files from iCloud HealthFit folder
- `python -m runbase sync --strava -v` — sync from Strava API (incremental)
- `python -m runbase sync --strava --full-history -v` — full Strava history sync
- `python -m runbase import --xlsx -v` — import historical XLSX spreadsheet
- `python -m runbase reconcile -v` — match activities against orphaned Strava sources
- `python -m runbase reconcile --backfill-dates -v` — one-time: fetch dates for Strava orphans (requires API)
- `python -m runbase vdot` — show current VDOT + training paces
- `python -m runbase vdot --set 50` — set VDOT manually
- `python -m runbase vdot --from-race <activity_id>` — calculate VDOT from a race
- `python -m runbase enrich -v` — batch enrich all activities (pace zones, track detection, etc.)
- `python -m runbase enrich --activity <id> -v` — enrich a single activity
- `python -m runbase analyze locations -v` — show workout location clusters
- `python -m runbase review` — launch the Flask review UI at http://localhost:5050
- `python -m runbase review --debug` — launch with Flask debug mode (auto-reload on code changes)
- `python -m runbase status` — show pipeline status (not yet implemented)
- `python scripts/setup_strava_auth.py` — set up Strava OAuth tokens
- `python scripts/backfill_xlsx_fields.py -v` — one-time migration for strides + workout_category
- `python scripts/split_group_matched.py -v` — one-time: split group-matched activities into individual rows
- `python scripts/split_group_matched.py --dry-run -v` — preview split without writing
- `python scripts/tag_races.py -v` — scan activities for races, compute VDOT, populate timeline
- `python scripts/tag_races.py --dry-run -v` — preview race candidates without writing
- `python scripts/tag_races.py --yes --re-enrich -v` — non-interactive: tag races + re-enrich all

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
├── reconcile/
│   ├── matcher.py         # Find orphaned Strava sources matching date+distance, backfill dates
│   └── enricher.py        # Apply shoe/name/category from matched Strava source to activity
├── analysis/
│   ├── vdot.py            # VDOT calculator (Daniels-Gilbert), pace zones, per-activity VDOT estimation
│   ├── track_detect.py    # GPS-based track detection, 100m distance snapping
│   ├── pace_segments.py   # Stream-based pace segmentation for unstructured runs
│   ├── locations.py       # Workout location clustering, measured course detection
│   └── interval_enricher.py # Enrichment waterfall orchestrator
└── review/
    ├── app.py             # Flask routes, API endpoints, override logic
    ├── static/
    │   ├── app.js         # UI: modal detail view, inline editing, charts, maps
    │   └── style.css      # Styling: grid, modal, zone colors
    └── templates/
        ├── index.html     # Main page: calendar grid, sidebar, stats footer, modal
        └── components/    # activity_row.html
```

## Configuration

- Config lives at `config/config.yaml` (copy from `config/config.example.yaml`)
- Paths support `~` expansion and `$ENV_VAR` substitution
- Default DB path: `~/runbase/data/runbase.db`
- Strava credentials: set `STRAVA_CLIENT_ID` and `STRAVA_CLIENT_SECRET` env vars, or edit config directly
- `athlete.hr_max`: max heart rate (used for computed VDOT estimation)

## Build Phases

See `runbase_build_plan.md` for the full phased build plan.

- Phase 0 (skeleton): Complete
- Phase 1 (FIT parser + iCloud sync): Complete
- Phase 2a (XLSX import): Complete
- Phase 3 (Strava API sync): Complete
- Phase 2b (XLSX backfill — strides + workout_category): Complete
- Phase 4 (Reconciliation — FIT↔Strava matching + enrichment): Complete
- Phase 5 (Interval enrichment — VDOT, pace zones, track detection, walking scrub): Complete
- Phase 6 (Review UI): Complete — Flask UI with activity grid, detail panels, inline editing

## XLSX Cutoff Date

The `xlsx.cutoff_date` config setting (default: `2024-12-15`) causes the XLSX import to
skip any rows after that date. Post-cutoff activities come from Strava/FIT sources instead,
avoiding false matches where an XLSX workout name (e.g. "8x200m") gets paired with a
different Strava activity that just happened to match on date + distance.

## Key Patterns

- CLI uses lazy imports inside command handlers (keeps startup fast)
- Each sync source gets its own module in `runbase/ingest/`
- Single transaction per file import (activity + source + streams + laps + processed_files)
- Dedup via `processed_files` table (check by path or hash before importing)
- Strava sync matches existing activities by date + distance tolerance, fills missing fields
- XLSX note parsing uses a 5-pattern regex cascade (splits/full/pace+HR/pace-only/@pattern/fallback)
- FIT and Strava cadence for running is per-foot (half strides) — double for full strides/min
- Enrichment waterfall: track detection → measured course → workout tagging → walking scrub → stride detection → pace zones → elapsed pace zone → GAP → elevation → computed VDOT
- VDOT stored in `vdot_history` table; current VDOT = most recent entry on or before activity date
- Grade-adjusted pace (GAP) computed from stream altitude data using Minetti 2002 energy cost polynomial with capped downhill (0.785 min ratio)
- Computed VDOT (CVD): per-activity VO2max estimate from GAP + HR using the Daniels-Gilbert/Runalyze formula
- Adjusted VDOT (AVD): 21-day duration-weighted exponential moving average of CVDs (half-life 7 days) × calibrated multiplier
- Unstructured runs (easy/long/recovery) get pace segments from stream data; structured workouts keep FIT laps
- Auto-enrichment runs on new FIT imports if a VDOT is set

### Enrichment by Interval Type

Enrichment features apply differently to manual intervals vs auto-generated pace segments:

| Feature | Manual Intervals (fit_lap, strava_lap, xlsx_split) | Pace Segments (auto-generated) |
|---|---|---|
| Walking filter | Yes | No — instantaneous pace is unreliable due to hills, GPS noise, wind |
| Pace zone (per-segment) | Yes | Yes (assigned at creation) |
| Elapsed pace zone | No | Yes — overall activity pace (total dist / total time) classified by VDOT |
| Stride detection | Yes | No |
| Workout tagging | Yes (structured only) | No |
| Measured course snap | Yes (structured only) | No |
| Track detection | Yes | No |

The **elapsed pace zone** provides a more accurate effort score for unstructured runs.
Individual pace segments can appear more or less intense than the actual effort due to
terrain (hills, wind), so the elapsed pace zone uses the overall activity pace as a
complementary measure of true exertion.

### Review UI

The Flask review UI (`python -m runbase review`) provides:

- **Activity grid**: Year calendar with color-coded date cells, 7-day trailing mileage, monthly sections
- **Modal detail view**: Click a row to open a modal popup — shows intervals/laps, pace/HR charts, GPS map, and edit form. Multi-activity days show each activity sequentially (title, charts, intervals, edit form). Save & Close re-enriches the activity.
- **Same-day merging**: Multiple activities on one day display as a single merged grid row; the modal shows each activity separately with its own edit form
- **Inline editing**: Double-click grid cells (name, type zone) for quick edits. Double-click interval cells (distance, duration, HR, zone) in the modal.
- **Override system**: Activity edits go through `activity_overrides` table and sync to `activities` table, making them canonical (won't be overwritten by future imports)
- **Interval editing**: Double-click interval cells to edit; sets `source='manual'`; auto-recalculates pace; does NOT update parent activity aggregates
- **Stride/recovery toggles**: Checkbox toggles save immediately and update the grid's stride count in real time
- **Nullable fields**: HR, cadence, pace, duration can be set to NULL by saving an empty value
- **Planned activities**: Click future blank rows to enter planned distance/workout
- **Import button**: Triggers the full pipeline from the UI
- **Tabs**: Calendar (main grid), Shoes (mileage tracking), Trends (charts + race table), Race Prediction (model calibration)

### VDOT Estimation Pipeline

Three VDOT-related columns appear in the activity grid (right side):

| Column | Name | Description |
|--------|------|-------------|
| **AVD** | Adjusted VDOT | 21-day EWA of CVDs × multiplier, inclusive of that day's workout. Smoothed "race-ready" estimate. Computed on-the-fly in the grid endpoint. |
| **CVD** | Computed VDOT | Per-activity VO2max estimate from GAP + avg HR via Daniels-Gilbert formula with Runalyze's HR-to-%VO2max log regression. Stored on `activities.computed_vdot`. |
| **VDOT** | Timeline VDOT | Current VDOT from `vdot_history` table (race-derived or manual). Used for pace zone classification. |

**CVD formula** (Runalyze/Daniels-Gilbert approach):
1. `%VO2max = exp((avg_hr/hr_max - 1.00466) / 0.68725)` — HR-to-effort mapping
2. `speed_at_100% = GAP_velocity / %VO2max` — extrapolate max-effort speed
3. `VO2max = -4.60 + 0.182258 * speed + 0.000104 * speed^2` — Daniels oxygen cost
4. Requires `athlete.hr_max` in config.yaml; rejects HR% < 55% or > 105%

**AVD formula** (race prediction model):
1. 21-day window, 7-day half-life exponential decay, duration-weighted
2. Multiplier calibrated via least-squares against race VDOTs from `vdot_history`
3. Shared helpers: `_avd_ewa()` and `_avd_multiplier()` in `app.py`

### Race Prediction Tab

The Race Prediction tab documents calibration of the VDOT prediction model:
- Shows model description, calibrated parameters (window, half-life, multiplier, RMSE, MAE)
- Table of all races: 21-day EWA, predicted VDOT, actual race VDOT, error
- Predicted vs actual scatter chart
- The Trends tab also shows a "Current Race VDOT" daily line chart using the same model

### Group-Matched Activity Split

Group-matched activities (where multiple Strava sub-activities were stored as one combined row)
have been split into individual activity rows via `scripts/split_group_matched.py`. The review
UI's `_merge_day()` handles same-day display merging in the grid. New orphan promotions via
`promote_orphans()` also create one activity per orphan (not one per day-group).
