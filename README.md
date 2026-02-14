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

### Reconcile and enrich

```bash
# Match activities against orphaned Strava sources
python -m runbase reconcile -v

# Run enrichment pipeline (VDOT zones, track detection, walking scrub, etc.)
python -m runbase enrich -v

# Enrich a single activity
python -m runbase enrich --activity 718 -v

# Set or view VDOT
python -m runbase vdot
python -m runbase vdot --set 50
```

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
├── ingest/
│   ├── fit_parser.py      # .fit file parsing (fitparse)
│   ├── icloud_sync.py     # iCloud HealthFit folder scanner
│   ├── xlsx_import.py     # XLSX import with note parsing, strides, categories
│   └── strava_sync.py     # Strava API sync with rate limiting
├── reconcile/
│   ├── matcher.py         # Match activities to orphaned Strava sources
│   └── enricher.py        # Apply shoe/name/category from matched sources
└── analysis/
    ├── interval_enricher.py # Enrichment waterfall orchestrator
    ├── vdot.py            # VDOT calculator (Daniels-Gilbert), pace zones
    ├── track_detect.py    # Oval template matching for track detection
    ├── pace_segments.py   # Stream-based pace segmentation
    └── locations.py       # Workout location clustering, measured course matching

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
- **detected_tracks** — cached track locations for fast lookup

## Enrichment Pipeline

The `enrich` command runs a waterfall of analysis steps on each activity:

1. **Structured vs unstructured** — Workouts with intervals (repetition, tempo, interval) keep their FIT/XLSX laps. Unstructured runs (easy, long, recovery) get pace segments generated from stream data.
2. **Track detection** — Determines if the activity was on a 400m track using oval template matching (see below).
3. **Measured course detection** — For structured workouts only, checks if the activity centroid is near a configured measured course and snaps intervals to known course distances (see below).
4. **Walking scrub** — Flags intervals slower than the walking threshold (default 11:00/mi).
5. **Stride detection** — Flags intervals shorter than 30s as strides.
6. **Pace zone assignment** — Labels each interval's pace zone (E/M/T/I/R/FR) based on current VDOT.
7. **Adjusted distance** — Sums non-walking interval distances.
8. **VDOT storage** — Stores the current VDOT on the activity record.

### Track Detection

Track detection uses a sliding window + OpenCV shape matching approach. A standard 400m lane-1 oval (two 84.39m straights + two semicircular turns of radius 36.5m) is generated as a template contour. The algorithm:

1. **Sliding window**: Scan the GPS stream in windows of 300 points (step 50). This isolates the track portion even in activities with warmup/cooldown on roads.
2. **Known track lookup**: Check if the window centroid is within 200m of a previously detected track. If so, label as track immediately.
3. **Convex hull matching**: Compute the convex hull of the window's GPS points and compare to the oval template via `cv2.matchShapes` (score < 0.15).
4. **Dimension checks**: Short axis 50-120m, long axis 120-220m, aspect ratio 1.5-3.0, fill ratio > 0.75.
5. **Decision**: Best-scoring passing window determines the track time range. Save the location for future lookups.

Detected tracks are stored in the `detected_tracks` table with centroid coordinates, orientation, and fit score. Intervals overlapping the track time window get `location_type = "track"`.

**Distance snapping** uses a three-tier system based on what the activity name tells us:

1. **Race** (name contains "Race", "TT", "time trial", "parkrun"): The interval closest to the parsed race distance (e.g. "Mile Race" → 1609m) gets snapped to the exact race distance and flagged `is_race = TRUE`. If no distance is found in the name, the longest track interval is snapped to the closest common race distance. Other intervals (warm-up, cool-down) are not snapped.

2. **Workout** (name contains "NxDist" like "6x400", "repeats", "intervals"): Only *work sets* are snapped — intervals faster than the activity's average pace. Warm-up/cool-down laps on the track at easy pace are left unsnapped. This prevents a 2800m warm-up jog from being treated as a prescribed distance.

3. **Generic** (no workout or race name): Intervals between 180m and 1300m are snapped to the nearest 100m. Below 180m is likely strides. Above 1300m is likely a warm-up mile. Both are left unsnapped.

The raw `gps_measured_distance_mi` is always preserved. Race intervals also get `is_race = TRUE` for downstream analysis.

### Measured Course Detection

Measured courses are user-whitelisted loops with known distances, configured in `config.yaml` under `paces.measured_courses`. Each entry has a lat/lon centroid, radius, and exact snap distance in meters.

The enricher applies measured course snapping only to **structured workouts** (tempo, interval, repetition, fartlek, hills, race) — not easy runs, whose FIT auto-laps would create false positives. Auto-generated pace segments are also excluded.

For each structured activity near a measured course area, each non-recovery interval is matched to the course whose `snap_distance_m` is closest to the GPS-measured distance (within 20% tolerance). The interval's `canonical_distance_mi` is set to the exact course distance and `location_type` is set to `"measured_course"`.

This allows different distances to coexist at the same training area. For example, a single location might have a 200m loop, 400m loop, 800m loop, and a mile loop — each with its own snap distance. The mile loop snaps to 1609m (exact mile), not 1600m.

Example config:

```yaml
paces:
  measured_courses:
    - name: "My 200m loop"
      lat: 40.3666
      lon: -75.2981
      radius_m: 1200
      snap_distance_m: 200
    - name: "My mile loop"
      lat: 40.3685
      lon: -75.2926
      radius_m: 1200
      snap_distance_m: 1609    # exact mile
```
