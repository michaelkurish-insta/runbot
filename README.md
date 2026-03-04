# RunBase

Personal running data pipeline. Ingests workout data from multiple sources, reconciles them into a canonical SQLite database, and provides a review UI for data browsing, conflict resolution, and manual corrections.

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

# Backfill Strava laps for XLSX activities (one-time, ~4 API calls/min)
# Fetches lap data with timestamps for activities that already have XLSX intervals.
# Needed for measured course detection (work-rep centroid matching).
python -m runbase sync --strava --backfill-laps -v
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

### Review UI

```bash
# Launch the review UI at http://localhost:5050
python -m runbase review

# With auto-reload for development
python -m runbase review --debug
```

The review UI provides a year calendar grid with a modal detail view. Click any activity row to open a popup with interval tables, pace/HR charts, GPS map, and an edit form. Multi-activity days show each activity's content sequentially (title, charts, intervals, edit form). Activity-level edits use a batch Save / Save & Close flow; interval-level operations (stride/recovery toggles, inline cell edits) save immediately. Double-click grid cells (name, type zone) for quick inline editing. Future blank rows accept planned distance/workout entries. An Import button triggers the full pipeline from the UI.

### One-time migrations

```bash
# Backfill strides + workout_category for existing XLSX rows
python scripts/backfill_xlsx_fields.py -v

# Split group-matched activities into individual rows (preview first)
python scripts/split_group_matched.py --dry-run -v
python scripts/split_group_matched.py -v
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
├── analysis/
│   ├── interval_enricher.py # Enrichment waterfall orchestrator
│   ├── vdot.py            # VDOT calculator (Daniels-Gilbert), pace zones
│   ├── workout_tagger.py  # Structured workout lap classification (warmup/work/recovery/cooldown)
│   ├── track_detect.py    # Oval template matching for track detection
│   ├── pace_segments.py   # Stream-based pace segmentation
│   └── locations.py       # Workout location clustering, measured course matching
└── review/
    ├── app.py             # Flask routes, API endpoints, override logic
    ├── static/
    │   ├── app.js         # UI: modal detail view, inline editing, charts, maps
    │   └── style.css      # Styling: grid, modal, zone colors
    └── templates/
        ├── index.html     # Main page: calendar grid, sidebar, stats footer, modal
        └── components/
            └── activity_row.html  # Grid row template

config/
├── config.example.yaml    # Template config (check into git)
└── config.yaml            # Your config (gitignored)

scripts/
├── setup_strava_auth.py        # Strava OAuth token setup
├── backfill_xlsx_fields.py     # Migration: strides + workout_category
├── split_group_matched.py      # Migration: split group-matched activities
└── tag_races.py                # Scan activities for races, compute VDOT, populate timeline
```

## Database

SQLite with WAL mode at `~/runbase/data/runbase.db`. Key tables:

- **activities** — canonical activity records (one per real-world run)
- **activity_sources** — per-source raw data for audit trail (one per import source per activity)
- **intervals** — interval/rep-level splits (FIT laps, Strava laps, XLSX splits, pace segments)
- **streams** — per-second time series from .fit files and Strava (lat/lon, HR, cadence, pace)
- **shoes** — shoe tracking (populated from Strava gear)
- **processed_files** — dedup manifest to avoid re-importing
- **detected_tracks** — cached track locations for fast lookup
- **activity_overrides** — manual field-level overrides from the review UI (synced to activities table)
- **planned_activities** — future planned runs entered via the review UI
- **vdot_history** — VDOT values over time (current = most recent entry on or before activity date)

## Reconciliation & Linking

Activities arrive from multiple sources (FIT files via iCloud, Strava API, historical XLSX spreadsheet). The reconciliation system links these sources together into canonical activity records.

### How Sources Become Activities

Each import source creates an `activity_sources` row. The reconciliation process matches these to `activities`:

1. **FIT import** (`sync --icloud`): Creates a new `activities` row + `activity_sources` row with `source='healthfit'`. The activity has GPS streams, HR, cadence, and laps, but may have a generic name like "Outdoor Running".

2. **Strava sync** (`sync --strava`): Fetches activities from the Strava API. If a Strava activity matches an existing activity by date (±1 day) and distance (±5%), it links as a new `activity_sources` row and fills missing fields (name, shoe, HR). If no match, it becomes an **orphaned source** — stored but not yet linked.

3. **XLSX import** (`import --xlsx`): Creates `activities` rows from the spreadsheet with `source='master_xlsx'`. These have hand-entered distances, workout names, interval splits, and shoe assignments.

### Matching Strategies

The reconcile command (`reconcile -v`) runs three matching passes:

**1:1 Matching**: For each activity without a Strava link, search orphaned Strava sources for a single match on date (±1 day) + distance (±5%). The pipeline's lightweight reconcile pass uses this strategy.

**Group Matching**: When no single orphan matches, check if multiple same-day orphans sum to the activity's distance (±10%). This handles days where a single logged run was actually recorded as separate Strava activities (e.g., warm-up + main workout + cool-down). All orphans in the group link to the same activity.

**Orphan Promotion**: Strava sources that can't match any existing activity (post-XLSX-cutoff dates) get promoted to standalone activities. Each orphan becomes its own activity row. The review UI's same-day merging handles display grouping.

### After Matching

When an orphan links to an activity, enrichment applies:
- **Shoe**: Strava `gear_id` maps to the `shoes` table
- **Name**: Replaces generic FIT names ("Outdoor Running") with Strava activity names
- **Category**: Inferred from Strava `workout_type` (race/long/workout) and name patterns ("tempo", "intervals", "easy")
- **Streams + Laps**: Fetched from the Strava API for GPS, pace, and HR data

### The Pipeline

`python -m runbase pipeline -v` runs the full automated flow:

1. **iCloud sync** — import new .fit files
2. **Strava sync** — fetch new Strava activities, match to existing
3. **Lightweight reconcile** — 1:1 match any remaining unlinked activities
4. **Enrich** — run the enrichment waterfall on new + reconciled activities

### Manual Overrides

The review UI allows manual edits to activity fields (distance, duration, pace, HR, cadence, shoe, name, notes) and interval fields (distance, duration, HR, zone). Activity edits are stored in `activity_overrides` and synced to the `activities` table, making them canonical — future imports will not overwrite them. Interval edits set the interval's `source` to `'manual'`.

## VDOT & Pace Zone System

All pace classification in RunBase is based on Jack Daniels' VDOT system. VDOT is a fitness metric derived from race performances using the Daniels-Gilbert formula — it represents your current VO2max-equivalent fitness level.

### How VDOT Works

A VDOT value (typically 30-85 for recreational to elite runners) determines training paces for six zones:

| Zone | Purpose | %VO2max | Example (VDOT 50) |
|------|---------|---------|-------------------|
| **E** (Easy) | Aerobic base, recovery | 70% | ~9:30/mi |
| **M** (Marathon) | Marathon race pace | 82% | ~8:00/mi |
| **T** (Threshold) | Lactate threshold / tempo | 88% | ~7:15/mi |
| **I** (Interval) | VO2max development | 98% | ~6:25/mi |
| **R** (Repetition) | Speed & running economy | 107% | ~5:50/mi |
| **FR** (Fast Rep) | Fast reps / sprint work | 115% | ~5:20/mi |

Each zone corresponds to a target %VO2max. The formula converts VDOT → VO2 → velocity → pace for each zone. Zone boundaries are placed at %VO2max midpoints between adjacent zones (e.g., E/M boundary at 76%, M/T at 85%, T/I at 93%). A walking threshold (default 11:00/mi) catches intervals too slow to be running.

### Setting VDOT

```bash
# View current VDOT and training paces
python -m runbase vdot

# Set manually
python -m runbase vdot --set 50

# Calculate from a race result
python -m runbase vdot --from-race <activity_id>
```

VDOT is stored in the `vdot_history` table with effective dates. The enricher uses the most recent VDOT entry on or before each activity's date, so zone boundaries evolve as your fitness changes. Race tagging from the review UI can also insert VDOT history entries.

### Workout Type Zone

Each activity in the grid displays a color-coded **workout type zone** (the "Type" column). This is the highest-priority pace zone found across the activity's qualifying intervals — those that are not strides, recoveries, walking, or hill sprints. An easy run shows "E", a tempo workout shows "T", an interval session shows "I", etc.

When an activity has both FIT and Strava laps (duplicate sources), only FIT laps are considered for workout type determination. This FIT-preferred source logic applies consistently across the UI: interval display, stride counting, and workout type classification.

## Enrichment Pipeline

The `enrich` command runs a waterfall of analysis steps on each activity:

1. **Category inference** — If the activity has no `workout_category`, infer one from the name (e.g., "tempo" → tempo, "8x400" → interval).
2. **Structured vs unstructured** — Workouts with intervals (repetition, tempo, interval, fartlek, hills, race) keep their FIT/XLSX laps. Unstructured runs (easy, long, recovery) get pace segments auto-generated from GPS stream data.
3. **Workout tagging** (structured only) — Classifies laps as warmup, work, recovery, or cooldown using VDOT pace zones. Groups work intervals into sets, separated by walking laps, long recoveries (≥2x median), or distance breaks (≥0.3mi).
4. **Track detection** — Determines if the activity was on a 400m track using oval template matching (see below).
5. **Measured course detection** — For structured workouts only, checks if work-rep GPS centroids are near configured measured courses and snaps intervals to known distances (see below).
6. **Walking scrub** — Flags intervals slower than the walking threshold (default 11:00/mi). Skips pace segments (instantaneous pace unreliable) and manual intervals (user edits are canonical).
7. **Hill sprint detection** — Very short intervals (<56m) with uphill elevation gain. Runs before stride detection so hills take priority.
8. **Stride detection** — Flags short intervals (<30s, >24m) as strides. Only applied to manually lapped intervals (FIT/Strava/XLSX), not pace segments. When both FIT and Strava laps exist, only FIT laps are candidates (higher GPS resolution). Stride flags are propagated to matching Strava laps by rep number.
9. **Pace zone assignment** — Labels each interval's pace zone (E/M/T/I/R/FR) based on current VDOT boundaries.
10. **Elapsed pace zone** (pace segments only) — Computes overall activity pace (total distance / total time) and classifies it. This gives a more stable effort score than instantaneous segment paces, which can be distorted by hills, GPS noise, and wind.
11. **Adjusted distance & pace** — Sums non-walking interval distances. Computes running pace excluding walking time. Respects manual overrides.
12. **Stride/hill counts** — Updates the activity's `strides` and `hill_sprints` fields from visible intervals (FIT-preferred when both sources exist).

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

### FIT-Preferred Source Logic

Many activities have intervals from two sources: FIT laps (from the watch, with high-resolution GPS) and Strava laps (from the API, lower resolution). Rather than showing both, the system uses a consistent FIT-preferred policy:

- **Interval display**: When both FIT and Strava laps exist, only FIT laps are shown in the modal. Strava laps are hidden.
- **Stride counting**: The `strides` field on each activity counts only from visible (FIT-preferred) intervals.
- **Workout type zone**: The grid's "Type" column is derived only from visible intervals. Even if a Strava lap has a faster pace zone, it won't affect the displayed type when FIT laps exist.
- **Stride detection**: FIT laps are preferred as stride candidates (higher GPS resolution for short intervals). Detected stride flags are propagated to corresponding Strava laps by rep number so they don't leak into other computations.

This prevents Strava "tail" laps (auto-created by Strava with different properties) from distorting stride counts, workout types, or other derived values.

### Enrichment by Interval Type

Enrichment features apply differently to manual intervals vs auto-generated pace segments:

| Feature | Manual Intervals (fit_lap, strava_lap, xlsx_split) | Pace Segments (auto-generated) |
|---|---|---|
| Walking filter | Yes | No — instantaneous pace unreliable |
| Pace zone (per-segment) | Yes | Yes (assigned at creation) |
| Elapsed pace zone | No | Yes — overall activity pace classified by VDOT |
| Stride detection | Yes | No |
| Hill sprint detection | Yes | No |
| Workout tagging | Yes (structured only) | No |
| Measured course snap | Yes (structured only) | No |
| Track detection | Yes | No |
