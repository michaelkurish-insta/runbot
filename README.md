# RunBase

Personal running data pipeline. Ingests workout data from multiple sources (Apple Watch via HealthFit, Strava, Garmin, historical spreadsheets), reconciles them into a canonical SQLite database, and provides a Flask-based review UI for data browsing, conflict resolution, training load tracking, and race prediction.

## What Each Activity Records

Every run is stored as a canonical activity row with data merged from all available sources. Here's what gets tracked:

| Field | Description |
|-------|-------------|
| **date** | Activity date (YYYY-MM-DD) |
| **distance_mi** | Raw total distance in miles |
| **adjusted_distance_mi** | Canonical running distance (excludes walking segments) |
| **duration_s** | Total elapsed time in seconds |
| **avg_pace_s_per_mi** | Average pace (seconds per mile), recomputed from adjusted distance |
| **avg_hr** | Average heart rate (bpm) |
| **avg_cadence** | Average cadence (full strides/min — FIT/Strava values are doubled from per-foot) |
| **elevation_gain_ft / elevation_loss_ft** | Cumulative climb/descent |
| **gap_s_per_mi** | Grade-adjusted pace (Minetti 2002 polynomial, see GAP section) |
| **computed_vdot** | Per-activity CVD: VO2max estimate from GAP + HR |
| **vdot** | Timeline VDOT from `vdot_history` (race-derived or manual) |
| **intensity_score** | Zone-weighted training load score (see Intensity section) |
| **strides** | Count of stride intervals detected |
| **hill_sprints** | Count of hill sprint intervals detected |
| **workout_name** | Human-readable name (e.g., "8x400m", "Easy 5") |
| **workout_category** | Structural category: easy, long, recovery, tempo, interval, repetition, fartlek, hills, race |
| **shoe_id** | Foreign key to shoes table |
| **temperature_f** | Air temperature at run start (Open-Meteo) |
| **humidity_pct** | Relative humidity at run start |
| **weather_conditions** | WMO weather code description (e.g., "Partly Cloudy", "Light Rain") |
| **cloud_cover_pct** | Cloud cover percentage (0-100) |
| **suppress_hr** | Flag: HR data is suspect, exclude from CVD and averages |
| **suppress_cadence** | Flag: cadence data is unreliable |
| **exclude_from_vdot** | Flag: exclude this activity from AVD/CVD calculations |

### Workout Type Zone

The grid's color-coded "Type" column shows the predominant pace zone of the activity's work intervals:

| Zone | Meaning | %VO2max |
|------|---------|---------|
| **E** | Easy / aerobic base | 70% |
| **M** | Marathon pace | 82% |
| **T** | Threshold / tempo | 88% |
| **I** | Interval / VO2max | 98% |
| **R** | Repetition / speed | 107% |
| **FR** | Fast reps / sprints | 115% |

Combo types (e.g., "I/R", "T/I") appear when an activity has work intervals at multiple distinct distances falling in different zones. E and M are excluded from combos since they represent warmup/cooldown — "E/I" is just "I". Zone boundaries are computed from the current VDOT using Daniels-Gilbert %VO2max midpoints between adjacent zones.

### Intervals

Each activity contains interval-level data from one or more sources:

| Source | Origin | When Used |
|--------|--------|-----------|
| `NULL` (FIT) | Apple Watch .fit file laps | Highest priority — raw GPS data |
| `strava_lap` | Strava API lap data | Used when no FIT laps exist |
| `xlsx_split` | Parsed from XLSX workout notes | Historical data |
| `pace_segment` | Auto-generated from GPS streams | Unstructured runs (easy/long/recovery) |
| `manual` | User edits via review UI | Canonical — never overwritten |

Each interval tracks: distance, duration, pace, HR, cadence, pace zone, and boolean flags for `is_recovery`, `is_walking`, `is_stride`, `is_hill_sprint`, and `is_race`.

## Data Sources

| Source | Status | Description |
|--------|--------|-------------|
| Apple Watch / HealthFit | Done | `.fit` files synced via iCloud |
| Training log spreadsheet | Done | Historical XLSX with note parsing, interval splits |
| Strava API | Done | Full-history sync with streams, laps, and shoe matching |
| Garmin Connect | Done | API sync with weather data |
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

### Configuration

Config lives at `config/config.yaml`. Key settings:

| Setting | Purpose |
|---------|---------|
| `paths.db` | SQLite database path (default: `~/runbase/data/runbase.db`) |
| `paths.icloud_healthfit` | iCloud HealthFit folder for .fit file scanning |
| `strava.client_id` / `strava.client_secret` | Strava API credentials (or use `$STRAVA_CLIENT_ID` env var) |
| `athlete.hr_max` | Max heart rate — required for CVD estimation (e.g., 189) |
| `xlsx.cutoff_date` | Date after which XLSX rows are skipped (default: 2024-12-15) |
| `paces.measured_courses` | List of known courses with GPS centroids and snap distances |
| `review.host` | Flask bind address (change to `0.0.0.0` for remote access) |

## Usage

### Full pipeline (recommended for daily use)

```bash
python -m runbase pipeline -v
```

Runs: iCloud sync → Strava sync → lightweight reconcile → enrich new activities. Cron-friendly.

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
python -m runbase sync --strava --backfill-laps -v
```

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

### One-time migrations

```bash
# Backfill strides + workout_category for existing XLSX rows
python scripts/backfill_xlsx_fields.py -v

# Split group-matched activities into individual rows (preview first)
python scripts/split_group_matched.py --dry-run -v
python scripts/split_group_matched.py -v

# Scan activities for races, compute VDOT, populate timeline
python scripts/tag_races.py -v
```

## Training Load System (ATL / CTL / TSB)

RunBase computes training load metrics using an intensity-score-based system analogous to TRIMP (Training Impulse). These appear in the calendar grid, the sidebar stats, and the Trends tab charts.

### Intensity Score

Each activity gets an intensity score computed from its intervals' pace zones and durations:

| Zone | Points per Minute |
|------|-------------------|
| walk | 0.0 |
| E | 0.2 |
| M | 0.4 |
| T | 0.6 |
| I | 1.0 |
| R | 1.5 |
| FR | 2.0 |

Plus flat bonuses: **+0.2 per stride**, **+0.25 per hill sprint**.

**Total I-score = sum(zone_pts_per_min x duration_min) + (strides x 0.2) + (hills x 0.25)**

The system uses a priority cascade for interval source: manual splits first, then FIT/Strava laps, then pace segments, then a whole-activity fallback using the activity-level pace.

### ATL, CTL, and TSB

Three exponential moving averages (EMAs) are computed daily from the intensity score time series:

| Metric | Full Name | Window | Smoothing Factor | Interpretation |
|--------|-----------|--------|------------------|----------------|
| **ATL** | Acute Training Load | 7-day | lambda = 2/(7+1) = 0.25 | Recent fatigue — how hard you've trained lately |
| **CTL** | Chronic Training Load | 42-day | lambda = 2/(42+1) ~ 0.0465 | Long-term fitness — accumulated training effect |
| **TSB** | Training Stress Balance | — | CTL - ATL | Freshness — positive = rested, negative = fatigued |

**Daily update formula:**

```
TRIMP[d] = sum of all activities' intensity scores on day d

ATL[d] = TRIMP[d] * 0.25 + ATL[d-1] * 0.75
CTL[d] = TRIMP[d] * 0.0465 + CTL[d-1] * 0.9535
TSB[d] = CTL[d] - ATL[d]
```

The EMA is seeded from 90 days before each view window to stabilize the initial values. ATL/CTL appear in the grid's rightmost columns and as line charts in the Trends tab.

### 7-Day Trailing Averages

The grid also shows:
- **7d MA** — 7-day trailing mileage (sum of adjusted distances)
- **7d I** — 7-day trailing intensity (sum of intensity scores)

## VDOT & Pace Zone System

All pace classification is based on Jack Daniels' VDOT system. VDOT is a fitness metric derived from race performances using the Daniels-Gilbert formula — it represents your current VO2max-equivalent fitness level.

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

Zone boundaries are placed at %VO2max midpoints between adjacent zones (E/M at 76%, M/T at 85%, T/I at 93%). A walking threshold (default 11:00/mi) catches intervals too slow to be running.

### Setting VDOT

```bash
python -m runbase vdot              # View current VDOT and training paces
python -m runbase vdot --set 50     # Set manually
python -m runbase vdot --from-race <activity_id>  # Calculate from a race
```

VDOT is stored in the `vdot_history` table with effective dates. The enricher uses the most recent entry on or before each activity's date, so zone boundaries evolve as fitness changes.

## Race Prediction System (CVD / AVD)

RunBase estimates fitness from every workout and predicts race performance using a three-layer VDOT pipeline. These columns appear in the activity grid:

| Column | Name | Description |
|--------|------|-------------|
| **CVD** | Computed VDOT | Per-activity VO2max estimate from GAP + HR |
| **AVD** | Adjusted VDOT | 21-day smoothed average of CVDs, calibrated to race results |
| **VDOT** | Timeline VDOT | Race-derived or manual VDOT from `vdot_history` |

### Step 1: Grade-Adjusted Pace (GAP)

Raw pace is adjusted for terrain using the Minetti 2002 energy cost polynomial. For each 30m distance window in the GPS stream, the grade (rise/run) is computed and an energy cost multiplier is applied. Downhill benefit is capped at a 0.785 ratio (running downhill isn't infinitely easier). The result is the pace you would have run on flat ground at the same effort.

### Step 2: Computed VDOT (CVD)

Each activity with both GAP and average HR produces a CVD — an estimate of what VDOT you were running at during that workout. The formula combines Daniels-Gilbert oxygen cost with Runalyze's HR-to-%VO2max log regression:

```
1. %VO2max = exp((avg_hr / hr_max - 1.00466) / 0.68725)
     → maps heart rate to fraction of maximum oxygen uptake
     → rejects HR% < 55% or > 105% as unreliable

2. speed_at_100% = GAP_velocity / %VO2max
     → extrapolates: "if I ran this fast at X% effort, how fast at 100%?"

3. CVD = -4.60 + 0.182258 * speed + 0.000104 * speed²
     → Daniels-Gilbert oxygen cost equation
     → speed in meters per minute
```

Requires `athlete.hr_max` in config.yaml. Activities with suppressed HR or excluded-from-VDOT flags are skipped.

### Step 3: Adjusted VDOT (AVD)

AVD smooths daily CVD fluctuations into a "race-ready" fitness estimate using a 21-day duration-weighted exponential moving average:

```
For each activity in the past 21 days:
    days_ago = (target_date - activity_date).days
    weight = exp(-ln(2) * days_ago / 7) * max(duration_s, 60)

AVD_raw = sum(weight * CVD) / sum(weight)
AVD = AVD_raw * multiplier
```

| Parameter | Value | Meaning |
|-----------|-------|---------|
| Window | 21 days | Only recent workouts count |
| Half-life | 7 days | A workout's influence halves every 7 days |
| Duration weighting | max(duration, 60s) | Longer runs contribute more |
| Multiplier | ~0.9772 | Calibrated against race results |

The multiplier is calibrated via least-squares regression: `multiplier = sum(ewa * race_vdot) / sum(ewa^2)` across all races in `vdot_history`.

**Inclusive vs exclusive:** AVD in the grid uses `inclusive=True` (includes that day's workout). Race prediction uses `inclusive=False` (what was your fitness *before* race day?).

### Race Prediction Tab

The Race Prediction tab shows model calibration:

- Calibrated parameters: window, half-life, multiplier, RMSE, MAE
- Table of all races with: 21-day EWA, predicted VDOT, actual race VDOT, error
- Predicted vs actual scatter chart
- The Trends tab also shows a daily "Predicted VDOT" line chart

Current calibration: 11 races, RMSE ~2.05, MAE ~1.63.

## Enrichment Pipeline

The `enrich` command runs a waterfall of analysis steps on each activity:

1. **Category inference** — If no `workout_category`, infer from name (e.g., "tempo" → tempo, "8x400" → interval).
2. **Structured vs unstructured** — Workouts with intervals keep their FIT/XLSX laps. Unstructured runs (easy, long, recovery) get pace segments auto-generated from GPS stream data.
3. **Workout tagging** (structured only) — Classifies laps as warmup, work, recovery, or cooldown using VDOT pace zones. Groups work intervals into sets, separated by walking laps, long recoveries (>=2x median), or distance breaks (>=0.3mi).
4. **Track detection** — Determines if the activity was on a 400m track using oval template matching (sliding window of 300 GPS points + OpenCV convex hull matchShapes, score < 0.15).
5. **Measured course detection** — For structured workouts, checks if work-rep GPS centroids are near configured measured courses and snaps intervals to known distances.
6. **Walking scrub** — Flags intervals slower than walking threshold (default 11:00/mi). Skips pace segments and manual intervals.
7. **Hill sprint detection** — Very short intervals (<56m) with uphill elevation gain.
8. **Stride detection** — Flags short intervals (<30s, >24m) as strides. Only applied to manually lapped intervals (FIT/Strava/XLSX), not pace segments.
9. **Pace zone assignment** — Labels each interval's pace zone (E/M/T/I/R/FR) based on current VDOT.
10. **Elapsed pace zone** (pace segments only) — Classifies overall activity pace (total distance / total time) as a complementary effort measure.
11. **Adjusted distance & pace** — Sums non-walking interval distances. Computes running pace excluding walking time.
12. **Grade-adjusted pace** — Minetti 2002 polynomial from altitude stream data.
13. **Computed VDOT** — CVD from GAP + HR (see Race Prediction section).
14. **Elevation gain/loss** — From 3-point smoothed stream altitude as fallback for missing Strava/FIT values.
15. **Weather** — Fetches temperature, humidity, conditions from Open-Meteo API.
16. **Stride/hill counts** — Updates activity-level counts from visible intervals.

### Track Detection Details

A standard 400m lane-1 oval template (two 84.39m straights + two semicircular turns, radius 36.5m) is generated and compared against GPS data:

1. **Sliding window**: Scan GPS stream in windows of 300 points (step 50)
2. **Known track lookup**: Check if centroid is within 200m of a previously detected track
3. **Convex hull matching**: Compare window's convex hull to oval template via `cv2.matchShapes` (score < 0.15)
4. **Dimension checks**: Short axis 50-120m, long axis 120-220m, aspect ratio 1.5-3.0, fill > 0.75

**Distance snapping** uses three tiers: race name → snap to parsed distance; workout name → snap work sets only; generic → snap 180-1300m to nearest 100m.

### Measured Course Detection

Configured in `config.yaml` under `paces.measured_courses`. Each entry has a lat/lon centroid, radius, and snap distance. Only applied to structured workouts. Multiple distances can coexist at one location (e.g., 200m loop, 400m loop, mile loop).

### FIT-Preferred Source Logic

When both FIT and Strava laps exist for an activity, FIT laps take priority everywhere: interval display, stride counting, workout type determination, and stride detection. This prevents Strava auto-laps from distorting derived values.

### Enrichment by Interval Type

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

## Review UI

Launch with `python -m runbase review` at http://localhost:5050.

### Calendar Tab

Year-view calendar grid. Each row is a day. Columns: date, distance, duration, pace, HR, cadence, strides, shoe, name, type zone, intensity score, 7d mileage, 7d intensity, ATL, CTL, AVD, CVD, VDOT, weather.

- **Same-day merging**: Multiple activities on one day display as a single merged grid row (summed distance/duration/intensity, highest-priority zone). The detail modal shows each activity separately.
- **Color coding**: Rows colored by workout type zone. Zone-specific CSS classes.
- **7-day moving averages**: Trailing mileage and intensity shown per row.
- **Click any row** to open a modal detail view with: intervals table, pace/HR charts, GPS map, and edit form.
- **Double-click cells** (name, type zone) for quick inline editing.
- **Future blank rows** accept planned distance/workout entries.
- **Import button** triggers the full pipeline from the UI.

### Detail Modal

Click a row to open a popup showing:

- **Interval table**: All laps/reps with distance, duration, pace, HR, cadence, zone. Double-click cells to edit inline. Stride/recovery checkboxes toggle immediately.
- **Pace chart**: Per-interval pace visualization with zone coloring.
- **HR chart**: Heart rate over intervals.
- **GPS map**: Route visualization (if streams exist).
- **Edit form**: Activity-level fields (distance, duration, name, shoe, notes, VDOT). Save & Close re-enriches the activity.
- **Race entry**: Distance dropdown + time input → computes VDOT and inserts into timeline.
- **Data quality flags**: Suppress HR, suppress cadence, exclude from VDOT.

### Planning Tab

Weekly training plan with:

- **Weekly targets**: Target mileage and intensity score per week
- **Phase annotations**: Training phase name (Base, Build, Peak, Taper) per week
- **Notes**: Free-form notes per week
- **Progress tracking**: Actual vs planned mileage and intensity, color-coded
- **Planned activity parsing**: Understands workout structures like "8x200m", "6x400 @I", "4x(3min I, 2min E)" to estimate intensity scores
- **Undo support**: Cmd/Ctrl+Z for recent edits

### Trends Tab

Charts and tables covering a configurable range (6m, 1y, 2y, all):

- **Weekly mileage** bar chart
- **Weekly average pace** line chart
- **Weekly average HR** line chart
- **ATL/CTL/TSB** line chart (downsampled to ~200 points)
- **Predicted VDOT** daily line chart (21-day AVD model, ~300 points)
- **VDOT timeline** markers (race-derived and manual entries)
- **Race table**: All races with date, name, distance, time, pace, VDOT

### Shoes Tab

- Per-shoe summary: total miles, activity count, date range, average pace
- Monthly mileage breakdown per shoe
- Active vs retired shoe filtering

### Race Prediction Tab

- Model description and calibrated parameters
- Race-by-race table: 21-day EWA, predicted VDOT, actual VDOT, error
- Predicted vs actual scatter chart
- RMSE and MAE metrics

## Data Pipeline & Reconciliation

### How Data Flows

```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  iCloud FIT  │   │  Strava API │   │  XLSX Sheet  │
│  (.fit files)│   │ (activities,│   │ (historical  │
│              │   │  streams,   │   │  workout log)│
│              │   │  laps)      │   │              │
└──────┬───────┘   └──────┬──────┘   └──────┬───────┘
       │                  │                  │
       ▼                  ▼                  ▼
┌──────────────────────────────────────────────────┐
│              activity_sources table               │
│  (one row per import source per activity)         │
└──────────────────────┬───────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────┐
│              Reconciliation                       │
│  1:1 match (date ±1d, distance ±5%)              │
│  Group match (multiple orphans sum to distance)   │
│  Orphan promotion (unmatched → new activity)      │
└──────────────────────┬───────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────┐
│              activities table                     │
│  (one canonical row per real-world run)           │
│  + intervals, streams, shoes                      │
└──────────────────────┬───────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────┐
│              Enrichment Waterfall                 │
│  Track → Course → Tagging → Walking → Strides →  │
│  Zones → GAP → CVD → Weather → Counts            │
└──────────────────────────────────────────────────┘
```

### Source Matching

1. **FIT import**: Creates activity + source. Has GPS streams, HR, cadence, laps, but may have generic names.
2. **Strava sync**: If it matches an existing activity by date (±1 day) and distance (±5%), links as a new source and fills missing fields (name, shoe, HR). Otherwise becomes an orphaned source.
3. **XLSX import**: Creates activities from spreadsheet with hand-entered data. Cutoff date prevents false matches after 2024-12-15.

### Reconciliation Passes

- **1:1 Matching**: Single orphan matches single activity on date + distance.
- **Group Matching**: Multiple same-day orphans whose distances sum to an activity's distance (±10%).
- **Orphan Promotion**: Unmatched Strava sources (post-XLSX-cutoff) become standalone activities.

### Deduplication

The `processed_files` table tracks every imported file by path, hash, and source. No file is ever imported twice. The XLSX importer has no internal dedup — it relies entirely on the processed_files hash check.

### Manual Overrides

Activity edits from the review UI go through `activity_overrides` and sync to the `activities` table, making them canonical. Future imports will not overwrite overridden fields. Interval edits set `source='manual'`.

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
│   ├── strava_sync.py     # Strava API sync with rate limiting
│   └── garmin_sync.py     # Garmin Connect API sync
├── reconcile/
│   ├── matcher.py         # Match activities to orphaned Strava sources
│   └── enricher.py        # Apply shoe/name/category from matched sources
├── analysis/
│   ├── interval_enricher.py # Enrichment waterfall orchestrator
│   ├── vdot.py            # VDOT calculator (Daniels-Gilbert), pace zones, CVD estimation
│   ├── workout_tagger.py  # Structured workout lap classification (warmup/work/recovery/cooldown)
│   ├── track_detect.py    # Oval template matching for track detection
│   ├── pace_segments.py   # Stream-based pace segmentation
│   ├── locations.py       # Workout location clustering, measured course matching
│   └── weather.py         # Open-Meteo API weather fetching
└── review/
    ├── app.py             # Flask routes, API endpoints, override logic, ATL/CTL/AVD computation
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

| Table | Purpose |
|-------|---------|
| **activities** | Canonical activity records (one per real-world run) |
| **activity_sources** | Per-source raw data for audit trail |
| **intervals** | Interval/rep-level splits (FIT laps, Strava laps, XLSX splits, pace segments) |
| **streams** | Per-second time series (lat/lon, HR, cadence, pace, altitude) |
| **shoes** | Shoe tracking (from Strava gear) |
| **processed_files** | Dedup manifest to avoid re-importing |
| **detected_tracks** | Cached track locations for fast lookup |
| **activity_overrides** | Manual field-level overrides from review UI (synced to activities) |
| **planned_activities** | Future planned runs entered via review UI |
| **weekly_plan** | Weekly training plan (phase, notes, target mileage/intensity) |
| **vdot_history** | VDOT values over time (race-derived or manual) |

## Deploying to a Remote Server

### What to transfer

```
~/runbase/                          # data directory
├── data/
│   ├── runbase.db                  # the SQLite database (this is everything)
│   └── raw/                        # archived .fit files (optional, for re-import)
└── state/
    └── strava_tokens.json          # Strava OAuth tokens (needed for pipeline syncs)
```

The database file (`runbase.db`) is the only required file. It contains all activities, intervals, streams, shoes, overrides, and plans.

### Steps

**1. On the remote machine, clone the repo and set up the environment:**

```bash
git clone https://github.com/michaelkurish-insta/runbot.git
cd runbot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**2. Copy the data directory from your local machine:**

```bash
rsync -avz ~/runbase/ user@remote-host:~/runbase/
```

**3. Copy and edit the config:**

```bash
cp config/config.example.yaml config/config.yaml
```

Edit `config/config.yaml`:
- `paths.db` — leave as `~/runbase/data/runbase.db` (or adjust)
- `paths.icloud_healthfit` — remove or leave blank (iCloud sync won't work on a server)
- `strava.*` — set your Strava credentials if you want automated syncs
- `review.host` — change to `0.0.0.0` for remote connections
- `athlete.hr_max` — set your max heart rate for CVD/VDOT estimation

**4. Initialize the database schema (adds any new columns from migrations):**

```bash
python -m runbase db init
```

**5. Start the review UI:**

```bash
python -m runbase review
```

Access from another device at `http://<remote-ip>:5050`.

### Running as a persistent service

Use systemd (Linux):

```ini
# /etc/systemd/system/runbase.service
[Unit]
Description=RunBase Review UI
After=network.target

[Service]
User=your-username
WorkingDirectory=/home/your-username/runbot
ExecStart=/home/your-username/runbot/venv/bin/python -m runbase review
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable runbase
sudo systemctl start runbase
```

Or use `tmux`/`screen`:

```bash
tmux new -s runbase
source venv/bin/activate
python -m runbase review
# Ctrl+B, D to detach; tmux attach -t runbase to reconnect
```

### Automated pipeline syncs

```bash
crontab -e
```

```
# Sync every 2 hours
0 */2 * * * cd /home/your-username/runbot && /home/your-username/runbot/venv/bin/python -m runbase pipeline -v >> ~/runbase/logs/pipeline.log 2>&1
```

### Remote access over the internet

- **Tailscale/WireGuard**: VPN tunnel (recommended — no port exposure)
- **SSH tunnel**: `ssh -L 5050:localhost:5050 user@remote-host`
- **Reverse proxy**: nginx or Caddy with HTTPS and basic auth
