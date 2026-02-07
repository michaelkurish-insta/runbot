# RunBase: Definitive Architecture & Build Plan

## v2 — Revised after discussion

### Key Decisions Made
- **Cloud sync:** iCloud Drive (already paid for, native Mac integration)
- **Phone bridge:** HealthFit app ($2, auto-syncs .fit files to iCloud Drive)
- **Historical data:** .xlsx spreadsheet (columns: miles, intensity, shoe, notes with unstructured HR/cadence/pace)
- **Garmin:** One-time historical import only (no longer using Garmin device)
- **Ongoing data flow:** Apple Watch → HealthKit → HealthFit → iCloud Drive → RunBase (fully automatic)
- **Strava:** API pull for metadata, shoe assignments, social data, and as a secondary source
- **Runalyze:** Paid supporter — can use Dropbox export or scrape CSV for computed metrics
- **Tech stack:** Python + SQLite + Flask (local now, lightweight remote server later)

---

## Simplified Data Flow

```
  ┌─────────────────────────────────────────────────────────────┐
  │              ON YOUR PHONE (configure once)                  │
  │                                                              │
  │  WorkOutDoors ──→ HealthKit ──→ HealthFit                   │
  │                                     │                        │
  │                          auto-sync .fit to                   │
  │                                     ↓                        │
  │                              iCloud Drive                    │
  │                         ~/Library/Mobile Documents/          │
  │                           .../HealthFit/                     │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
                                 │  (appears as local folder on Mac)
                                 │
  ┌──────────────────────────────┼──────────────────────────────┐
  │              ON YOUR MAC (cron / launchd)                    │
  │                                                              │
  │              ┌───────────────┴───────────────┐               │
  │              │     ingest.icloud_sync        │  ONGOING      │
  │              │  (watch for new .fit files)    │  (automated)  │
  │              └───────────────┬───────────────┘               │
  │                              │                               │
  │  ┌───────────────┐          │       ┌──────────────────┐    │
  │  │ ingest.xlsx   │          │       │ ingest.strava    │    │
  │  │ (historical   │          │       │ (API: metadata,  │    │
  │  │  spreadsheet) │          │       │  shoes, splits)  │    │
  │  └───────┬───────┘          │       └────────┬─────────┘    │
  │          │                  │                │              │
  │  ┌───────┴──────┐          │       ┌────────┴─────────┐    │
  │  │ingest.garmin │          │       │ ingest.runalyze  │    │
  │  │(one-time     │          │       │ (CSV scrape for  │    │
  │  │ historical)  │          │       │  computed stats)  │    │
  │  └───────┬──────┘          │       └────────┬─────────┘    │
  │          │                 │                │              │
  │          ▼                 ▼                ▼              │
  │  ┌─────────────────────────────────────────────────────┐   │
  │  │              ingest.fit_parser                       │   │
  │  │   (universal .fit/.gpx/.tcx → normalized records)   │   │
  │  └─────────────────────┬───────────────────────────────┘   │
  │                        │                                    │
  │                        ▼                                    │
  │  ┌─────────────────────────────────────────────────────┐   │
  │  │                 reconcile                            │   │
  │  │   match activities across sources (date+distance)   │   │
  │  │   apply source priority per field                   │   │
  │  │   parse workout names ("4x400m")                    │   │
  │  │   log conflicts                                     │   │
  │  └─────────────────────┬───────────────────────────────┘   │
  │                        │                                    │
  │                        ▼                                    │
  │  ┌─────────────────────────────────────────────────────┐   │
  │  │              SQLite Canonical DB                     │   │
  │  └─────────────────────┬───────────────────────────────┘   │
  │                        │                                    │
  │                        ▼                                    │
  │  ┌─────────────────────────────────────────────────────┐   │
  │  │         review (Flask localhost UI)                  │   │
  │  │   resolve conflicts, approve imports, browse data   │   │
  │  └─────────────────────────────────────────────────────┘   │
  └────────────────────────────────────────────────────────────┘
```

---

## Build Plan: Phased Approach

### Phase 0: Project Skeleton & Setup
**Goal:** Repo structure, config, DB schema, and phone-side setup.

```
runbase/
├── runbase/
│   ├── __init__.py
│   ├── config.py              # YAML config loader
│   ├── db.py                  # SQLite connection, migrations
│   ├── models.py              # dataclasses for Activity, Interval, Shoe, etc.
│   ├── ingest/
│   │   ├── __init__.py
│   │   ├── fit_parser.py      # universal .fit/.gpx/.tcx parser
│   │   ├── icloud_sync.py     # watch iCloud Drive folder for new files
│   │   ├── xlsx_import.py     # historical spreadsheet import
│   │   ├── garmin_import.py   # one-time Garmin historical pull
│   │   ├── strava_sync.py     # Strava API incremental sync
│   │   └── runalyze_sync.py   # Runalyze CSV scrape
│   ├── reconcile/
│   │   ├── __init__.py
│   │   ├── matcher.py         # fuzzy activity matching across sources
│   │   ├── resolver.py        # source priority field resolution
│   │   ├── workout_parser.py  # "4x400m" → structured intervals
│   │   └── conflicts.py       # conflict logging and status
│   ├── review/
│   │   ├── __init__.py
│   │   ├── app.py             # Flask app
│   │   ├── templates/         # Jinja2 templates
│   │   └── static/            # minimal CSS/JS
│   └── cli.py                 # command-line entry points
├── config/
│   ├── config.yaml            # main config (paths, API creds ref)
│   └── config.example.yaml    # template for fresh setup
├── migrations/                # SQL migration files
├── scripts/
│   ├── setup_strava_auth.py   # one-time OAuth helper
│   └── setup.sh               # install deps, init DB
├── tests/
├── requirements.txt
└── README.md
```

**Setup checklist (documented in README):**
1. Install HealthFit on iPhone, configure auto-export to iCloud Drive as .fit
2. Verify .fit files appear in `~/Library/Mobile Documents/.../HealthFit/` on Mac
3. Create Strava API app, run `setup_strava_auth.py` to get refresh token
4. (One-time) Request Garmin GDPR data export, download zip
5. (One-time) Log into Runalyze, download CSV export (or set up Dropbox connection)
6. Copy `config.example.yaml` → `config.yaml`, fill in paths and tokens
7. Run `python -m runbase.db init` to create SQLite database

**Config structure:**
```yaml
# config.yaml
paths:
  db: "~/runbase/data/runbase.db"
  raw_store: "~/runbase/data/raw/"          # archived original files
  icloud_healthfit: "~/Library/Mobile Documents/iCloud~com~altifondo~HealthFit/Documents/"
  garmin_export_zip: "~/Downloads/garmin_export.zip"   # one-time
  xlsx_import: "~/runbase/import/training_log.xlsx"

strava:
  client_id: "${STRAVA_CLIENT_ID}"
  client_secret: "${STRAVA_CLIENT_SECRET}"
  token_file: "~/runbase/state/strava_tokens.json"

garmin:
  email: "${GARMIN_EMAIL}"
  password: "${GARMIN_PASSWORD}"

runalyze:
  username: "${RUNALYZE_USERNAME}"
  password: "${RUNALYZE_PASSWORD}"
  method: "csv_scrape"                      # or "dropbox" if configured

llm:
  provider: "ollama"                        # or "anthropic"
  model: "phi3:mini"                        # lightweight for parsing
  endpoint: "http://localhost:11434"         # ollama default

reconcile:
  distance_tolerance_pct: 5                 # % difference before flagging
  time_match_window_minutes: 5              # for cross-source matching
  source_priority:                          # highest to lowest
    - "manual_override"
    - "master_xlsx"
    - "workout_name_parse"
    - "fit_file"
    - "strava"
    - "garmin"
    - "runalyze"

review:
  host: "127.0.0.1"
  port: 5050
```

---

### Phase 1: Foundation — Fit Parser + iCloud Sync
**Goal:** Get new workouts flowing in automatically.

**Build order:**
1. `db.py` — SQLite schema creation with all tables (see schema below)
2. `models.py` — Python dataclasses mirroring the DB
3. `fit_parser.py` — parse .fit → `RawActivity` dataclass. Use `fitparse` library. Also handle .gpx (`gpxpy`) and .tcx (`lxml`). Extract: summary stats, laps, per-second streams, device info, workout name.
4. `icloud_sync.py` — scan the iCloud Drive HealthFit folder, compare against a processed-files manifest, copy new .fit files to raw store, run them through `fit_parser`, insert into DB as `source='healthfit'`
5. Wire up CLI: `python -m runbase sync --icloud`

**After Phase 1 you can:** Run a cron job that auto-imports every new workout from your watch. You have per-second data in SQLite.

---

### Phase 2: Historical Import — XLSX + Garmin
**Goal:** Backfill all historical data.

**Build order:**
1. `xlsx_import.py`:
   - Read with `openpyxl`
   - Map columns: miles, intensity score, shoe, notes
   - For the notes column ("pace; HR; cadence; felt good, legs heavy"):
     - First pass: regex for common patterns (`\d+:\d+;\s*\d+;\s*\d+` = pace; HR; cadence)
     - Fallback: batch LLM calls for ambiguous rows. Send 20-30 rows at a time with a strict JSON schema prompt.
     - Output: structured fields + a `parse_confidence` score
   - Generate a review HTML file showing original vs. parsed, highlighting low-confidence rows
   - Insert into DB with `source='master_xlsx'`
2. `garmin_import.py`:
   - Extract Garmin GDPR zip (one-time)
   - Find all .fit files in the archive
   - Run each through `fit_parser.py`
   - Insert with `source='garmin'`
   - Alternative: use `garminconnect` package to pull activity list + download .fit files. Better metadata but uses the unofficial API.
3. Wire up CLI: `python -m runbase import --xlsx`, `python -m runbase import --garmin`

**After Phase 2 you can:** Query all your historical runs alongside new ones. The xlsx data is master source.

---

### Phase 3: Strava + Runalyze Enrichment
**Goal:** Pull supplementary data from APIs.

**Build order:**
1. `strava_sync.py`:
   - One-time auth setup script (opens browser, captures OAuth code, exchanges for tokens)
   - Incremental sync: pull activities newer than last sync timestamp
   - Per activity: summary, splits, laps, gear assignment
   - Optionally: streams (per-second data — useful as secondary source, slower to pull)
   - Store shoe data in `shoes` table, activity-shoe mapping
   - Insert with `source='strava'`
2. `runalyze_sync.py`:
   - Authenticate via POST to login endpoint (get CSRF token first)
   - Download CSV from `/_internal/data/activities/all`
   - Parse CSV into records
   - Map to existing activities by date + distance
   - Insert computed metrics (TRIMP, VDOT, etc.) into `runalyze_metrics` table
3. Wire up CLI: `python -m runbase sync --strava`, `python -m runbase sync --runalyze`

**After Phase 3 you can:** See Strava shoe assignments and Runalyze training metrics alongside your data. Every run may now have 2-4 source records.

---

### Phase 4: Reconciliation Engine
**Goal:** Merge multi-source records into canonical activities, detect and log conflicts.

**Build order:**
1. `matcher.py`:
   - Group all `activity_sources` records by (date, approximate start time)
   - Within each group, confirm match via distance similarity (within configured tolerance)
   - Create/update canonical `activities` record for each matched group
   - Handle unmatched records (new activity from single source = auto-create canonical)
2. `workout_parser.py`:
   - Regex patterns for common formats: `NxDISTANCE`, `tempo N`, `Nk race`, `easy N`, `long N`
   - Extract: rep count, prescribed distance, workout type, recovery info
   - When parsed workout has reps: match against .fit file laps, create `intervals` records with both prescribed and actual distances
   - LLM fallback for unusual names
3. `resolver.py`:
   - For each field in a matched group, pick the value from the highest-priority source that has it
   - Distance comes from xlsx if present, else .fit file, else Strava...
   - Special case: when `workout_parser` provides prescribed distances, those override GPS for interval reps
4. `conflicts.py`:
   - When sources disagree beyond tolerance thresholds, create a `conflicts` record
   - Auto-resolve when priority is clear and values are close
   - Flag for manual review when values diverge significantly
5. Wire up CLI: `python -m runbase reconcile`

**After Phase 4 you can:** Have one clean, canonical record per run with full provenance. Conflicts are logged for review.

---

### Phase 5: Review UI
**Goal:** Minimal Flask app for conflict resolution and data browsing.

**Build order:**
1. `app.py` — Flask routes:
   - `GET /` — dashboard: total activities, pending conflicts, last sync time
   - `GET /conflicts` — list of unresolved conflicts with filtering
   - `GET /conflicts/<id>` — detail view showing all source values, pick resolution
   - `POST /conflicts/<id>/resolve` — save resolution
   - `GET /activities` — paginated activity browser with search/filter
   - `GET /activities/<id>` — detail view with all source data, intervals, notes
2. Templates — minimal, functional Jinja2 templates. No framework, just clean HTML + a small CSS file.
3. Conflict resolution actions:
   - Pick a source value
   - Enter a custom value
   - Use average of sources
   - Approve all auto-resolved conflicts in bulk

**After Phase 5 you can:** Open `localhost:5050`, review flagged discrepancies, browse all your running data.

---

### Phase 6: Scheduler + Polish
**Goal:** Fully automated pipeline.

1. `launchd` plist (macOS) or cron job:
   - Every 30 minutes: `sync --icloud` (new workouts)
   - Every 6 hours: `sync --strava` (metadata enrichment)
   - Weekly: `sync --runalyze` (training metrics refresh)
   - After each sync: `reconcile`
2. Logging: structured logs to `~/runbase/logs/`
3. Notifications: optional macOS notification on new activity imported or conflict flagged
4. Health check: CLI command `python -m runbase status` showing last sync times, pending conflicts, DB stats

---

## Canonical SQLite Schema

```sql
-- Core activity record (one per real-world run)
CREATE TABLE activities (
    id                  INTEGER PRIMARY KEY,
    date                TEXT NOT NULL,               -- YYYY-MM-DD
    start_time          TEXT,                         -- ISO 8601 datetime
    distance_m          REAL,                         -- meters (canonical)
    duration_s          REAL,                         -- seconds
    avg_pace_s_per_km   REAL,                         -- derived: duration / (distance/1000)
    avg_hr              REAL,
    max_hr              REAL,
    avg_cadence         REAL,
    total_ascent_m      REAL,
    total_descent_m     REAL,
    calories            REAL,
    workout_type        TEXT,                         -- easy, tempo, interval, long, race, recovery, etc.
    workout_name        TEXT,                         -- original name verbatim
    intensity_score     REAL,                         -- from xlsx if available
    notes               TEXT,                         -- merged/combined notes
    shoe_id             INTEGER REFERENCES shoes(id),
    rpe                 INTEGER,                      -- 1-10 subjective
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now'))
);

-- Per-source raw data (audit trail, re-resolution)
CREATE TABLE activity_sources (
    id                  INTEGER PRIMARY KEY,
    activity_id         INTEGER REFERENCES activities(id),
    source              TEXT NOT NULL,                 -- 'master_xlsx', 'fit_file', 'strava', 'garmin', 'runalyze'
    source_id           TEXT,                          -- external ID (strava activity ID, garmin ID, etc.)
    raw_file_path       TEXT,                          -- path to archived .fit/.gpx
    distance_m          REAL,
    duration_s          REAL,
    avg_pace_s_per_km   REAL,
    avg_hr              REAL,
    max_hr              REAL,
    avg_cadence         REAL,
    total_ascent_m      REAL,
    calories            REAL,
    workout_name        TEXT,
    notes               TEXT,
    metadata_json       TEXT,                          -- full source-specific dump
    imported_at         TEXT DEFAULT (datetime('now'))
);

-- Interval/rep-level data
CREATE TABLE intervals (
    id                      INTEGER PRIMARY KEY,
    activity_id             INTEGER REFERENCES activities(id),
    rep_number              INTEGER,
    prescribed_distance_m   REAL,                     -- from workout name ("400m")
    actual_distance_m       REAL,                     -- from GPS
    canonical_distance_m    REAL,                     -- resolved (usually prescribed)
    duration_s              REAL,
    avg_pace_s_per_km       REAL,                     -- recalculated with canonical distance
    avg_hr                  REAL,
    avg_cadence             REAL,
    is_recovery             BOOLEAN DEFAULT FALSE
);

-- Per-second time series (from .fit files)
CREATE TABLE streams (
    id                  INTEGER PRIMARY KEY,
    activity_id         INTEGER REFERENCES activities(id),
    timestamp_s         REAL,                         -- seconds from start
    lat                 REAL,
    lon                 REAL,
    altitude_m          REAL,
    heart_rate          INTEGER,
    cadence             INTEGER,
    pace_s_per_km       REAL,
    distance_m          REAL                          -- cumulative
);

-- Shoe tracking
CREATE TABLE shoes (
    id                  INTEGER PRIMARY KEY,
    name                TEXT NOT NULL,
    brand               TEXT,
    model               TEXT,
    strava_gear_id      TEXT,                         -- for mapping from Strava
    purchase_date       TEXT,
    retired             BOOLEAN DEFAULT FALSE,
    notes               TEXT
);

-- Conflicts needing review
CREATE TABLE conflicts (
    id                  INTEGER PRIMARY KEY,
    activity_id         INTEGER REFERENCES activities(id),
    field_name          TEXT NOT NULL,
    source_values_json  TEXT NOT NULL,                 -- {"fit_file": 5023, "strava": 5010}
    resolved_value      TEXT,
    resolved_source     TEXT,
    status              TEXT DEFAULT 'pending',        -- pending, auto_resolved, manual_resolved
    created_at          TEXT DEFAULT (datetime('now')),
    resolved_at         TEXT
);

-- Runalyze enrichment (computed training metrics)
CREATE TABLE runalyze_metrics (
    id                  INTEGER PRIMARY KEY,
    activity_id         INTEGER REFERENCES activities(id),
    trimp               REAL,
    vdot                REAL,
    effective_vo2max    REAL,
    training_effect     REAL,
    hr_reserve_pct      REAL,
    fitness             REAL,                         -- CTL equivalent
    fatigue             REAL,                         -- ATL equivalent
    form                REAL,                         -- TSB equivalent
    raw_csv_json        TEXT
);

-- Sync state tracking
CREATE TABLE sync_state (
    id                  INTEGER PRIMARY KEY,
    source              TEXT NOT NULL UNIQUE,          -- 'icloud', 'strava', 'runalyze', 'garmin'
    last_sync_at        TEXT,
    last_activity_date  TEXT,
    metadata_json       TEXT                          -- source-specific state (e.g., strava page cursor)
);

-- Processed file manifest (avoid re-importing)
CREATE TABLE processed_files (
    id                  INTEGER PRIMARY KEY,
    file_path           TEXT NOT NULL UNIQUE,
    file_hash           TEXT,                         -- SHA256 for dedup
    source              TEXT,
    processed_at        TEXT DEFAULT (datetime('now')),
    activity_id         INTEGER REFERENCES activities(id)
);

-- Indexes
CREATE INDEX idx_activities_date ON activities(date);
CREATE INDEX idx_activity_sources_activity ON activity_sources(activity_id);
CREATE INDEX idx_activity_sources_source ON activity_sources(source);
CREATE INDEX idx_intervals_activity ON intervals(activity_id);
CREATE INDEX idx_streams_activity ON streams(activity_id);
CREATE INDEX idx_conflicts_status ON conflicts(status);
CREATE INDEX idx_processed_files_hash ON processed_files(file_hash);
```

---

## Source Priority Table (field-level)

| Field | Priority Order (highest → lowest) |
|---|---|
| distance | manual_override > master_xlsx > workout_name_parse > fit_file > strava |
| duration | fit_file > strava > master_xlsx |
| avg_hr, max_hr, cadence | fit_file > strava > master_xlsx (parsed from notes) |
| per-second streams | fit_file only |
| workout_type | master_xlsx > workout_name_parse > strava |
| shoe | strava > master_xlsx |
| notes | merged from all sources (not replaced) |
| intensity_score | master_xlsx only |
| TRIMP, VDOT, etc. | runalyze only |
| interval prescribed dist | workout_name_parse only |
| interval actual dist | fit_file > strava |

---

## Setup Runbook (one-time manual steps)

### Phone Setup
1. Install HealthFit from App Store (~$2)
2. Grant HealthFit read access to all Health categories
3. In HealthFit settings → Auto Export → enable iCloud Drive
4. Set export format to .FIT (preferred) or .GPX
5. Verify: do a test workout, confirm .fit file appears in Files app under iCloud Drive > HealthFit

### Mac Setup
```bash
# 1. Clone repo and install
git clone <repo> ~/runbase
cd ~/runbase
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Verify iCloud Drive path
ls ~/Library/Mobile\ Documents/iCloud~com~altifondo~HealthFit/Documents/
# Should show .fit files from HealthFit

# 3. Set up config
cp config/config.example.yaml config/config.yaml
# Edit config.yaml with your paths

# 4. Set up Strava OAuth (one-time, opens browser)
python scripts/setup_strava_auth.py

# 5. Initialize database
python -m runbase db init

# 6. (One-time) Import historical data
python -m runbase import --xlsx ~/path/to/training_log.xlsx --review
python -m runbase import --garmin ~/path/to/garmin_export.zip

# 7. (One-time) Pull Runalyze CSV
python -m runbase sync --runalyze

# 8. First reconciliation
python -m runbase reconcile

# 9. Launch review UI
python -m runbase review
# Opens localhost:5050

# 10. Set up scheduled sync
# Add to crontab or create launchd plist:
# Every 30 min: python -m runbase sync --icloud && python -m runbase reconcile
# Every 6 hours: python -m runbase sync --strava
# Weekly: python -m runbase sync --runalyze
```

### Runalyze Setup
1. Log into runalyze.com
2. Go to Settings → Personal API → create a token with activity read scope
3. Alternatively: just note your login credentials for the CSV scrape approach
4. Add credentials to config.yaml (or env vars)

---

## Future-Proofing Notes

**Remote server migration path:**
- SQLite works fine single-user; if you ever need multi-device access, swap to PostgreSQL (schema is compatible)
- Flask app is already a standard WSGI app — deploy behind nginx/gunicorn on any VPS
- iCloud Drive sync would need to be replaced with a Dropbox/GDrive sync (rclone) on the server
- Config uses env vars for secrets — 12-factor ready
- The sync modules are all idempotent — safe to run from anywhere

**Analysis module (Phase 7+, not in this build):**
- Weekly/monthly mileage rollups
- Training load (acute:chronic workload ratio)
- Pace/HR trends over time
- Shoe mileage tracking
- Race equivalency / VDOT tracking
- Export to CSV/JSON for custom visualization
