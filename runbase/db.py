import sqlite3
from pathlib import Path

from runbase.config import load_config

SCHEMA_SQL = """\
-- Core activity record (one per real-world run)
CREATE TABLE IF NOT EXISTS activities (
    id                  INTEGER PRIMARY KEY,
    date                TEXT NOT NULL,
    start_time          TEXT,
    distance_mi         REAL,
    duration_s          REAL,
    avg_pace_s_per_mi   REAL,
    avg_pace_display    TEXT,
    avg_hr              REAL,
    max_hr              REAL,
    avg_cadence         REAL,
    total_ascent_ft     REAL,
    total_descent_ft    REAL,
    calories            REAL,
    workout_type        TEXT,
    workout_name        TEXT,
    strides             INTEGER,
    workout_category    TEXT,
    fit_file_path       TEXT,
    intensity_score     REAL,
    notes               TEXT,
    adjusted_distance_mi REAL,
    vdot                REAL,
    shoe_id             INTEGER REFERENCES shoes(id),
    rpe                 INTEGER,
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now'))
);

-- Per-source raw data (audit trail, re-resolution)
CREATE TABLE IF NOT EXISTS activity_sources (
    id                  INTEGER PRIMARY KEY,
    activity_id         INTEGER REFERENCES activities(id),
    source              TEXT NOT NULL,
    source_id           TEXT,
    raw_file_path       TEXT,
    distance_mi         REAL,
    duration_s          REAL,
    avg_pace_s_per_mi   REAL,
    avg_hr              REAL,
    max_hr              REAL,
    avg_cadence         REAL,
    total_ascent_ft     REAL,
    calories            REAL,
    workout_name        TEXT,
    notes               TEXT,
    metadata_json       TEXT,
    imported_at         TEXT DEFAULT (datetime('now'))
);

-- Interval/rep-level data
CREATE TABLE IF NOT EXISTS intervals (
    id                      INTEGER PRIMARY KEY,
    activity_id             INTEGER REFERENCES activities(id),
    rep_number              INTEGER,
    prescribed_distance_mi  REAL,
    gps_measured_distance_mi REAL,
    canonical_distance_mi   REAL,
    duration_s              REAL,
    avg_pace_s_per_mi       REAL,
    avg_pace_display        TEXT,
    avg_hr                  REAL,
    avg_cadence             REAL,
    is_recovery             BOOLEAN DEFAULT FALSE,
    pace_zone               TEXT,
    is_walking              BOOLEAN DEFAULT FALSE,
    is_stride               BOOLEAN DEFAULT FALSE,
    elapsed_pace_zone       TEXT,
    is_race                 BOOLEAN DEFAULT FALSE,
    location_type           TEXT,
    set_number              INTEGER,
    start_timestamp_s       REAL,
    end_timestamp_s         REAL,
    source                  TEXT
);

-- Per-second time series (from .fit files)
CREATE TABLE IF NOT EXISTS streams (
    id                  INTEGER PRIMARY KEY,
    activity_id         INTEGER REFERENCES activities(id),
    timestamp_s         REAL,
    lat                 REAL,
    lon                 REAL,
    altitude_ft         REAL,
    heart_rate          INTEGER,
    cadence             INTEGER,
    pace_s_per_mi       REAL,
    distance_mi         REAL,
    source_id           INTEGER REFERENCES activity_sources(id)
);

-- Shoe tracking
CREATE TABLE IF NOT EXISTS shoes (
    id                  INTEGER PRIMARY KEY,
    name                TEXT NOT NULL,
    brand               TEXT,
    model               TEXT,
    strava_gear_id      TEXT,
    purchase_date       TEXT,
    retired             BOOLEAN DEFAULT FALSE,
    notes               TEXT
);

-- Conflicts needing review
CREATE TABLE IF NOT EXISTS conflicts (
    id                  INTEGER PRIMARY KEY,
    activity_id         INTEGER REFERENCES activities(id),
    field_name          TEXT NOT NULL,
    source_values_json  TEXT NOT NULL,
    resolved_value      TEXT,
    resolved_source     TEXT,
    status              TEXT DEFAULT 'pending',
    created_at          TEXT DEFAULT (datetime('now')),
    resolved_at         TEXT
);

-- Runalyze enrichment (computed training metrics)
CREATE TABLE IF NOT EXISTS runalyze_metrics (
    id                  INTEGER PRIMARY KEY,
    activity_id         INTEGER REFERENCES activities(id),
    trimp               REAL,
    vdot                REAL,
    effective_vo2max    REAL,
    training_effect     REAL,
    hr_reserve_pct      REAL,
    fitness             REAL,
    fatigue             REAL,
    form                REAL,
    raw_csv_json        TEXT
);

-- Sync state tracking
CREATE TABLE IF NOT EXISTS sync_state (
    id                  INTEGER PRIMARY KEY,
    source              TEXT NOT NULL UNIQUE,
    last_sync_at        TEXT,
    last_activity_date  TEXT,
    metadata_json       TEXT
);

-- Processed file manifest (avoid re-importing)
CREATE TABLE IF NOT EXISTS processed_files (
    id                  INTEGER PRIMARY KEY,
    file_path           TEXT NOT NULL UNIQUE,
    file_hash           TEXT,
    source              TEXT,
    processed_at        TEXT DEFAULT (datetime('now')),
    activity_id         INTEGER REFERENCES activities(id)
);

-- VDOT history
CREATE TABLE IF NOT EXISTS vdot_history (
    id              INTEGER PRIMARY KEY,
    effective_date  TEXT NOT NULL,
    vdot            REAL NOT NULL,
    source          TEXT,
    activity_id     INTEGER REFERENCES activities(id),
    notes           TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

-- Detected track locations (cached for fast lookup)
CREATE TABLE IF NOT EXISTS detected_tracks (
    id                      INTEGER PRIMARY KEY,
    lat                     REAL NOT NULL,
    lon                     REAL NOT NULL,
    orientation_deg         REAL,
    fit_score               REAL,
    detected_by_activity_id INTEGER REFERENCES activities(id),
    created_at              TEXT DEFAULT (datetime('now'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_activities_date ON activities(date);
CREATE INDEX IF NOT EXISTS idx_activity_sources_activity ON activity_sources(activity_id);
CREATE INDEX IF NOT EXISTS idx_activity_sources_source ON activity_sources(source);
CREATE INDEX IF NOT EXISTS idx_intervals_activity ON intervals(activity_id);
CREATE INDEX IF NOT EXISTS idx_streams_activity ON streams(activity_id);
CREATE INDEX IF NOT EXISTS idx_conflicts_status ON conflicts(status);
CREATE INDEX IF NOT EXISTS idx_processed_files_hash ON processed_files(file_hash);
CREATE INDEX IF NOT EXISTS idx_vdot_history_date ON vdot_history(effective_date);
"""

DEFAULT_DB_PATH = Path.home() / "runbase" / "data" / "runbase.db"


def get_db_path(config=None):
    """Resolve the database path from config or fall back to default."""
    if config and "paths" in config and "db" in config["paths"]:
        return Path(config["paths"]["db"])
    return DEFAULT_DB_PATH


def get_connection(config=None):
    """Return a sqlite3 connection using the configured db path."""
    db_path = get_db_path(config)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _migrate_schema(conn):
    """Add columns that may be missing from existing databases."""
    migrations = [
        # Phase 5: interval enrichment columns
        ("intervals", "pace_zone", "TEXT"),
        ("intervals", "is_walking", "BOOLEAN DEFAULT FALSE"),
        ("intervals", "is_stride", "BOOLEAN DEFAULT FALSE"),
        ("intervals", "location_type", "TEXT"),
        ("intervals", "start_timestamp_s", "REAL"),
        ("intervals", "end_timestamp_s", "REAL"),
        ("intervals", "source", "TEXT"),
        ("intervals", "is_race", "BOOLEAN DEFAULT FALSE"),
        # Phase 5+: workout tagging
        ("intervals", "set_number", "INTEGER"),
        # Phase 5+: elapsed pace zone for pace segments
        ("intervals", "elapsed_pace_zone", "TEXT"),
        # Phase 5: activity enrichment columns
        ("activities", "adjusted_distance_mi", "REAL"),
        ("activities", "vdot", "REAL"),
        # Stream source tracking
        ("streams", "source_id", "INTEGER REFERENCES activity_sources(id)"),
    ]

    existing = {}
    for table, col, col_type in migrations:
        if table not in existing:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            existing[table] = {r[1] for r in rows}
        if col not in existing[table]:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")

    # Column renames
    renames = [
        ("intervals", "actual_distance_mi", "gps_measured_distance_mi"),
    ]
    for table, old_col, new_col in renames:
        if table not in existing:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            existing[table] = {r[1] for r in rows}
        if old_col in existing[table] and new_col not in existing[table]:
            conn.execute(f"ALTER TABLE {table} RENAME COLUMN {old_col} TO {new_col}")

    # New tables for existing databases
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS detected_tracks (
            id                      INTEGER PRIMARY KEY,
            lat                     REAL NOT NULL,
            lon                     REAL NOT NULL,
            orientation_deg         REAL,
            fit_score               REAL,
            detected_by_activity_id INTEGER REFERENCES activities(id),
            created_at              TEXT DEFAULT (datetime('now'))
        );
    """)

    conn.commit()


def init_db(config=None):
    """Create all tables and indexes."""
    conn = get_connection(config)
    conn.executescript(SCHEMA_SQL)
    _migrate_schema(conn)
    conn.close()
    db_path = get_db_path(config)
    print(f"Database initialized at {db_path}")
    return db_path
