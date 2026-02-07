import sqlite3
from pathlib import Path

from runbase.config import load_config

SCHEMA_SQL = """\
-- Core activity record (one per real-world run)
CREATE TABLE IF NOT EXISTS activities (
    id                  INTEGER PRIMARY KEY,
    date                TEXT NOT NULL,
    start_time          TEXT,
    distance_m          REAL,
    duration_s          REAL,
    avg_pace_s_per_km   REAL,
    avg_hr              REAL,
    max_hr              REAL,
    avg_cadence         REAL,
    total_ascent_m      REAL,
    total_descent_m     REAL,
    calories            REAL,
    workout_type        TEXT,
    workout_name        TEXT,
    intensity_score     REAL,
    notes               TEXT,
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
    metadata_json       TEXT,
    imported_at         TEXT DEFAULT (datetime('now'))
);

-- Interval/rep-level data
CREATE TABLE IF NOT EXISTS intervals (
    id                      INTEGER PRIMARY KEY,
    activity_id             INTEGER REFERENCES activities(id),
    rep_number              INTEGER,
    prescribed_distance_m   REAL,
    actual_distance_m       REAL,
    canonical_distance_m    REAL,
    duration_s              REAL,
    avg_pace_s_per_km       REAL,
    avg_hr                  REAL,
    avg_cadence             REAL,
    is_recovery             BOOLEAN DEFAULT FALSE
);

-- Per-second time series (from .fit files)
CREATE TABLE IF NOT EXISTS streams (
    id                  INTEGER PRIMARY KEY,
    activity_id         INTEGER REFERENCES activities(id),
    timestamp_s         REAL,
    lat                 REAL,
    lon                 REAL,
    altitude_m          REAL,
    heart_rate          INTEGER,
    cadence             INTEGER,
    pace_s_per_km       REAL,
    distance_m          REAL
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

-- Indexes
CREATE INDEX IF NOT EXISTS idx_activities_date ON activities(date);
CREATE INDEX IF NOT EXISTS idx_activity_sources_activity ON activity_sources(activity_id);
CREATE INDEX IF NOT EXISTS idx_activity_sources_source ON activity_sources(source);
CREATE INDEX IF NOT EXISTS idx_intervals_activity ON intervals(activity_id);
CREATE INDEX IF NOT EXISTS idx_streams_activity ON streams(activity_id);
CREATE INDEX IF NOT EXISTS idx_conflicts_status ON conflicts(status);
CREATE INDEX IF NOT EXISTS idx_processed_files_hash ON processed_files(file_hash);
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


def init_db(config=None):
    """Create all tables and indexes."""
    conn = get_connection(config)
    conn.executescript(SCHEMA_SQL)
    conn.close()
    db_path = get_db_path(config)
    print(f"Database initialized at {db_path}")
    return db_path
