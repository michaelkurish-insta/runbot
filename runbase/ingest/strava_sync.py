"""Strava API sync — fetch activities and enrich existing DB records.

XLSX is always treated as truth. Strava data only fills NULL fields on
canonical activities. Unmatched Strava activities are stored as activity_sources
with activity_id=NULL for later review.
"""

import json
import time as time_mod
from datetime import datetime, timedelta, timezone
from pathlib import Path

from stravalib import Client

from runbase.db import get_connection
from runbase.ingest.fit_parser import format_pace

METERS_PER_MILE = 1609.344
METERS_TO_FEET = 3.28084

# Strava activity types we care about
RUNNING_TYPES = {"Run", "TrailRun", "VirtualRun"}


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

class StravaRateLimiter:
    """Track Strava API usage from response headers and pause when near limits.

    Strava limits:
      - 100 requests per 15 minutes (short-term)
      - 1000 requests per day (daily)
    """

    SHORT_LIMIT = 100
    DAILY_LIMIT = 1000
    SHORT_THRESHOLD = 0.95   # pause at 95 of 100
    DAILY_THRESHOLD = 0.95   # abort at 950 of 1000

    def __init__(self):
        self.short_usage = 0
        self.daily_usage = 0
        self.pause_count = 0
        self.aborted = False

    def update_from_response(self, response):
        """Extract rate limit usage from Strava response headers."""
        usage = response.headers.get("X-ReadRateLimit-Usage", "")
        if usage:
            parts = usage.split(",")
            if len(parts) >= 2:
                self.short_usage = int(parts[0].strip())
                self.daily_usage = int(parts[1].strip())

    def check(self, verbose=False):
        """Check limits and sleep/abort if needed. Returns False if daily limit hit."""
        if self.daily_usage >= int(self.DAILY_LIMIT * self.DAILY_THRESHOLD):
            self.aborted = True
            if verbose:
                print(f"\n  RATE LIMIT: Daily usage {self.daily_usage}/{self.DAILY_LIMIT}. "
                      f"Re-run after midnight UTC.")
            return False

        if self.short_usage >= int(self.SHORT_LIMIT * self.SHORT_THRESHOLD):
            # Sleep until next 15-minute boundary
            now = datetime.now(timezone.utc)
            minute = now.minute
            next_boundary = ((minute // 15) + 1) * 15
            if next_boundary >= 60:
                wait_minutes = 60 - minute
            else:
                wait_minutes = next_boundary - minute
            wait_seconds = wait_minutes * 60 - now.second + 5  # 5s buffer
            if wait_seconds > 0:
                self.pause_count += 1
                if verbose:
                    print(f"\n  RATE LIMIT: {self.short_usage}/{self.SHORT_LIMIT} "
                          f"(15-min). Sleeping {wait_seconds}s...")
                time_mod.sleep(wait_seconds)
                self.short_usage = 0  # Reset after waiting

        return True


# ---------------------------------------------------------------------------
# Token management
# ---------------------------------------------------------------------------

def _load_tokens(config: dict) -> dict:
    """Load tokens from disk."""
    token_path = Path(config["strava"]["token_file"]).expanduser()
    if not token_path.exists():
        raise FileNotFoundError(
            f"Strava tokens not found at {token_path}. "
            f"Run: python scripts/setup_strava_auth.py"
        )
    with open(token_path) as f:
        return json.load(f)


def _save_tokens(config: dict, tokens: dict):
    """Save tokens to disk."""
    token_path = Path(config["strava"]["token_file"]).expanduser()
    token_path.parent.mkdir(parents=True, exist_ok=True)
    with open(token_path, "w") as f:
        json.dump(tokens, f, indent=2)


def _get_client(config: dict) -> Client:
    """Create an authenticated stravalib Client, refreshing tokens if needed."""
    tokens = _load_tokens(config)

    # Refresh if expired or expiring within 60s
    if tokens["expires_at"] < time_mod.time() + 60:
        client_id = config["strava"]["client_id"]
        client_secret = config["strava"]["client_secret"]

        client = Client()
        new_tokens = client.refresh_access_token(
            client_id=int(client_id),
            client_secret=client_secret,
            refresh_token=tokens["refresh_token"],
        )
        tokens = {
            "access_token": new_tokens["access_token"],
            "refresh_token": new_tokens["refresh_token"],
            "expires_at": new_tokens["expires_at"],
        }
        _save_tokens(config, tokens)

    client = Client(access_token=tokens["access_token"])
    return client


# ---------------------------------------------------------------------------
# DB lookup helpers
# ---------------------------------------------------------------------------

def _build_activity_lookup(conn) -> dict:
    """Load all activities into a dict keyed by date -> list of activity rows.

    Each row is a dict with id, distance_mi, duration_s, start_time, and
    fields needed for NULL-check merging.
    """
    rows = conn.execute(
        """SELECT id, date, distance_mi, duration_s, start_time,
                  avg_hr, max_hr, avg_cadence, total_ascent_ft, total_descent_ft,
                  calories, shoe_id
           FROM activities"""
    ).fetchall()

    lookup = {}
    for r in rows:
        date = r[0 + 1]  # date is column index 1
        entry = {
            "id": r[0],
            "date": r[1],
            "distance_mi": r[2],
            "duration_s": r[3],
            "start_time": r[4],
            "avg_hr": r[5],
            "max_hr": r[6],
            "avg_cadence": r[7],
            "total_ascent_ft": r[8],
            "total_descent_ft": r[9],
            "calories": r[10],
            "shoe_id": r[11],
        }
        lookup.setdefault(date, []).append(entry)

    return lookup


def _load_processed_strava_ids(conn) -> set:
    """Load already-processed Strava IDs from processed_files table."""
    rows = conn.execute(
        "SELECT file_path FROM processed_files WHERE source = 'strava'"
    ).fetchall()
    ids = set()
    for r in rows:
        # file_path format: "strava:{strava_id}"
        path = r[0]
        if path.startswith("strava:"):
            ids.add(path[7:])
    return ids


def _get_last_sync_timestamp(conn) -> datetime | None:
    """Get the last synced Strava activity timestamp from sync_state."""
    row = conn.execute(
        "SELECT metadata_json FROM sync_state WHERE source = 'strava'"
    ).fetchone()
    if row and row[0]:
        meta = json.loads(row[0])
        ts = meta.get("last_activity_timestamp")
        if ts:
            return datetime.fromisoformat(ts)
    return None


def _activity_has_intervals(conn, activity_id: int) -> bool:
    """Check if an activity already has intervals (from XLSX splits)."""
    row = conn.execute(
        "SELECT COUNT(*) FROM intervals WHERE activity_id = ?", (activity_id,)
    ).fetchone()
    return row[0] > 0


def _activity_has_streams(conn, activity_id: int) -> bool:
    """Check if an activity already has stream data."""
    row = conn.execute(
        "SELECT COUNT(*) FROM streams WHERE activity_id = ?", (activity_id,)
    ).fetchone()
    return row[0] > 0


# ---------------------------------------------------------------------------
# Activity matching
# ---------------------------------------------------------------------------

def _match_strava_activity(strava_act, lookup: dict, tolerance_pct: float) -> dict | None:
    """Match a Strava activity to a DB activity by date + distance.

    Returns the matched DB activity dict or None.
    """
    strava_date = strava_act.start_date_local.strftime("%Y-%m-%d")
    strava_dist_mi = float(strava_act.distance) / METERS_PER_MILE if strava_act.distance else 0

    # Check date and ±1 day as timezone fallback
    candidate_dates = [strava_date]
    dt = strava_act.start_date_local
    candidate_dates.append((dt - timedelta(days=1)).strftime("%Y-%m-%d"))
    candidate_dates.append((dt + timedelta(days=1)).strftime("%Y-%m-%d"))

    candidates = []
    for d in candidate_dates:
        candidates.extend(lookup.get(d, []))

    if not candidates:
        return None

    # Score candidates by distance similarity
    best_match = None
    best_diff_pct = float("inf")

    for cand in candidates:
        db_dist = cand["distance_mi"]
        if db_dist is None or db_dist <= 0:
            # No distance on DB side — match by date only (lower confidence)
            if cand["date"] == strava_date and best_match is None:
                best_match = cand
                best_diff_pct = 100  # low-confidence sentinel
            continue

        diff_pct = abs(strava_dist_mi - db_dist) / db_dist * 100
        if diff_pct <= tolerance_pct and diff_pct < best_diff_pct:
            best_match = cand
            best_diff_pct = diff_pct

    return best_match


# ---------------------------------------------------------------------------
# Data extraction and conversion
# ---------------------------------------------------------------------------

def _extract_strava_data(strava_act) -> dict:
    """Extract and convert fields from a stravalib activity object."""
    distance_mi = float(strava_act.distance) / METERS_PER_MILE if strava_act.distance else None
    duration_s = float(int(strava_act.moving_time)) if strava_act.moving_time else None
    elapsed_s = float(int(strava_act.elapsed_time)) if strava_act.elapsed_time else None

    avg_pace = None
    avg_pace_display = None
    if distance_mi and distance_mi > 0 and duration_s and duration_s > 0:
        avg_pace = round(duration_s / distance_mi, 1)
        avg_pace_display = format_pace(avg_pace)

    start_time = None
    if strava_act.start_date_local:
        start_time = strava_act.start_date_local.isoformat()

    total_ascent_ft = None
    if strava_act.total_elevation_gain is not None:
        total_ascent_ft = round(float(strava_act.total_elevation_gain) * METERS_TO_FEET, 1)

    avg_hr = None
    if strava_act.average_heartrate is not None:
        avg_hr = round(float(strava_act.average_heartrate), 1)

    max_hr = None
    if strava_act.max_heartrate is not None:
        max_hr = round(float(strava_act.max_heartrate), 1)

    avg_cadence = None
    if strava_act.average_cadence is not None:
        # Strava reports full strides/min for running (no doubling needed)
        avg_cadence = round(float(strava_act.average_cadence) * 2, 1)

    calories = None
    if hasattr(strava_act, "calories") and strava_act.calories is not None:
        calories = round(float(strava_act.calories), 1)

    gear_id = None
    if strava_act.gear_id:
        gear_id = str(strava_act.gear_id)

    return {
        "strava_id": str(strava_act.id),
        "name": strava_act.name,
        "type": strava_act.type.root if hasattr(strava_act.type, 'root') else str(strava_act.type),
        "date": strava_act.start_date_local.strftime("%Y-%m-%d"),
        "start_time": start_time,
        "distance_mi": round(distance_mi, 3) if distance_mi else None,
        "duration_s": round(duration_s, 1) if duration_s else None,
        "elapsed_s": round(elapsed_s, 1) if elapsed_s else None,
        "avg_pace_s_per_mi": avg_pace,
        "avg_pace_display": avg_pace_display,
        "avg_hr": avg_hr,
        "max_hr": max_hr,
        "avg_cadence": avg_cadence,
        "total_ascent_ft": total_ascent_ft,
        "calories": calories,
        "gear_id": gear_id,
    }


# ---------------------------------------------------------------------------
# Merge, laps, streams, shoes
# ---------------------------------------------------------------------------

# Fields Strava can fill (only when NULL on canonical activity)
FILLABLE_FIELDS = [
    "start_time", "max_hr", "total_ascent_ft", "total_descent_ft",
    "calories", "duration_s", "avg_hr", "avg_cadence",
]

# Generic FIT/Strava default names that should be replaced by real Strava names
_GENERIC_NAME_PATTERNS = [
    "Outdoor Running", "Indoor Running", "Treadmill Running",
    "Morning Run", "Afternoon Run", "Evening Run", "Lunch Run",
    "Night Run",
]


def _is_generic_name(name: str | None) -> bool:
    """Check if a workout name is a generic default that should be overridden."""
    if not name:
        return True
    # Match exact generic names and day-prefixed variants like "Monday Morning Run"
    name_lower = name.lower().strip()
    for pattern in _GENERIC_NAME_PATTERNS:
        if name_lower == pattern.lower():
            return True
        # "Monday Morning Run", "Tuesday Afternoon Run", etc.
        if name_lower.endswith(pattern.lower()):
            return True
    return False


def _merge_fields(conn, activity_id: int, strava_data: dict, verbose: bool) -> list:
    """Fill NULL fields on canonical activity with Strava data. Returns list of filled fields."""
    # Map strava_data keys to DB column names (most are the same)
    field_map = {
        "start_time": "start_time",
        "max_hr": "max_hr",
        "total_ascent_ft": "total_ascent_ft",
        "calories": "calories",
        "duration_s": "duration_s",
        "avg_hr": "avg_hr",
        "avg_cadence": "avg_cadence",
    }

    # total_descent_ft: Strava doesn't provide descent separately in summary,
    # but we include it in the fillable list for future use
    filled = []
    for strava_key, db_col in field_map.items():
        strava_val = strava_data.get(strava_key)
        if strava_val is None:
            continue

        # Check if DB field is NULL
        row = conn.execute(
            f"SELECT {db_col} FROM activities WHERE id = ?", (activity_id,)
        ).fetchone()
        if row and row[0] is None:
            conn.execute(
                f"UPDATE activities SET {db_col} = ?, updated_at = datetime('now') WHERE id = ?",
                (strava_val, activity_id),
            )
            filled.append(db_col)
            if verbose:
                print(f"    FILL {db_col} = {strava_val}")

    # Replace generic workout names with real Strava names
    strava_name = strava_data.get("name")
    if strava_name and not _is_generic_name(strava_name):
        row = conn.execute(
            "SELECT workout_name FROM activities WHERE id = ?", (activity_id,)
        ).fetchone()
        current_name = row[0] if row else None
        if _is_generic_name(current_name):
            conn.execute(
                "UPDATE activities SET workout_name = ?, updated_at = datetime('now') WHERE id = ?",
                (strava_name, activity_id),
            )
            filled.append("workout_name")
            if verbose:
                print(f"    NAME '{current_name}' → '{strava_name}'")

    return filled


def _insert_activity_source(conn, activity_id: int | None, strava_data: dict,
                            match_status: str) -> int:
    """Insert an activity_source record for Strava data."""
    metadata = {
        "strava_id": strava_data["strava_id"],
        "strava_name": strava_data["name"],
        "strava_type": strava_data.get("type"),
        "match_status": match_status,
        "gear_id": strava_data.get("gear_id"),
        "start_date": strava_data.get("date"),
        "start_time": strava_data.get("start_time"),
    }
    if strava_data.get("elapsed_s"):
        metadata["elapsed_s"] = strava_data["elapsed_s"]

    cursor = conn.execute(
        """INSERT INTO activity_sources
           (activity_id, source, source_id, distance_mi, duration_s,
            avg_pace_s_per_mi, avg_hr, max_hr, avg_cadence,
            total_ascent_ft, calories, workout_name, metadata_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (activity_id, "strava", strava_data["strava_id"],
         strava_data["distance_mi"], strava_data["duration_s"],
         strava_data["avg_pace_s_per_mi"], strava_data["avg_hr"],
         strava_data["max_hr"], strava_data["avg_cadence"],
         strava_data["total_ascent_ft"], strava_data["calories"],
         strava_data["name"], json.dumps(metadata)),
    )
    return cursor.lastrowid


def _fetch_and_insert_laps(client, conn, strava_id: str, activity_id: int,
                           rate_limiter: StravaRateLimiter, verbose: bool) -> int:
    """Fetch laps from Strava and insert as intervals. Returns count."""
    laps = client.get_activity_laps(int(strava_id))

    # Hook into rate limiter via the session
    _update_rate_limiter(client, rate_limiter)

    cumulative_s = 0.0
    count = 0
    for i, lap in enumerate(laps, start=1):
        dist_mi = float(lap.distance) / METERS_PER_MILE if lap.distance else None
        elapsed_s = float(int(lap.elapsed_time)) if lap.elapsed_time else None
        moving_s = float(int(lap.moving_time)) if lap.moving_time else None
        dur_s = moving_s  # use moving time for pace

        pace = None
        pace_display = None
        if dist_mi and dist_mi > 0 and dur_s and dur_s > 0:
            pace = round(dur_s / dist_mi, 1)
            pace_display = format_pace(pace)

        avg_hr = round(float(lap.average_heartrate), 1) if lap.average_heartrate else None
        avg_cadence = round(float(lap.average_cadence) * 2, 1) if lap.average_cadence else None

        start_ts = cumulative_s
        end_ts = cumulative_s + (elapsed_s or 0)
        cumulative_s = end_ts

        conn.execute(
            """INSERT INTO intervals
               (activity_id, rep_number, gps_measured_distance_mi, duration_s,
                avg_pace_s_per_mi, avg_pace_display, avg_hr, avg_cadence,
                is_recovery, start_timestamp_s, end_timestamp_s, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (activity_id, i, round(dist_mi, 3) if dist_mi else None,
             round(dur_s, 1) if dur_s else None,
             pace, pace_display, avg_hr, avg_cadence, False,
             round(start_ts, 1), round(end_ts, 1), "strava_lap"),
        )
        count += 1

    return count


def _fetch_and_insert_streams(client, conn, strava_id: str, activity_id: int,
                              rate_limiter: StravaRateLimiter, verbose: bool,
                              source_id: int | None = None) -> int:
    """Fetch streams from Strava and insert. Returns point count."""
    stream_types = ["time", "latlng", "altitude", "heartrate", "cadence",
                    "velocity_smooth", "distance"]

    try:
        streams = client.get_activity_streams(
            int(strava_id), types=stream_types, resolution="high"
        )
    except Exception as e:
        if verbose:
            print(f"    WARN streams fetch failed: {e}")
        return 0

    _update_rate_limiter(client, rate_limiter)

    if not streams:
        return 0

    # Extract arrays (each stream type has a .data list)
    time_data = streams.get("time")
    latlng_data = streams.get("latlng")
    alt_data = streams.get("altitude")
    hr_data = streams.get("heartrate")
    cad_data = streams.get("cadence")
    vel_data = streams.get("velocity_smooth")
    dist_data = streams.get("distance")

    time_arr = time_data.data if time_data else []
    latlng_arr = latlng_data.data if latlng_data else []
    alt_arr = alt_data.data if alt_data else []
    hr_arr = hr_data.data if hr_data else []
    cad_arr = cad_data.data if cad_data else []
    vel_arr = vel_data.data if vel_data else []
    dist_arr = dist_data.data if dist_data else []

    n = len(time_arr)
    if n == 0:
        return 0

    rows = []
    for i in range(n):
        ts = time_arr[i] if i < len(time_arr) else None

        lat = None
        lon = None
        if i < len(latlng_arr) and latlng_arr[i]:
            lat = latlng_arr[i][0]
            lon = latlng_arr[i][1]

        alt_ft = None
        if i < len(alt_arr) and alt_arr[i] is not None:
            alt_ft = round(alt_arr[i] * METERS_TO_FEET, 1)

        hr = int(hr_arr[i]) if i < len(hr_arr) and hr_arr[i] is not None else None

        # Strava cadence is in RPM (revolutions per minute, i.e., steps per foot)
        # Multiply by 2 for full strides per minute
        cad = int(cad_arr[i] * 2) if i < len(cad_arr) and cad_arr[i] is not None else None

        pace = None
        if i < len(vel_arr) and vel_arr[i] and vel_arr[i] > 0:
            pace = round(METERS_PER_MILE / vel_arr[i], 1)

        dist_mi = None
        if i < len(dist_arr) and dist_arr[i] is not None:
            dist_mi = round(dist_arr[i] / METERS_PER_MILE, 4)

        rows.append((activity_id, ts, lat, lon, alt_ft, hr, cad, pace, dist_mi, source_id))

    conn.executemany(
        """INSERT INTO streams
           (activity_id, timestamp_s, lat, lon, altitude_ft,
            heart_rate, cadence, pace_s_per_mi, distance_mi, source_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    return len(rows)


def _update_rate_limiter(client, rate_limiter: StravaRateLimiter):
    """Update rate limiter from the client's last response."""
    # stravalib stores the last response on the protocol adapter
    try:
        resp = client.protocol.rsession.last_response
        if resp is not None:
            rate_limiter.update_from_response(resp)
    except AttributeError:
        pass


def _ensure_shoe(conn, client, gear_id: str, shoe_cache: dict,
                 rate_limiter: StravaRateLimiter, verbose: bool) -> int | None:
    """Look up or create a shoe from Strava gear_id. Returns shoes.id or None."""
    if gear_id in shoe_cache:
        return shoe_cache[gear_id]

    # Check if already in DB
    row = conn.execute(
        "SELECT id FROM shoes WHERE strava_gear_id = ?", (gear_id,)
    ).fetchone()
    if row:
        shoe_cache[gear_id] = row[0]
        return row[0]

    # Fetch from Strava API
    try:
        gear = client.get_gear(gear_id)
        _update_rate_limiter(client, rate_limiter)
    except Exception as e:
        if verbose:
            print(f"    WARN gear fetch failed for {gear_id}: {e}")
        shoe_cache[gear_id] = None
        return None

    if not rate_limiter.check(verbose):
        shoe_cache[gear_id] = None
        return None

    name = str(gear.name) if gear.name else gear_id
    brand = str(gear.brand_name) if hasattr(gear, "brand_name") and gear.brand_name else None
    model = str(gear.model_name) if hasattr(gear, "model_name") and gear.model_name else None

    cursor = conn.execute(
        """INSERT INTO shoes (name, brand, model, strava_gear_id)
           VALUES (?, ?, ?, ?)""",
        (name, brand, model, gear_id),
    )
    shoe_id = cursor.lastrowid
    shoe_cache[gear_id] = shoe_id

    if verbose:
        print(f"    SHOE {name} (gear {gear_id}) → shoe #{shoe_id}")

    return shoe_id


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def backfill_strava_laps(config: dict, verbose: bool = False) -> dict:
    """Fetch Strava laps for activities that have XLSX intervals but no Strava laps.

    These activities were matched during Strava sync but their laps were skipped
    because XLSX intervals already existed.  Strava laps are stored alongside
    XLSX intervals with source='strava_lap' and include timestamps for GPS-based
    centroid computation in the enrichment pipeline.

    Returns dict with keys: fetched, skipped, errors, rate_limit_pauses.
    """
    conn = get_connection(config)
    client = _get_client(config)
    rate_limiter = StravaRateLimiter()

    # Find activities with a Strava source + existing intervals but no strava_lap
    rows = conn.execute("""
        SELECT a.id,
               json_extract(s.metadata_json, '$.strava_id') AS strava_id,
               a.workout_name
        FROM activities a
        JOIN activity_sources s ON s.activity_id = a.id AND s.source = 'strava'
        WHERE EXISTS (
            SELECT 1 FROM intervals i
            WHERE i.activity_id = a.id AND (i.source IS NULL OR i.source = 'xlsx_split')
        )
        AND NOT EXISTS (
            SELECT 1 FROM intervals i
            WHERE i.activity_id = a.id AND i.source = 'strava_lap'
        )
        ORDER BY a.date
    """).fetchall()

    result = {"fetched": 0, "skipped": 0, "errors": 0, "rate_limit_pauses": 0}

    if verbose:
        print(f"Found {len(rows)} activities needing Strava laps.")

    for activity_id, strava_id, name in rows:
        if not strava_id:
            result["skipped"] += 1
            continue

        if not rate_limiter.check(verbose):
            result["rate_limit_pauses"] = rate_limiter.pause_count
            break

        try:
            laps = client.get_activity_laps(int(strava_id))
            _update_rate_limiter(client, rate_limiter)

            cumulative_s = 0.0
            count = 0
            for i, lap in enumerate(laps, start=1):
                dist_mi = float(lap.distance) / METERS_PER_MILE if lap.distance else None
                elapsed_s = float(int(lap.elapsed_time)) if lap.elapsed_time else None
                moving_s = float(int(lap.moving_time)) if lap.moving_time else None
                dur_s = moving_s  # use moving time for pace

                pace = None
                pace_display = None
                if dist_mi and dist_mi > 0 and dur_s and dur_s > 0:
                    pace = round(dur_s / dist_mi, 1)
                    pace_display = format_pace(pace)

                avg_hr = round(float(lap.average_heartrate), 1) if lap.average_heartrate else None
                avg_cadence = round(float(lap.average_cadence) * 2, 1) if lap.average_cadence else None

                start_ts = cumulative_s
                end_ts = cumulative_s + (elapsed_s or 0)
                cumulative_s = end_ts

                conn.execute(
                    """INSERT INTO intervals
                       (activity_id, rep_number, gps_measured_distance_mi, duration_s,
                        avg_pace_s_per_mi, avg_pace_display, avg_hr, avg_cadence,
                        is_recovery, start_timestamp_s, end_timestamp_s, source)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (activity_id, i,
                     round(dist_mi, 3) if dist_mi else None,
                     round(dur_s, 1) if dur_s else None,
                     pace, pace_display, avg_hr, avg_cadence, False,
                     round(start_ts, 1), round(end_ts, 1), "strava_lap"),
                )
                count += 1

            conn.commit()
            result["fetched"] += 1
            if verbose:
                print(f"  #{activity_id} ({name or '?'}): {count} Strava laps")

        except Exception as e:
            result["errors"] += 1
            if verbose:
                print(f"  #{activity_id} ERROR: {e}")
            try:
                conn.rollback()
            except Exception:
                pass

    result["rate_limit_pauses"] = rate_limiter.pause_count
    conn.close()
    return result


def backfill_orphan_streams(config: dict, conn, pairs: list[tuple],
                            verbose: bool = False) -> dict:
    """Fetch streams and laps from Strava for newly-linked orphans.

    Args:
        config: App config dict.
        conn: Open DB connection.
        pairs: List of (strava_id, activity_id, source_id) tuples to fetch.
        verbose: Print progress.

    Returns dict with keys: streams_inserted, laps_inserted, errors, rate_limit_pauses.
    """
    if not pairs:
        return {"streams_inserted": 0, "laps_inserted": 0, "errors": 0, "rate_limit_pauses": 0}

    client = _get_client(config)
    rate_limiter = StravaRateLimiter()

    result = {"streams_inserted": 0, "laps_inserted": 0, "errors": 0, "rate_limit_pauses": 0}

    for item in pairs:
        strava_id, activity_id = item[0], item[1]
        source_id = item[2] if len(item) > 2 else None
        if not rate_limiter.check(verbose):
            result["rate_limit_pauses"] = rate_limiter.pause_count
            break

        try:
            # Streams
            if not _activity_has_streams(conn, activity_id):
                stream_count = _fetch_and_insert_streams(
                    client, conn, strava_id, activity_id, rate_limiter, verbose,
                    source_id=source_id)
                result["streams_inserted"] += stream_count
                if verbose and stream_count:
                    print(f"    STREAMS strava:{strava_id} → activity #{activity_id}: {stream_count} points")

            # Laps (only if activity has no intervals yet)
            if not rate_limiter.check(verbose):
                result["rate_limit_pauses"] = rate_limiter.pause_count
                break

            if not _activity_has_intervals(conn, activity_id):
                lap_count = _fetch_and_insert_laps(
                    client, conn, strava_id, activity_id, rate_limiter, verbose)
                result["laps_inserted"] += lap_count
                if verbose and lap_count:
                    print(f"    LAPS strava:{strava_id} → activity #{activity_id}: {lap_count} intervals")

            conn.commit()
        except Exception as e:
            result["errors"] += 1
            if verbose:
                print(f"    ERROR fetching strava:{strava_id}: {e}")
            try:
                conn.rollback()
            except Exception:
                pass

    result["rate_limit_pauses"] = rate_limiter.pause_count
    return result


def sync_strava(config: dict, dry_run: bool = False, verbose: bool = False,
                full_history: bool = False, fetch_streams: bool = True) -> dict:
    """Sync activities from Strava API.

    Returns summary dict with keys: matched, unmatched, skipped, errors,
    fields_filled, laps_inserted, streams_inserted, shoes_created,
    rate_limit_pauses, details.
    """
    client = _get_client(config)
    conn = get_connection(config)
    rate_limiter = StravaRateLimiter()

    tolerance_pct = config.get("reconcile", {}).get("distance_tolerance_pct", 5)

    # Build lookup structures
    lookup = _build_activity_lookup(conn)
    processed_ids = _load_processed_strava_ids(conn)
    shoe_cache = {}

    # Determine fetch range
    after = None
    if not full_history:
        after = _get_last_sync_timestamp(conn)
        if verbose and after:
            print(f"Fetching activities after {after.isoformat()}")

    if verbose:
        total_db = sum(len(v) for v in lookup.values())
        print(f"DB has {total_db} activities across {len(lookup)} dates")
        print(f"Already processed {len(processed_ids)} Strava activities")

    # Fetch activity list from Strava
    if verbose:
        print("Fetching activity list from Strava...")

    strava_activities = []
    try:
        activities_iter = client.get_activities(after=after)
        for act in activities_iter:
            _update_rate_limiter(client, rate_limiter)
            act_type = act.type.root if hasattr(act.type, 'root') else str(act.type)
            if act_type in RUNNING_TYPES:
                strava_activities.append(act)
    except Exception as e:
        print(f"Error fetching activity list: {e}")
        conn.close()
        return {"matched": 0, "unmatched": 0, "skipped": 0, "errors": 1,
                "fields_filled": 0, "laps_inserted": 0, "streams_inserted": 0,
                "shoes_created": 0, "rate_limit_pauses": 0, "details": []}

    if verbose:
        print(f"Found {len(strava_activities)} running activities on Strava")

    result = {
        "matched": 0, "unmatched": 0, "skipped": 0, "errors": 0,
        "fields_filled": 0, "laps_inserted": 0, "streams_inserted": 0,
        "shoes_created": 0, "rate_limit_pauses": 0, "details": [],
    }

    latest_timestamp = None

    for strava_act in strava_activities:
        strava_id = str(strava_act.id)

        # Track latest activity for sync_state
        if strava_act.start_date:
            act_ts = strava_act.start_date
            if hasattr(act_ts, 'timestamp'):
                if latest_timestamp is None or act_ts > latest_timestamp:
                    latest_timestamp = act_ts

        # Skip if already processed
        if strava_id in processed_ids:
            result["skipped"] += 1
            continue

        # Check rate limit
        if not rate_limiter.check(verbose):
            result["rate_limit_pauses"] = rate_limiter.pause_count
            break

        try:
            strava_data = _extract_strava_data(strava_act)
            match = _match_strava_activity(strava_act, lookup, tolerance_pct)

            if match:
                activity_id = match["id"]
                match_status = "matched"

                if verbose:
                    print(f"  MATCH {strava_data['date']} {strava_data['distance_mi']:.2f}mi "
                          f"→ activity #{activity_id} "
                          f"(DB: {match['distance_mi']:.2f}mi)")

                if not dry_run:
                    # Merge NULL fields
                    filled = _merge_fields(conn, activity_id, strava_data, verbose)
                    result["fields_filled"] += len(filled)

                    # Insert activity source
                    src_id = _insert_activity_source(conn, activity_id, strava_data, match_status)

                    # Laps (skip if activity already has intervals from XLSX)
                    if not _activity_has_intervals(conn, activity_id):
                        if rate_limiter.check(verbose):
                            lap_count = _fetch_and_insert_laps(
                                client, conn, strava_id, activity_id, rate_limiter, verbose)
                            result["laps_inserted"] += lap_count
                            if verbose and lap_count:
                                print(f"    LAPS {lap_count} intervals")

                    # Streams
                    if fetch_streams and not _activity_has_streams(conn, activity_id):
                        if rate_limiter.check(verbose):
                            stream_count = _fetch_and_insert_streams(
                                client, conn, strava_id, activity_id, rate_limiter, verbose,
                                source_id=src_id)
                            result["streams_inserted"] += stream_count
                            if verbose and stream_count:
                                print(f"    STREAMS {stream_count} points")

                    # Shoe handling
                    if strava_data.get("gear_id"):
                        if rate_limiter.check(verbose):
                            shoe_id = _ensure_shoe(
                                conn, client, strava_data["gear_id"],
                                shoe_cache, rate_limiter, verbose)
                            if shoe_id and match["shoe_id"] is None:
                                conn.execute(
                                    "UPDATE activities SET shoe_id = ?, updated_at = datetime('now') WHERE id = ?",
                                    (shoe_id, activity_id),
                                )
                                if verbose:
                                    print(f"    SHOE → shoe #{shoe_id}")

                    # Record in processed_files
                    conn.execute(
                        "INSERT INTO processed_files (file_path, source, activity_id) VALUES (?, ?, ?)",
                        (f"strava:{strava_id}", "strava", activity_id),
                    )
                    conn.commit()

                    # Remove matched DB activity from lookup to prevent double-matching
                    date = match["date"]
                    if date in lookup:
                        lookup[date] = [a for a in lookup[date] if a["id"] != activity_id]

                result["matched"] += 1
                result["details"].append({
                    "strava_id": strava_id, "status": "matched",
                    "activity_id": activity_id, "date": strava_data["date"],
                })

            else:
                # Unmatched
                if verbose:
                    print(f"  UNMATCHED {strava_data['date']} {strava_data['distance_mi']:.2f}mi "
                          f'"{strava_data["name"]}"')

                if not dry_run:
                    _insert_activity_source(conn, None, strava_data, "unmatched")
                    conn.execute(
                        "INSERT INTO processed_files (file_path, source) VALUES (?, ?)",
                        (f"strava:{strava_id}", "strava"),
                    )
                    conn.commit()

                result["unmatched"] += 1
                result["details"].append({
                    "strava_id": strava_id, "status": "unmatched",
                    "date": strava_data["date"],
                })

        except Exception as e:
            result["errors"] += 1
            result["details"].append({
                "strava_id": strava_id, "status": "error", "error": str(e),
            })
            if verbose:
                print(f"  ERROR {strava_id}: {e}")
            try:
                conn.rollback()
            except Exception:
                pass

    # Update sync_state
    if not dry_run and (result["matched"] > 0 or result["unmatched"] > 0):
        now = datetime.now(timezone.utc).isoformat()
        meta = {
            "matched": result["matched"],
            "unmatched": result["unmatched"],
        }
        if latest_timestamp:
            meta["last_activity_timestamp"] = latest_timestamp.isoformat()

        conn.execute(
            """INSERT INTO sync_state (source, last_sync_at, metadata_json)
               VALUES ('strava', ?, ?)
               ON CONFLICT(source)
               DO UPDATE SET last_sync_at=excluded.last_sync_at,
                             metadata_json=excluded.metadata_json""",
            (now, json.dumps(meta)),
        )
        conn.commit()

    result["rate_limit_pauses"] = rate_limiter.pause_count
    result["shoes_created"] = len([v for v in shoe_cache.values() if v is not None])

    conn.close()
    return result
