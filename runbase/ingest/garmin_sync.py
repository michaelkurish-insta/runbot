"""Garmin Connect sync — fetch activities and enrich existing DB records.

Mirrors the Strava sync pattern: API for activity discovery/matching,
FIT file download for streams and laps (reusing fit_parser.py).
"""

import io
import json
import time as time_mod
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from runbase.db import get_connection
from runbase.ingest.fit_parser import format_pace, parse_fit_file
from runbase.ingest.common import (
    METERS_PER_MILE, METERS_TO_FEET,
    build_activity_lookup, match_activity, merge_fields,
    activity_has_intervals, activity_has_streams,
)

# Garmin activity type keys we care about
RUNNING_TYPE_KEYS = {"running", "trail_running", "treadmill_running", "track_running"}

# Rate limiting defaults (seconds)
_PAGE_DELAY = 1.5
_DOWNLOAD_DELAY = 2.0
_MAX_BACKOFF = 60
_PAGE_SIZE = 100
_CONSECUTIVE_KNOWN_STOP = 10


# ---------------------------------------------------------------------------
# Auth / client
# ---------------------------------------------------------------------------

def _get_client(config: dict):
    """Create an authenticated Garmin client, resuming from saved tokens."""
    from garminconnect import Garmin, GarminConnectAuthenticationError

    garmin_cfg = config.get("garmin", {})
    token_store = Path(
        garmin_cfg.get("token_store", "~/runbase/state/garmin_tokens")
    ).expanduser()

    # Try resuming from saved tokens first
    if token_store.exists():
        try:
            garmin = Garmin()
            garmin.login(tokenstore=str(token_store))
            return garmin
        except GarminConnectAuthenticationError:
            pass  # tokens expired, fall through to credential login

    # Fall back to credential login
    email = garmin_cfg.get("email")
    password = garmin_cfg.get("password")
    if not email or not password:
        raise FileNotFoundError(
            f"Garmin tokens not found at {token_store} and no credentials configured. "
            f"Run: python scripts/setup_garmin_auth.py"
        )

    garmin = Garmin(email, password)
    garmin.login()

    # Save tokens for next time
    token_store.mkdir(parents=True, exist_ok=True)
    garmin.garth.dump(str(token_store))

    return garmin


# ---------------------------------------------------------------------------
# DB lookup helpers
# ---------------------------------------------------------------------------

def _load_processed_garmin_ids(conn) -> set:
    """Load already-processed Garmin IDs from processed_files + suppressed_sources."""
    rows = conn.execute(
        "SELECT file_path FROM processed_files WHERE source = 'garmin'"
    ).fetchall()
    ids = set()
    for r in rows:
        path = r[0]
        if path.startswith("garmin:"):
            ids.add(path[7:])
    # Also skip suppressed (user-deleted) Garmin IDs
    suppressed = conn.execute(
        "SELECT source_identifier FROM suppressed_sources WHERE source = 'garmin'"
    ).fetchall()
    for r in suppressed:
        ids.add(r[0])
    return ids


def _get_last_sync_timestamp(conn) -> str | None:
    """Get the last synced Garmin activity date from sync_state."""
    row = conn.execute(
        "SELECT metadata_json FROM sync_state WHERE source = 'garmin'"
    ).fetchone()
    if row and row[0]:
        meta = json.loads(row[0])
        return meta.get("last_activity_date")
    return None


# ---------------------------------------------------------------------------
# Activity fetching
# ---------------------------------------------------------------------------

def _is_running_activity(garmin_act: dict) -> bool:
    """Check if a Garmin activity is a running type we care about."""
    act_type = garmin_act.get("activityType", {})
    type_key = act_type.get("typeKey", "")
    return type_key in RUNNING_TYPE_KEYS


def _fetch_running_activities(garmin, full_history: bool, last_sync_date: str | None,
                              processed_ids: set, verbose: bool) -> list[dict]:
    """Fetch running activities from Garmin, paginated newest-first.

    Stops after _CONSECUTIVE_KNOWN_STOP consecutive already-processed IDs
    (unless full_history=True).
    """
    from garminconnect import GarminConnectTooManyRequestsError

    all_activities = []
    start = 0
    consecutive_known = 0

    while True:
        if verbose:
            print(f"  Fetching activities offset={start}...")

        backoff = 2
        for attempt in range(5):
            try:
                batch = garmin.get_activities(start=start, limit=_PAGE_SIZE,
                                              activitytype="running")
                break
            except GarminConnectTooManyRequestsError:
                if verbose:
                    print(f"    Rate limited, backing off {backoff}s...")
                time_mod.sleep(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF)
        else:
            if verbose:
                print("    Max retries reached, stopping pagination.")
            break

        if not batch:
            break

        for act in batch:
            garmin_id = str(act.get("activityId", ""))

            if garmin_id in processed_ids:
                consecutive_known += 1
                if not full_history and consecutive_known >= _CONSECUTIVE_KNOWN_STOP:
                    if verbose:
                        print(f"  {_CONSECUTIVE_KNOWN_STOP} consecutive known IDs, stopping.")
                    return all_activities
                continue
            else:
                consecutive_known = 0

            if _is_running_activity(act):
                all_activities.append(act)

        start += len(batch)
        time_mod.sleep(_PAGE_DELAY)

        if len(batch) < _PAGE_SIZE:
            break

    return all_activities


# ---------------------------------------------------------------------------
# Data extraction
# ---------------------------------------------------------------------------

def _extract_garmin_data(garmin_act: dict) -> dict:
    """Extract and convert fields from a Garmin activity dict."""
    distance_m = garmin_act.get("distance") or 0
    distance_mi = distance_m / METERS_PER_MILE if distance_m else None

    duration_s = garmin_act.get("movingDuration") or garmin_act.get("duration")
    elapsed_s = garmin_act.get("elapsedDuration") or garmin_act.get("duration")

    avg_pace = None
    avg_pace_display = None
    if distance_mi and distance_mi > 0 and duration_s and duration_s > 0:
        avg_pace = round(duration_s / distance_mi, 1)
        avg_pace_display = format_pace(avg_pace)

    start_time = garmin_act.get("startTimeLocal")  # "YYYY-MM-DD HH:MM:SS"

    # Parse date from startTimeLocal
    date_str = None
    if start_time:
        date_str = start_time[:10]  # "YYYY-MM-DD"

    total_ascent_ft = None
    elev_gain = garmin_act.get("elevationGain")
    if elev_gain is not None:
        total_ascent_ft = round(elev_gain * METERS_TO_FEET, 1)

    total_descent_ft = None
    elev_loss = garmin_act.get("elevationLoss")
    if elev_loss is not None:
        total_descent_ft = round(elev_loss * METERS_TO_FEET, 1)

    avg_hr = None
    if garmin_act.get("averageHR") is not None:
        avg_hr = round(float(garmin_act["averageHR"]), 1)

    max_hr = None
    if garmin_act.get("maxHR") is not None:
        max_hr = round(float(garmin_act["maxHR"]), 1)

    # Garmin reports full strides/min in averageRunningCadenceInStepsPerMinute
    # (unlike FIT/Strava which report per-foot). No doubling needed.
    avg_cadence = None
    cad_field = garmin_act.get("averageRunningCadenceInStepsPerMinute")
    if cad_field is not None:
        avg_cadence = round(float(cad_field), 1)

    calories = None
    if garmin_act.get("calories") is not None:
        calories = round(float(garmin_act["calories"]), 1)

    return {
        "garmin_id": str(garmin_act.get("activityId", "")),
        "name": garmin_act.get("activityName") or "Garmin Activity",
        "type": garmin_act.get("activityType", {}).get("typeKey", "running"),
        "date": date_str,
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
        "total_descent_ft": total_descent_ft,
        "calories": calories,
    }


# ---------------------------------------------------------------------------
# Source insertion
# ---------------------------------------------------------------------------

def _insert_activity_source(conn, activity_id: int | None, garmin_data: dict,
                            match_status: str, fit_path: str | None = None) -> int:
    """Insert an activity_source record for Garmin data."""
    metadata = {
        "garmin_id": garmin_data["garmin_id"],
        "garmin_name": garmin_data["name"],
        "garmin_type": garmin_data.get("type"),
        "match_status": match_status,
        "start_date": garmin_data.get("date"),
        "start_time": garmin_data.get("start_time"),
    }
    if garmin_data.get("elapsed_s"):
        metadata["elapsed_s"] = garmin_data["elapsed_s"]
    if garmin_data.get("total_descent_ft"):
        metadata["total_descent_ft"] = garmin_data["total_descent_ft"]

    cursor = conn.execute(
        """INSERT INTO activity_sources
           (activity_id, source, source_id, raw_file_path, distance_mi, duration_s,
            avg_pace_s_per_mi, avg_hr, max_hr, avg_cadence,
            total_ascent_ft, calories, workout_name, metadata_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (activity_id, "garmin", garmin_data["garmin_id"], fit_path,
         garmin_data["distance_mi"], garmin_data["duration_s"],
         garmin_data["avg_pace_s_per_mi"], garmin_data["avg_hr"],
         garmin_data["max_hr"], garmin_data["avg_cadence"],
         garmin_data["total_ascent_ft"], garmin_data["calories"],
         garmin_data["name"], json.dumps(metadata)),
    )
    return cursor.lastrowid


# ---------------------------------------------------------------------------
# FIT download + import
# ---------------------------------------------------------------------------

def _save_fit_from_download(fit_bytes: bytes, dest_dir: Path,
                            garmin_id: str) -> Path:
    """Handle ZIP wrapping and save the .fit file. Returns path to saved file."""
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Garmin wraps FIT in ZIP — detect via PK magic bytes
    if fit_bytes[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(fit_bytes)) as zf:
            fit_names = [n for n in zf.namelist() if n.lower().endswith(".fit")]
            if not fit_names:
                raise ValueError(f"ZIP for garmin:{garmin_id} contains no .fit files")
            fit_data = zf.read(fit_names[0])
            dest_path = dest_dir / f"garmin_{garmin_id}.fit"
    else:
        # Raw .fit bytes (unlikely but possible)
        fit_data = fit_bytes
        dest_path = dest_dir / f"garmin_{garmin_id}.fit"

    # Handle name collisions
    if dest_path.exists():
        stem = dest_path.stem
        suffix = dest_path.suffix
        counter = 1
        while dest_path.exists():
            dest_path = dest_dir / f"{stem}_{counter}{suffix}"
            counter += 1

    dest_path.write_bytes(fit_data)
    return dest_path


def _download_and_import_fit(garmin, garmin_id: str, conn,
                              activity_id: int, source_id: int,
                              raw_store: str, verbose: bool) -> dict:
    """Download FIT file from Garmin, parse, and insert streams + laps.

    Returns dict with stream_count and lap_count.
    """
    from garminconnect import Garmin as GarminClass, GarminConnectTooManyRequestsError

    result = {"stream_count": 0, "lap_count": 0}

    # Download with retry/backoff
    backoff = 2
    for attempt in range(3):
        try:
            fit_bytes = garmin.download_activity(
                garmin_id,
                dl_fmt=GarminClass.ActivityDownloadFormat.ORIGINAL,
            )
            break
        except GarminConnectTooManyRequestsError:
            if verbose:
                print(f"    Rate limited on FIT download, backing off {backoff}s...")
            time_mod.sleep(backoff)
            backoff = min(backoff * 2, _MAX_BACKOFF)
        except Exception as e:
            if verbose:
                print(f"    WARN FIT download failed: {e}")
            return result
    else:
        if verbose:
            print(f"    WARN FIT download gave up after retries")
        return result

    if not fit_bytes:
        return result

    # Save to raw store
    raw_dir = Path(raw_store).expanduser()
    try:
        fit_path = _save_fit_from_download(fit_bytes, raw_dir, garmin_id)
    except Exception as e:
        if verbose:
            print(f"    WARN FIT save failed: {e}")
        return result

    # Update source with fit path
    conn.execute(
        "UPDATE activity_sources SET raw_file_path = ? WHERE id = ?",
        (str(fit_path), source_id),
    )

    # Parse the FIT file
    try:
        parsed = parse_fit_file(str(fit_path))
    except Exception as e:
        if verbose:
            print(f"    WARN FIT parse failed: {e}")
        return result

    # Insert streams (same pattern as icloud_sync)
    if parsed.streams:
        conn.executemany(
            """INSERT INTO streams
               (activity_id, timestamp_s, lat, lon, altitude_ft,
                heart_rate, cadence, pace_s_per_mi, distance_mi, source_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [(activity_id, st.timestamp_s, st.lat, st.lon, st.altitude_ft,
              st.heart_rate, st.cadence, st.pace_s_per_mi, st.distance_mi,
              source_id)
             for st in parsed.streams],
        )
        result["stream_count"] = len(parsed.streams)

    # Insert laps/intervals
    for lap in parsed.laps:
        conn.execute(
            """INSERT INTO intervals
               (activity_id, rep_number, gps_measured_distance_mi, duration_s,
                avg_pace_s_per_mi, avg_pace_display, avg_hr, avg_cadence, is_recovery,
                start_timestamp_s, end_timestamp_s, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (activity_id, lap.rep_number, lap.gps_measured_distance_mi, lap.duration_s,
             lap.avg_pace_s_per_mi, lap.avg_pace_display, lap.avg_hr, lap.avg_cadence,
             lap.is_recovery, lap.start_timestamp_s, lap.end_timestamp_s, "fit_lap"),
        )
        result["lap_count"] += 1

    return result


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def sync_garmin(config: dict, dry_run: bool = False, verbose: bool = False,
                full_history: bool = False, fetch_fit: bool = True) -> dict:
    """Sync activities from Garmin Connect.

    Returns summary dict with keys: matched, unmatched, skipped, errors,
    fields_filled, laps_inserted, streams_inserted,
    rate_limit_pauses, details.
    """
    garmin = _get_client(config)
    conn = get_connection(config)

    tolerance_pct = config.get("reconcile", {}).get("distance_tolerance_pct", 5)
    raw_store = config["paths"]["raw_store"]

    # Build lookup structures
    lookup = build_activity_lookup(conn)
    processed_ids = _load_processed_garmin_ids(conn)

    last_sync_date = None
    if not full_history:
        last_sync_date = _get_last_sync_timestamp(conn)
        if verbose and last_sync_date:
            print(f"Last Garmin sync date: {last_sync_date}")

    if verbose:
        total_db = sum(len(v) for v in lookup.values())
        print(f"DB has {total_db} activities across {len(lookup)} dates")
        print(f"Already processed {len(processed_ids)} Garmin activities")

    # Fetch activity list
    if verbose:
        print("Fetching activity list from Garmin...")

    garmin_activities = _fetch_running_activities(
        garmin, full_history, last_sync_date, processed_ids, verbose)

    if verbose:
        print(f"Found {len(garmin_activities)} new running activities on Garmin")

    result = {
        "matched": 0, "unmatched": 0, "skipped": 0, "errors": 0,
        "fields_filled": 0, "laps_inserted": 0, "streams_inserted": 0,
        "rate_limit_pauses": 0, "details": [],
    }

    latest_date = None

    for garmin_act in garmin_activities:
        garmin_id = str(garmin_act.get("activityId", ""))

        # Skip if already processed (double-check)
        if garmin_id in processed_ids:
            result["skipped"] += 1
            continue

        try:
            garmin_data = _extract_garmin_data(garmin_act)
            act_date = garmin_data.get("date")

            # Track latest date for sync_state
            if act_date and (latest_date is None or act_date > latest_date):
                latest_date = act_date

            if not act_date:
                if verbose:
                    print(f"  SKIP garmin:{garmin_id} (no date)")
                result["skipped"] += 1
                continue

            dist_mi = garmin_data["distance_mi"] or 0
            match = match_activity(act_date, dist_mi, lookup, tolerance_pct)

            if match:
                activity_id = match["id"]
                match_status = "matched"

                if verbose:
                    print(f"  MATCH {act_date} {dist_mi:.2f}mi "
                          f"→ activity #{activity_id} "
                          f"(DB: {match['distance_mi']:.2f}mi)")

                if not dry_run:
                    # Merge NULL fields
                    filled = merge_fields(conn, activity_id, garmin_data, verbose)
                    result["fields_filled"] += len(filled)

                    # Insert activity source
                    src_id = _insert_activity_source(
                        conn, activity_id, garmin_data, match_status)

                    # Download FIT + insert streams/laps
                    if fetch_fit:
                        if not activity_has_streams(conn, activity_id):
                            time_mod.sleep(_DOWNLOAD_DELAY)
                            fit_result = _download_and_import_fit(
                                garmin, garmin_id, conn, activity_id,
                                src_id, raw_store, verbose)
                            result["streams_inserted"] += fit_result["stream_count"]
                            if verbose and fit_result["stream_count"]:
                                print(f"    STREAMS {fit_result['stream_count']} points")

                            if not activity_has_intervals(conn, activity_id):
                                result["laps_inserted"] += fit_result["lap_count"]
                                if verbose and fit_result["lap_count"]:
                                    print(f"    LAPS {fit_result['lap_count']} intervals")
                            elif fit_result["lap_count"]:
                                # Already has intervals — laps were inserted from FIT
                                # but they supplement existing ones
                                result["laps_inserted"] += fit_result["lap_count"]

                    # Record in processed_files
                    conn.execute(
                        "INSERT INTO processed_files (file_path, source, activity_id) "
                        "VALUES (?, ?, ?)",
                        (f"garmin:{garmin_id}", "garmin", activity_id),
                    )
                    conn.commit()

                    # Remove from lookup to prevent double-matching
                    date = match["date"]
                    if date in lookup:
                        lookup[date] = [a for a in lookup[date] if a["id"] != activity_id]

                result["matched"] += 1
                result["details"].append({
                    "garmin_id": garmin_id, "status": "matched",
                    "activity_id": activity_id, "date": act_date,
                })

            else:
                # Unmatched
                if verbose:
                    print(f"  UNMATCHED {act_date} {dist_mi:.2f}mi "
                          f'"{garmin_data["name"]}"')

                if not dry_run:
                    _insert_activity_source(conn, None, garmin_data, "unmatched")
                    conn.execute(
                        "INSERT INTO processed_files (file_path, source) VALUES (?, ?)",
                        (f"garmin:{garmin_id}", "garmin"),
                    )
                    conn.commit()

                result["unmatched"] += 1
                result["details"].append({
                    "garmin_id": garmin_id, "status": "unmatched",
                    "date": act_date,
                })

        except Exception as e:
            result["errors"] += 1
            result["details"].append({
                "garmin_id": garmin_id, "status": "error", "error": str(e),
            })
            if verbose:
                print(f"  ERROR {garmin_id}: {e}")
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
        if latest_date:
            meta["last_activity_date"] = latest_date

        conn.execute(
            """INSERT INTO sync_state (source, last_sync_at, metadata_json)
               VALUES ('garmin', ?, ?)
               ON CONFLICT(source)
               DO UPDATE SET last_sync_at=excluded.last_sync_at,
                             metadata_json=excluded.metadata_json""",
            (now, json.dumps(meta)),
        )
        conn.commit()

    # Save updated tokens after sync
    garmin_cfg = config.get("garmin", {})
    token_store = Path(
        garmin_cfg.get("token_store", "~/runbase/state/garmin_tokens")
    ).expanduser()
    try:
        garmin.garth.dump(str(token_store))
    except Exception:
        pass

    conn.close()
    return result
