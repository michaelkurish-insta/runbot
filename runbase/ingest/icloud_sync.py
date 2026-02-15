"""Scan iCloud HealthFit folder, parse .fit files, and import into RunBase DB."""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from runbase.db import get_connection
from runbase.ingest.fit_parser import parse_fit_file


def _enrich_new_activities(conn, activity_ids: list[int], verbose: bool) -> int:
    """Try to match new FIT activities against orphaned Strava sources."""
    from runbase.reconcile.matcher import find_strava_match
    from runbase.reconcile.enricher import enrich_from_strava

    enriched = 0
    for activity_id in activity_ids:
        row = conn.execute(
            "SELECT date, distance_mi FROM activities WHERE id = ?",
            (activity_id,),
        ).fetchone()
        if not row:
            continue

        date, distance_mi = row
        match = find_strava_match(conn, date, distance_mi)
        if match:
            if verbose:
                print(f"  ENRICH activity #{activity_id} ← Strava \"{match.get('strava_name', '')}\"")
            enrich_from_strava(conn, activity_id, match, verbose=verbose)
            conn.commit()
            enriched += 1

    return enriched


def _interval_enrich_new(conn, activity_ids: list[int], config: dict, verbose: bool) -> int:
    """Run interval enrichment on newly imported activities (if VDOT is set)."""
    from runbase.analysis.vdot import get_current_vdot
    from runbase.analysis.interval_enricher import enrich_activity

    enriched = 0
    for activity_id in activity_ids:
        row = conn.execute("SELECT date FROM activities WHERE id = ?", (activity_id,)).fetchone()
        if not row:
            continue
        vdot = get_current_vdot(conn, row[0])
        if not vdot:
            continue
        try:
            enrich_activity(conn, activity_id, config, verbose=verbose)
            enriched += 1
        except Exception as e:
            if verbose:
                print(f"  WARN  interval enrichment failed for #{activity_id}: {e}")
    return enriched


def sync_icloud(config: dict, dry_run: bool = False, verbose: bool = False) -> dict:
    """Scan iCloud HealthFit folder and import new .fit files.

    Returns dict with keys: new, skipped, errors, details.
    """
    icloud_path = config["paths"]["icloud_healthfit"]
    raw_store_path = config["paths"]["raw_store"]

    fit_files = _scan_fit_files(icloud_path)

    if verbose:
        print(f"Found {len(fit_files)} .fit file(s) in {icloud_path}")

    conn = get_connection(config)
    result = {"new": 0, "skipped": 0, "errors": 0, "enriched": 0, "details": []}

    new_activity_ids = []
    for file_path in fit_files:
        try:
            activity_id = _import_single_file(conn, file_path, raw_store_path, dry_run, verbose)
            if activity_id is not None:
                result["new"] += 1
                result["details"].append({"file": str(file_path), "status": "imported",
                                          "activity_id": activity_id})
                if not dry_run and activity_id > 0:
                    new_activity_ids.append(activity_id)
            else:
                result["skipped"] += 1
                if verbose:
                    print(f"  SKIP  {file_path.name} (already processed)")
        except Exception as e:
            result["errors"] += 1
            result["details"].append({"file": str(file_path), "status": "error", "error": str(e)})
            if verbose:
                print(f"  ERROR {file_path.name}: {e}")

    # Post-import enrichment: match new activities against orphaned Strava sources
    if new_activity_ids:
        result["enriched"] = _enrich_new_activities(conn, new_activity_ids, verbose)

    # Post-import interval enrichment (pace zones, track detection, etc.)
    if new_activity_ids:
        result["interval_enriched"] = _interval_enrich_new(conn, new_activity_ids, config, verbose)

    # Update sync state
    if not dry_run and result["new"] > 0:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """INSERT INTO sync_state (source, last_sync_at, metadata_json)
               VALUES ('icloud', ?, ?)
               ON CONFLICT(source)
               DO UPDATE SET last_sync_at=excluded.last_sync_at,
                             metadata_json=excluded.metadata_json""",
            (now, json.dumps({"files_imported": result["new"]})),
        )
        conn.commit()

    conn.close()
    return result


def _scan_fit_files(icloud_path: str) -> list[Path]:
    """Glob for .fit files (case-insensitive), sorted by name."""
    root = Path(icloud_path)
    if not root.exists():
        raise FileNotFoundError(f"iCloud HealthFit folder not found: {root}")

    fit_files = sorted(
        [f for f in root.rglob("*") if f.suffix.lower() == ".fit"],
        key=lambda p: p.name,
    )
    return fit_files


def _is_already_processed(conn, file_path: str, file_hash: str) -> bool:
    """Check if a file has already been processed (by path or hash)."""
    row = conn.execute(
        "SELECT id FROM processed_files WHERE file_path = ? OR file_hash = ?",
        (str(file_path), file_hash),
    ).fetchone()
    return row is not None


def _copy_to_raw_store(file_path: Path, raw_store_path: str) -> str:
    """Copy .fit file to raw_store archive. Returns destination path."""
    dest_dir = Path(raw_store_path)
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest = dest_dir / file_path.name
    # Handle name collisions
    if dest.exists():
        stem = file_path.stem
        suffix = file_path.suffix
        counter = 1
        while dest.exists():
            dest = dest_dir / f"{stem}_{counter}{suffix}"
            counter += 1

    shutil.copy2(str(file_path), str(dest))
    return str(dest)


def _import_single_file(
    conn, file_path: Path, raw_store_path: str, dry_run: bool, verbose: bool
) -> int | None:
    """Parse and import a single .fit file. Returns activity_id if imported, None if skipped."""
    from runbase.ingest.fit_parser import _compute_file_hash

    file_hash = _compute_file_hash(str(file_path))

    if _is_already_processed(conn, str(file_path), file_hash):
        return None

    parsed = parse_fit_file(str(file_path))

    if dry_run:
        if verbose:
            print(f"  DRY   {file_path.name} → {parsed.activity.date} "
                  f"{parsed.activity.distance_mi:.2f}mi {parsed.activity.avg_pace_display}/mi")
        return 0  # truthy, signals dry-run import

    # Archive to raw store
    archived_path = _copy_to_raw_store(file_path, raw_store_path)

    # Single transaction for all inserts
    cursor = conn.cursor()
    try:
        # 1. Insert activity
        a = parsed.activity
        a.fit_file_path = archived_path
        cursor.execute(
            """INSERT INTO activities
               (date, start_time, distance_mi, duration_s, avg_pace_s_per_mi,
                avg_pace_display, avg_hr, max_hr, avg_cadence, total_ascent_ft,
                total_descent_ft, calories, workout_type, workout_name, fit_file_path)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (a.date, a.start_time, a.distance_mi, a.duration_s, a.avg_pace_s_per_mi,
             a.avg_pace_display, a.avg_hr, a.max_hr, a.avg_cadence, a.total_ascent_ft,
             a.total_descent_ft, a.calories, a.workout_type, a.workout_name, a.fit_file_path),
        )
        activity_id = cursor.lastrowid

        # 2. Insert activity source
        s = parsed.source
        cursor.execute(
            """INSERT INTO activity_sources
               (activity_id, source, source_id, raw_file_path, distance_mi, duration_s,
                avg_pace_s_per_mi, avg_hr, max_hr, avg_cadence, total_ascent_ft,
                calories, workout_name, metadata_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (activity_id, s.source, s.source_id, s.raw_file_path,
             s.distance_mi, s.duration_s, s.avg_pace_s_per_mi, s.avg_hr, s.max_hr,
             s.avg_cadence, s.total_ascent_ft, s.calories, s.workout_name,
             json.dumps(parsed.device_info) if parsed.device_info else None),
        )

        source_id = cursor.lastrowid

        # 3. Batch insert streams
        if parsed.streams:
            cursor.executemany(
                """INSERT INTO streams
                   (activity_id, timestamp_s, lat, lon, altitude_ft,
                    heart_rate, cadence, pace_s_per_mi, distance_mi, source_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [(activity_id, st.timestamp_s, st.lat, st.lon, st.altitude_ft,
                  st.heart_rate, st.cadence, st.pace_s_per_mi, st.distance_mi,
                  source_id)
                 for st in parsed.streams],
            )

        # 4. Insert laps/intervals
        for lap in parsed.laps:
            cursor.execute(
                """INSERT INTO intervals
                   (activity_id, rep_number, gps_measured_distance_mi, duration_s,
                    avg_pace_s_per_mi, avg_pace_display, avg_hr, avg_cadence, is_recovery,
                    start_timestamp_s, end_timestamp_s, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (activity_id, lap.rep_number, lap.gps_measured_distance_mi, lap.duration_s,
                 lap.avg_pace_s_per_mi, lap.avg_pace_display, lap.avg_hr, lap.avg_cadence,
                 lap.is_recovery, lap.start_timestamp_s, lap.end_timestamp_s, lap.source),
            )

        # 5. Record processed file
        cursor.execute(
            """INSERT INTO processed_files
               (file_path, file_hash, source, activity_id)
               VALUES (?, ?, ?, ?)""",
            (str(file_path), parsed.file_hash, "healthfit", activity_id),
        )

        conn.commit()

        if verbose:
            stream_count = len(parsed.streams)
            lap_count = len(parsed.laps)
            print(f"  NEW   {file_path.name} → activity {activity_id} "
                  f"({a.date}, {a.distance_mi:.2f}mi @ {a.avg_pace_display}/mi, "
                  f"{stream_count} streams, {lap_count} laps)")

        return activity_id

    except Exception:
        conn.rollback()
        raise
