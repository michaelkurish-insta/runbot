#!/usr/bin/env python3
"""Backfill weather data (temperature, humidity, conditions) on activities.

Fetches historical weather from the free Open-Meteo API using GPS coordinates
sampled at 2/3 through each activity's duration.

Usage:
    python scripts/backfill_weather.py -v              # backfill all missing
    python scripts/backfill_weather.py --activity 123 -v  # single activity
    python scripts/backfill_weather.py --force -v      # re-fetch even if set
    python scripts/backfill_weather.py --dry-run -v    # preview only
"""

import argparse
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runbase.config import load_config
from runbase.db import get_connection
from runbase.analysis.weather import fetch_weather


def backfill_weather(conn, verbose=False, dry_run=False, activity_id=None,
                     force=False):
    """Fetch and store weather for activities missing weather data."""
    where_clauses = [
        "a.start_time IS NOT NULL",
        "a.duration_s > 0",
    ]
    params = []

    if activity_id:
        where_clauses.append("a.id = ?")
        params.append(activity_id)

    if not force:
        where_clauses.append("a.temperature_f IS NULL")

    # Only consider activities that have GPS streams
    where = " AND ".join(where_clauses)
    query = f"""
        SELECT DISTINCT a.id, a.date, a.start_time, a.duration_s,
               a.workout_name, a.temperature_f
        FROM activities a
        JOIN streams s ON s.activity_id = a.id AND s.lat IS NOT NULL
        WHERE {where}
        ORDER BY a.date
    """
    rows = conn.execute(query, params).fetchall()

    if verbose:
        print(f"Found {len(rows)} activities to process")

    updated = 0
    skipped = 0
    failed = 0

    for i, (act_id, date, start_time, duration_s, name, cur_temp) in enumerate(rows):
        if verbose:
            label = name or "unnamed"
            status = f"[{i+1}/{len(rows)}]"
            print(f"  {status} #{act_id} {date} — {label}")

        # Parse start_time
        try:
            start_dt = datetime.fromisoformat(start_time)
        except (ValueError, TypeError):
            if verbose:
                print(f"    Skipped: invalid start_time '{start_time}'")
            skipped += 1
            continue

        # Compute target time at 2/3 through the run
        target_offset = duration_s * 2 / 3
        target_ts_offset = target_offset  # seconds from first stream point

        # Get GPS coords from streams at closest timestamp to target
        stream_rows = conn.execute(
            """SELECT timestamp_s, lat, lon FROM streams
               WHERE activity_id = ? AND lat IS NOT NULL
               ORDER BY timestamp_s""",
            (act_id,),
        ).fetchall()

        if not stream_rows:
            if verbose:
                print("    Skipped: no GPS streams")
            skipped += 1
            continue

        first_ts = stream_rows[0][0] or 0
        target_ts = first_ts + target_ts_offset

        best_pt = None
        best_diff = float("inf")
        for ts, lat, lon in stream_rows:
            if ts is None:
                continue
            diff = abs(ts - target_ts)
            if diff < best_diff:
                best_diff = diff
                best_pt = (lat, lon)

        if not best_pt:
            if verbose:
                print("    Skipped: no valid GPS point found")
            skipped += 1
            continue

        target_dt = start_dt + timedelta(seconds=target_offset)

        if dry_run:
            if verbose:
                print(f"    Would fetch: ({best_pt[0]:.4f}, {best_pt[1]:.4f}) "
                      f"at {target_dt.strftime('%H:%M')}")
            updated += 1
            continue

        weather = fetch_weather(best_pt[0], best_pt[1], target_dt, verbose=verbose)

        if weather:
            conn.execute(
                """UPDATE activities
                   SET temperature_f = ?, humidity_pct = ?, weather_conditions = ?,
                       updated_at = datetime('now')
                   WHERE id = ?""",
                (weather["temperature_f"], weather["humidity_pct"],
                 weather["weather_conditions"], act_id),
            )
            conn.commit()
            updated += 1
        else:
            failed += 1

        # Rate limit: 0.15s between calls (~400/min, under Open-Meteo's 600/min)
        if i < len(rows) - 1:
            time.sleep(0.15)

    return updated, skipped, failed


def main():
    parser = argparse.ArgumentParser(
        description="Backfill weather data on activities")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without writing")
    parser.add_argument("--activity", type=int,
                        help="Process a single activity ID")
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch even if weather already set")
    args = parser.parse_args()

    config = load_config()
    conn = get_connection(config)

    # Ensure weather columns exist
    from runbase.db import _migrate_schema
    _migrate_schema(conn)

    updated, skipped, failed = backfill_weather(
        conn, verbose=args.verbose, dry_run=args.dry_run,
        activity_id=args.activity, force=args.force,
    )

    action = "Would update" if args.dry_run else "Updated"
    print(f"\n{action} {updated} activities "
          f"(skipped {skipped}, failed {failed})")

    if not args.dry_run and updated:
        # Show sample of results
        rows = conn.execute("""
            SELECT id, date, temperature_f, humidity_pct, weather_conditions
            FROM activities
            WHERE temperature_f IS NOT NULL
            ORDER BY date DESC
            LIMIT 5
        """).fetchall()
        print("\nRecent weather data:")
        for act_id, date, temp, humid, cond in rows:
            h = f"{humid:.0f}%" if humid else "—"
            print(f"  #{act_id} {date}: {temp:.0f}°F  {h}  {cond}")

    conn.close()


if __name__ == "__main__":
    main()
