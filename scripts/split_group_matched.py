#!/usr/bin/env python3
"""One-time migration: split group-matched activities into separate rows.

Group-matched activities store multiple Strava sub-activities as a single
activities row. This script splits each into individual activity rows so
the review UI can display and edit them independently. The UI's _merge_day()
handles same-day display merging.

Usage:
    python scripts/split_group_matched.py -v              # run migration
    python scripts/split_group_matched.py --dry-run -v    # preview only
    python scripts/split_group_matched.py --skip-fetch -v # skip Strava API calls
"""

import argparse
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runbase.config import load_config
from runbase.db import get_connection
from runbase.ingest.fit_parser import format_pace
from runbase.reconcile.enricher import _lookup_shoe_id, _infer_category, _map_workout_type


def find_group_matched_activities(conn, verbose=False):
    """Find activities with >1 Strava source in activity_sources."""
    rows = conn.execute("""
        SELECT a.id, a.date, a.distance_mi, a.workout_name,
               COUNT(s.id) as source_count
        FROM activities a
        JOIN activity_sources s ON s.activity_id = a.id AND s.source = 'strava'
        GROUP BY a.id
        HAVING COUNT(s.id) > 1
        ORDER BY a.date
    """).fetchall()

    targets = []
    for r in rows:
        targets.append({
            "activity_id": r[0],
            "date": r[1],
            "distance_mi": r[2],
            "workout_name": r[3],
            "source_count": r[4],
        })

    if verbose:
        print(f"Found {len(targets)} group-matched activities to split.")
    return targets


def load_sources_for_activity(conn, activity_id):
    """Load all Strava activity_sources for an activity, sorted by start_time."""
    rows = conn.execute("""
        SELECT id, source_id, distance_mi, duration_s,
               avg_pace_s_per_mi, avg_hr, max_hr, avg_cadence,
               total_ascent_ft, calories, workout_name, metadata_json
        FROM activity_sources
        WHERE activity_id = ? AND source = 'strava'
        ORDER BY json_extract(metadata_json, '$.start_time')
    """, (activity_id,)).fetchall()

    sources = []
    for r in rows:
        meta = json.loads(r[11]) if r[11] else {}
        sources.append({
            "id": r[0],
            "source_id": r[1],
            "distance_mi": r[2],
            "duration_s": r[3],
            "avg_pace_s_per_mi": r[4],
            "avg_hr": r[5],
            "max_hr": r[6],
            "avg_cadence": r[7],
            "total_ascent_ft": r[8],
            "calories": r[9],
            "workout_name": r[10],
            "metadata": meta,
            "start_date": meta.get("start_date"),
            "start_time": meta.get("start_time"),
            "strava_name": meta.get("strava_name"),
            "gear_id": meta.get("gear_id"),
            "workout_type": meta.get("workout_type"),
        })
    return sources


def split_activity(conn, activity_id, sources, verbose=False):
    """Split one group-matched activity into individual activities.

    Returns list of (strava_id, new_activity_id, source_id) tuples for fetch.
    """
    pairs = []

    for src in sources:
        # Compute fields for the new activity
        dist = src["distance_mi"]
        dur = src["duration_s"]
        avg_pace = dur / dist if dist and dist > 0 and dur else None
        avg_pace_display = format_pace(avg_pace) if avg_pace else None

        workout_name = src["strava_name"] or src["workout_name"]
        workout_type = _map_workout_type(src["workout_type"])
        # Build a source-like dict for _infer_category
        cat_source = {
            "metadata": src["metadata"],
            "strava_name": src["strava_name"],
            "workout_name": src["workout_name"],
        }
        workout_category = _infer_category(cat_source)
        shoe_id = _lookup_shoe_id(conn, src["gear_id"])
        date = src["start_date"] or src["metadata"].get("start_date")
        start_time = src["start_time"]

        # Insert new activity
        cursor = conn.execute(
            """INSERT INTO activities
               (date, start_time, distance_mi, duration_s,
                avg_pace_s_per_mi, avg_pace_display,
                avg_hr, max_hr, avg_cadence, total_ascent_ft, calories,
                workout_name, workout_type, workout_category, shoe_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (date, start_time, dist, dur,
             avg_pace, avg_pace_display,
             src["avg_hr"], src["max_hr"], src["avg_cadence"],
             src["total_ascent_ft"] or None, src["calories"] or None,
             workout_name, workout_type, workout_category, shoe_id),
        )
        new_id = cursor.lastrowid

        # Point the activity_source at the new activity
        conn.execute(
            "UPDATE activity_sources SET activity_id = ? WHERE id = ?",
            (new_id, src["id"]),
        )

        # Reassign streams by source_id
        conn.execute(
            "UPDATE streams SET activity_id = ? WHERE activity_id = ? AND source_id = ?",
            (new_id, activity_id, src["id"]),
        )

        pairs.append((src["source_id"], new_id, src["id"]))

        if verbose:
            print(f"    → activity #{new_id}: {dist:.2f}mi "
                  f"\"{workout_name or '?'}\" (source #{src['id']})")

    # Unlink non-Strava sources (e.g. master_xlsx) — orphan them rather than
    # leaving them pointing at an activity_id that's about to be deleted
    conn.execute(
        "UPDATE activity_sources SET activity_id = NULL "
        "WHERE activity_id = ? AND source != 'strava'",
        (activity_id,),
    )

    # Reassign detected_tracks to the new activity with the most streams
    # (the sub-activity whose GPS data was used for track detection)
    if pairs:
        best_new_id = pairs[0][1]  # default to first
        best_count = 0
        for _, new_id, _ in pairs:
            cnt = conn.execute(
                "SELECT COUNT(*) FROM streams WHERE activity_id = ?", (new_id,)
            ).fetchone()[0]
            if cnt > best_count:
                best_count = cnt
                best_new_id = new_id
        conn.execute(
            "UPDATE detected_tracks SET detected_by_activity_id = ? "
            "WHERE detected_by_activity_id = ?",
            (best_new_id, activity_id),
        )

    # Delete old intervals (interleaved from different sub-activities)
    deleted_intervals = conn.execute(
        "DELETE FROM intervals WHERE activity_id = ?", (activity_id,)
    ).rowcount

    # Delete old activity row (no longer has sources)
    conn.execute("DELETE FROM activities WHERE id = ?", (activity_id,))

    if verbose and deleted_intervals:
        print(f"    deleted {deleted_intervals} old intervals from activity #{activity_id}")

    return pairs


def main():
    parser = argparse.ArgumentParser(
        description="Split group-matched activities into separate rows")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without writing to DB")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="Skip Strava API calls (laps + streams)")
    args = parser.parse_args()

    config = load_config()
    conn = get_connection(config)
    conn.execute("PRAGMA busy_timeout = 30000")

    targets = find_group_matched_activities(conn, verbose=args.verbose)
    if not targets:
        print("No group-matched activities found.")
        conn.close()
        return

    all_pairs = []
    new_activity_ids = []
    split_count = 0
    new_count = 0

    for t in targets:
        activity_id = t["activity_id"]
        sources = load_sources_for_activity(conn, activity_id)

        if args.verbose or args.dry_run:
            dists = [f"{s['distance_mi']:.2f}" for s in sources]
            names = [s.get("strava_name") or s.get("workout_name") or "?" for s in sources]
            print(f"{'[DRY RUN] ' if args.dry_run else ''}"
                  f"SPLIT activity #{activity_id} ({t['date']}, {t['distance_mi']:.2f}mi) "
                  f"→ {len(sources)} activities: {' + '.join(dists)}mi "
                  f"({', '.join(names)})")

        if args.dry_run:
            split_count += 1
            new_count += len(sources)
            continue

        pairs = split_activity(conn, activity_id, sources, verbose=args.verbose)
        conn.commit()

        all_pairs.extend(pairs)
        new_activity_ids.extend(p[1] for p in pairs)
        split_count += 1
        new_count += len(pairs)

    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}"
          f"Split {split_count} activities → {new_count} new activities")

    if args.dry_run:
        conn.close()
        return

    # Fetch Strava laps + streams for new activities
    if not args.skip_fetch and all_pairs:
        from runbase.ingest.strava_sync import backfill_orphan_streams

        print(f"\nFetching streams/laps for {len(all_pairs)} new activities...")
        fetch_result = backfill_orphan_streams(
            config, conn, all_pairs, verbose=args.verbose)
        print(f"  Streams: {fetch_result['streams_inserted']} points")
        print(f"  Laps: {fetch_result['laps_inserted']} intervals")
        if fetch_result["errors"]:
            print(f"  Errors: {fetch_result['errors']}")

    # Re-enrich each new activity
    if new_activity_ids:
        from runbase.analysis.interval_enricher import enrich_activity

        print(f"\nEnriching {len(new_activity_ids)} new activities...")
        enriched = 0
        for aid in new_activity_ids:
            try:
                result = enrich_activity(conn, aid, config, verbose=args.verbose)
                if not result["skipped"]:
                    enriched += 1
            except Exception as e:
                if args.verbose:
                    print(f"  ERROR enriching activity #{aid}: {e}")
        print(f"  Enriched: {enriched}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
