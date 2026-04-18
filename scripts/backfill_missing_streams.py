#!/usr/bin/env python3
"""Backfill missing streams and laps for promoted orphan activities.

Finds activities that have a Strava source but no streams (typically from
backfill_orphan_streams() silently failing on rate limits), re-fetches
streams + laps from the Strava API, then enriches each activity.

Usage:
    python scripts/backfill_missing_streams.py -v              # fetch all missing
    python scripts/backfill_missing_streams.py --dry-run -v    # preview only
    python scripts/backfill_missing_streams.py --activity 1168 -v  # one activity
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runbase.config import load_config
from runbase.db import get_connection


def find_missing(conn, activity_id=None):
    """Find activities with a Strava source but no streams.

    Returns list of (strava_id, activity_id, source_id) tuples.
    """
    sql = """
        SELECT s.source_id, s.activity_id, s.id
        FROM activity_sources s
        LEFT JOIN streams st ON st.activity_id = s.activity_id
        WHERE s.source = 'strava'
          AND s.activity_id IS NOT NULL
          AND st.id IS NULL
    """
    params = []
    if activity_id:
        sql += " AND s.activity_id = ?"
        params.append(activity_id)
    sql += " ORDER BY s.activity_id"
    rows = conn.execute(sql, params).fetchall()
    return [(int(r[0]), r[1], r[2]) for r in rows]


def main():
    parser = argparse.ArgumentParser(
        description="Backfill missing streams/laps for activities with Strava sources")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true",
                        help="List affected activities without fetching")
    parser.add_argument("--activity", type=int, metavar="ID",
                        help="Target a single activity ID")
    args = parser.parse_args()

    config = load_config()
    conn = get_connection(config)

    pairs = find_missing(conn, args.activity)
    print(f"Found {len(pairs)} activities with Strava source but no streams")

    if not pairs:
        return

    if args.verbose:
        for strava_id, act_id, src_id in pairs:
            row = conn.execute(
                "SELECT workout_name FROM activities WHERE id = ?", (act_id,)
            ).fetchone()
            name = row[0] if row and row[0] else "?"
            print(f"  activity #{act_id} (strava:{strava_id}, source:{src_id}) — {name}")

    if args.dry_run:
        print("Dry run — no API calls made.")
        return

    # Lazy imports to keep startup fast
    from runbase.ingest.strava_sync import backfill_orphan_streams
    from runbase.analysis.interval_enricher import enrich_activity

    print(f"\nFetching streams + laps from Strava API...")
    result = backfill_orphan_streams(config, conn, pairs, verbose=args.verbose)

    print(f"\nBackfill results:")
    print(f"  Streams inserted: {result['streams_inserted']}")
    print(f"  Laps inserted:    {result['laps_inserted']}")
    print(f"  Errors:           {result['errors']}")
    print(f"  Rate limit pauses:{result['rate_limit_pauses']}")

    # Enrich activities that gained new streams
    enriched = 0
    for strava_id, act_id, src_id in pairs:
        has_streams = conn.execute(
            "SELECT 1 FROM streams WHERE activity_id = ? LIMIT 1", (act_id,)
        ).fetchone()
        if has_streams:
            if args.verbose:
                print(f"  Enriching activity #{act_id}...")
            try:
                enrich_activity(conn, act_id, config, verbose=args.verbose)
                enriched += 1
            except Exception as e:
                print(f"  ERROR enriching activity #{act_id}: {e}")

    print(f"\nEnriched {enriched} activities.")
    conn.close()


if __name__ == "__main__":
    main()
