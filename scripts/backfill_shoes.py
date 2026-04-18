#!/usr/bin/env python3
"""Backfill missing shoe_id on activities using two strategies:

1. Same-date inheritance: if another activity on the same date has a shoe, inherit it.
2. Orphaned XLSX match: orphaned XLSX sources (activity_id=NULL from group-match split)
   are matched to unshoed dates by summed distance, and their xlsx_shoe_id is applied.

Usage:
    python scripts/backfill_shoes.py -v              # apply fixes
    python scripts/backfill_shoes.py --dry-run -v    # preview only
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runbase.config import load_config
from runbase.db import get_connection

# XLSX shoe_id → shoes table id mapping
XLSX_TO_SHOE = {1: 10, 2: 8, 3: 9, 4: 11, 5: 6, 6: 3, 7: 7, 8: 4, 9: 2, 10: 1}


def backfill_same_date(conn, verbose=False, dry_run=False):
    """For unshoed activities, inherit shoe from a shoed activity on the same date."""
    rows = conn.execute("""
        SELECT a.id, a.date, COALESCE(a.adjusted_distance_mi, a.distance_mi) as mi,
               a.workout_name, a2.shoe_id
        FROM activities a
        JOIN activities a2 ON a2.date = a.date AND a2.id != a.id AND a2.shoe_id IS NOT NULL
        WHERE a.shoe_id IS NULL
        GROUP BY a.id
    """).fetchall()

    if verbose:
        print(f"\n--- Same-date inheritance: {len(rows)} activities ---")
    updated = 0
    for act_id, date, mi, name, shoe_id in rows:
        if verbose:
            shoe = conn.execute("SELECT name FROM shoes WHERE id = ?", (shoe_id,)).fetchone()
            print(f"  #{act_id} {date} {mi:.1f}mi → shoe#{shoe_id} ({shoe[0]}) — {name}")
        if not dry_run:
            conn.execute("UPDATE activities SET shoe_id = ? WHERE id = ?", (shoe_id, act_id))
            updated += 1
    if not dry_run:
        conn.commit()
    return updated


def backfill_orphaned_xlsx(conn, verbose=False, dry_run=False):
    """Match orphaned XLSX sources to unshoed dates by distance, apply xlsx_shoe_id."""
    # Get all dates where ALL activities are unshoed
    unshoed_dates = conn.execute("""
        SELECT a.date,
               COUNT(*) as n_acts,
               ROUND(SUM(COALESCE(a.adjusted_distance_mi, a.distance_mi, 0)), 2) as total_mi,
               GROUP_CONCAT(a.id) as act_ids
        FROM activities a
        WHERE a.shoe_id IS NULL
          AND a.date >= '2023-11-01' AND a.date <= '2025-12-31'
          AND NOT EXISTS (
              SELECT 1 FROM activities a2
              WHERE a2.date = a.date AND a2.shoe_id IS NOT NULL
          )
        GROUP BY a.date
        ORDER BY a.date
    """).fetchall()

    # Get orphaned XLSX sources (sorted by row_number for stable matching)
    orphaned = conn.execute("""
        SELECT s.id, s.distance_mi, s.workout_name,
               json_extract(s.metadata_json, '$.xlsx_shoe_id') as xsid,
               json_extract(s.metadata_json, '$.row_number') as rownum
        FROM activity_sources s
        WHERE s.source = 'master_xlsx' AND s.activity_id IS NULL
        ORDER BY CAST(json_extract(s.metadata_json, '$.row_number') AS INTEGER)
    """).fetchall()

    if verbose:
        print(f"\n--- Orphaned XLSX match: {len(unshoed_dates)} unshoed dates, "
              f"{len(orphaned)} orphaned XLSX sources ---")

    # Match each unshoed date to the closest orphaned XLSX source by distance (±15%)
    used_sources = set()
    matches = []
    for date, n_acts, total_mi, act_ids in unshoed_dates:
        best = None
        best_diff = float('inf')
        for src_id, src_mi, src_name, xsid, rownum in orphaned:
            if src_id in used_sources or not src_mi or not xsid:
                continue
            diff = abs(total_mi - src_mi) / max(src_mi, 0.01)
            if diff < 0.15 and diff < best_diff:
                best = (src_id, src_mi, src_name, int(xsid))
                best_diff = diff
        if best:
            matches.append((date, act_ids.split(','), total_mi, best))
            used_sources.add(best[0])

    updated = 0
    for date, act_ids, total_mi, (src_id, src_mi, src_name, xsid) in matches:
        shoe_id = XLSX_TO_SHOE.get(xsid)
        if not shoe_id:
            continue
        if verbose:
            shoe = conn.execute("SELECT name FROM shoes WHERE id = ?", (shoe_id,)).fetchone()
            print(f"  {date} [{len(act_ids)} acts, {total_mi:.1f}mi] ← xlsx src#{src_id} "
                  f"{src_mi:.1f}mi shoe#{shoe_id} ({shoe[0]}) — {src_name}")
        if not dry_run:
            for act_id in act_ids:
                conn.execute("UPDATE activities SET shoe_id = ? WHERE id = ?",
                             (shoe_id, int(act_id)))
            updated += len(act_ids)
    if not dry_run:
        conn.commit()

    unmatched = len(unshoed_dates) - len(matches)
    if unmatched and verbose:
        print(f"  {unmatched} date(s) could not be matched")

    return updated


def main():
    parser = argparse.ArgumentParser(description="Backfill missing shoe_id on activities")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    config = load_config()
    conn = get_connection(config)

    # Strategy 1: orphaned XLSX match (do this first — covers fully-unshoed dates)
    n_xlsx = backfill_orphaned_xlsx(conn, verbose=args.verbose, dry_run=args.dry_run)

    # Strategy 2: same-date inheritance (covers remaining partial-day gaps)
    n_same = backfill_same_date(conn, verbose=args.verbose, dry_run=args.dry_run)

    total = n_xlsx + n_same
    action = "Would update" if args.dry_run else "Updated"
    print(f"\n{action} {total} activities ({n_xlsx} from XLSX match, {n_same} from same-date)")

    if not args.dry_run:
        # Show new shoe totals
        print("\nUpdated shoe mileages:")
        rows = conn.execute("""
            SELECT s.id, s.name,
                   ROUND(COALESCE(SUM(COALESCE(a.adjusted_distance_mi, a.distance_mi, 0)), 0), 1) as miles,
                   COUNT(a.id) as cnt
            FROM shoes s
            LEFT JOIN activities a ON a.shoe_id = s.id
            GROUP BY s.id
            ORDER BY s.id
        """).fetchall()
        for r in rows:
            print(f"  shoe#{r[0]} {r[1]:30} {r[3]:>4} activities  {r[2]:>7} mi")

    conn.close()


if __name__ == "__main__":
    main()
