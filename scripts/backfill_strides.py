#!/usr/bin/env python3
"""One-time backfill: populate activities.strides from XLSX for activities that
are missing strides data (e.g. promoted Strava orphans, group-matched splits,
post-cutoff activities).

The earlier backfill_xlsx_fields.py only matched activities that had a
'master_xlsx' source.  This script uses a looser date-based match so it can
fill strides for activities that came from Strava/FIT alone.

Usage:
    python scripts/backfill_strides.py [-v] [--dry-run]
"""

import argparse
import re
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runbase.config import load_config
from runbase.db import get_connection
from runbase.ingest.xlsx_import import (
    _classify_row,
    _normalize_date,
    _parse_strides,
    _read_xlsx,
)


def main():
    parser = argparse.ArgumentParser(
        description="Backfill strides from XLSX for activities missing them"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without writing to DB")
    args = parser.parse_args()

    config = load_config()
    conn = get_connection(config)
    conn.execute("PRAGMA busy_timeout = 30000")

    # ── Read XLSX ──────────────────────────────────────────────────────
    xlsx_path = str(Path(config["paths"]["xlsx_import"]).expanduser())
    if not Path(xlsx_path).exists():
        print(f"ERROR: XLSX not found at {xlsx_path}")
        sys.exit(1)

    raw_rows = _read_xlsx(xlsx_path)
    if args.verbose:
        print(f"Read {len(raw_rows)} data rows from XLSX")

    # ── Build date → strides map from XLSX ─────────────────────────────
    # Each entry: (date_str, xlsx_distance_mi, strides)
    xlsx_strides: list[tuple[str, float | None, int]] = []

    for raw in raw_rows:
        row_type = _classify_row(raw)
        if row_type is None:
            continue

        cn = raw.get("cardio_note")
        cn_str = str(cn).strip() if cn else None
        if cn_str in ("", "None"):
            cn_str = None

        wt = raw.get("workout_title")
        wt_str = str(wt).strip() if wt else None
        if wt_str in ("", "None"):
            wt_str = None

        strides = _parse_strides(cn_str, wt_str)
        if not strides or strides <= 0:
            continue

        date_str = _normalize_date(raw["date"])

        # Extract distance
        cardio = raw["cardio"]
        distance_mi = None
        if row_type == "numeric":
            try:
                distance_mi = round(float(str(cardio).strip()), 2)
            except (ValueError, TypeError):
                pass
        elif row_type == "text":
            m = re.search(r"(\d+\.?\d*)\s*miles?", str(cardio), re.IGNORECASE)
            if m:
                distance_mi = round(float(m.group(1)), 2)

        xlsx_strides.append((date_str, distance_mi, strides))

    if args.verbose:
        print(f"XLSX rows with strides > 0: {len(xlsx_strides)}")

    # ── Match and update ───────────────────────────────────────────────
    updated = 0
    skipped_has_strides = 0
    no_match = 0

    for date_str, xlsx_dist, strides in xlsx_strides:
        # Find all activities on this date that are missing strides
        rows = conn.execute(
            """SELECT id, distance_mi, strides
               FROM activities
               WHERE date = ?
               ORDER BY distance_mi DESC""",
            (date_str,),
        ).fetchall()

        if not rows:
            no_match += 1
            if args.verbose:
                print(f"  NO MATCH   {date_str}  xlsx_dist={xlsx_dist}  strides={strides}")
            continue

        # Check if the largest activity already has strides
        # (the largest is the main run; smaller ones are warmup/cooldown)
        largest = rows[0]
        if largest[2] and largest[2] > 0:
            skipped_has_strides += 1
            continue

        # Verify distance sanity: sum of all same-day activities should be
        # within 20% of XLSX distance (generous to account for GPS vs manual)
        if xlsx_dist is not None:
            db_total = sum(r[1] or 0 for r in rows)
            if db_total > 0 and abs(db_total - xlsx_dist) / xlsx_dist > 0.20:
                if args.verbose:
                    print(f"  DIST SKIP  {date_str}  xlsx={xlsx_dist}  "
                          f"db_total={db_total:.2f}  strides={strides}")
                no_match += 1
                continue

        # Assign strides to the largest activity on that date
        target_id = largest[0]
        if args.verbose:
            print(f"  UPDATE     id={target_id}  {date_str}  "
                  f"dist={largest[1]}  strides={strides}")

        if not args.dry_run:
            conn.execute(
                """UPDATE activities
                   SET strides = ?, updated_at = datetime('now')
                   WHERE id = ?""",
                (strides, target_id),
            )
        updated += 1

    if not args.dry_run:
        conn.commit()
    conn.close()

    label = "Would update" if args.dry_run else "Updated"
    print(f"\nBackfill strides {'(dry run)' if args.dry_run else ''}:")
    print(f"  XLSX rows with strides: {len(xlsx_strides)}")
    print(f"  {label}:               {updated}")
    print(f"  Already had strides:    {skipped_has_strides}")
    print(f"  No match / dist skip:   {no_match}")


if __name__ == "__main__":
    main()
