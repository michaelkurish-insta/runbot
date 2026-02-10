#!/usr/bin/env python3
"""One-time migration: backfill strides, workout_category, and fix workout_name
for existing XLSX-imported activities.

Usage:
    python scripts/backfill_xlsx_fields.py [-v]
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runbase.config import load_config
from runbase.db import get_connection
from runbase.ingest.xlsx_import import (
    _read_xlsx,
    _classify_row,
    _normalize_date,
    _parse_strides,
    _parse_workout_category,
)


def main():
    parser = argparse.ArgumentParser(description="Backfill strides + workout_category from XLSX")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    config = load_config()
    conn = get_connection(config)
    conn.execute("PRAGMA busy_timeout = 30000")  # wait up to 30s for locks

    # Step 1: ALTER TABLE to add new columns (idempotent)
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(activities)").fetchall()}

    if "strides" not in existing_cols:
        conn.execute("ALTER TABLE activities ADD COLUMN strides INTEGER")
        if args.verbose:
            print("Added column: strides")

    if "workout_category" not in existing_cols:
        conn.execute("ALTER TABLE activities ADD COLUMN workout_category TEXT")
        if args.verbose:
            print("Added column: workout_category")

    conn.commit()

    # Step 2: Read the XLSX
    xlsx_path = str(Path(config["paths"]["xlsx_import"]).expanduser())
    if not Path(xlsx_path).exists():
        print(f"ERROR: XLSX not found at {xlsx_path}")
        sys.exit(1)

    raw_rows = _read_xlsx(xlsx_path)
    if args.verbose:
        print(f"Read {len(raw_rows)} data rows from XLSX")

    # Step 3: Parse and match to existing activities
    updated = 0
    matched = 0
    unmatched = 0
    skipped_non_running = 0

    for raw in raw_rows:
        row_type = _classify_row(raw)
        if row_type is None:
            skipped_non_running += 1
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
            import re
            m = re.search(r'(\d+\.?\d*)\s*miles?', str(cardio), re.IGNORECASE)
            if m:
                distance_mi = round(float(m.group(1)), 2)

        # Parse new fields
        cardio_note = raw.get("cardio_note")
        cn_str = str(cardio_note).strip() if cardio_note else None
        if cn_str == "" or cn_str == "None":
            cn_str = None

        workout_title = raw.get("workout_title")
        wt_str = str(workout_title).strip() if workout_title else None
        if wt_str == "" or wt_str == "None":
            wt_str = None

        strides = _parse_strides(cn_str, wt_str)
        workout_category = _parse_workout_category(cn_str, wt_str)
        if workout_category is None:
            workout_category = "easy"

        # Workout name fix: only use col 10
        workout_name = wt_str

        # Match to DB activity by date + distance via activity_sources
        if distance_mi is not None:
            row_match = conn.execute(
                """SELECT a.id, a.workout_name FROM activities a
                   JOIN activity_sources s ON s.activity_id = a.id
                   WHERE a.date = ? AND s.source = 'master_xlsx'
                         AND ABS(a.distance_mi - ?) < 0.01""",
                (date_str, distance_mi),
            ).fetchone()
        else:
            row_match = conn.execute(
                """SELECT a.id, a.workout_name FROM activities a
                   JOIN activity_sources s ON s.activity_id = a.id
                   WHERE a.date = ? AND s.source = 'master_xlsx'
                         AND a.distance_mi IS NULL""",
                (date_str,),
            ).fetchone()

        if row_match is None:
            unmatched += 1
            if args.verbose:
                print(f"  UNMATCHED  {date_str}  {distance_mi}mi  cn={cn_str}")
            continue

        matched += 1
        activity_id = row_match[0]
        current_workout_name = row_match[1]

        # Determine if workout_name needs fixing:
        # If current name came from col 9 (cardio_note) and col 10 was empty,
        # clear it. We detect this by checking if current_workout_name == cn_str
        # and col 10 was empty.
        fixed_name = workout_name
        if current_workout_name and wt_str is None and current_workout_name == cn_str:
            fixed_name = None  # Was a col 9 fallback — clear it

        conn.execute(
            """UPDATE activities
               SET strides = ?, workout_category = ?, workout_name = ?,
                   updated_at = datetime('now')
               WHERE id = ?""",
            (strides, workout_category, fixed_name, activity_id),
        )
        updated += 1

        if args.verbose and (strides or workout_category != "easy" or fixed_name != current_workout_name):
            print(f"  UPDATED  id={activity_id}  {date_str}  "
                  f"cat={workout_category}  strides={strides}  "
                  f"name={fixed_name!r}")

    # Step 4: Default workout_category='easy' for any remaining NULL rows
    # (could be from FIT/Strava imports that don't have category info)
    remaining = conn.execute(
        "UPDATE activities SET workout_category = 'easy' WHERE workout_category IS NULL"
    ).rowcount

    conn.commit()
    conn.close()

    print(f"\nBackfill complete:")
    print(f"  XLSX rows read:      {len(raw_rows)}")
    print(f"  Skipped non-running: {skipped_non_running}")
    print(f"  Matched to DB:       {matched}")
    print(f"  Updated:             {updated}")
    print(f"  Unmatched:           {unmatched}")
    if remaining:
        print(f"  Defaulted to easy:   {remaining} (non-XLSX activities)")


if __name__ == "__main__":
    main()
