#!/usr/bin/env python3
"""Audit stride-based VDOT defaults; --apply backs up the DB before updating flags."""

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runbase.analysis.vdot_eligibility import stride_distance_fraction, stride_exclusion_decision
from runbase.config import load_config
from runbase.db import _migrate_schema


def pending_changes(conn):
    by_activity = defaultdict(list)
    for row in conn.execute(
        "SELECT activity_id, source, lap_kind, canonical_distance_mi, gps_measured_distance_mi, "
        "prescribed_distance_mi, is_stride, is_walking FROM intervals"
    ):
        by_activity[row["activity_id"]].append(dict(row))
    changes = []
    for row in conn.execute("SELECT * FROM activities ORDER BY date, id"):
        activity = dict(row)
        intervals = by_activity[activity["id"]]
        excluded, source = stride_exclusion_decision(activity, intervals)
        if (excluded, source) == (activity.get("exclude_from_vdot"), activity.get("vdot_exclusion_source")):
            continue
        changes.append({
            "id": activity["id"], "date": activity["date"],
            "stride_fraction": stride_distance_fraction(activity["distance_mi"], intervals),
            "old_excluded": activity.get("exclude_from_vdot"),
            "old_source": activity.get("vdot_exclusion_source"),
            "excluded": excluded, "source": source,
        })
    return changes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    db = Path(load_config()["paths"]["db"]).resolve()
    conn = sqlite3.connect(str(db) if args.apply else db.as_uri() + "?mode=ro", uri=not args.apply)
    conn.row_factory = sqlite3.Row
    try:
        changes = pending_changes(conn)
        if not args.apply:
            for change in changes:
                print(f"{change['date']} #{change['id']}: {change['stride_fraction']:.1%} strides; excluded={change['excluded']}")
            print(f"{len(changes)} activities would change. No database changes made.")
            return
        if not changes:
            print("Stride-based VDOT defaults are already current.")
            return
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup = db.parent / "data" / "backups" / f"data-before-stride-vdot-defaults-{stamp}.db"
        backup.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(backup) as destination:
            conn.backup(destination)
        _migrate_schema(conn)
        with conn:
            conn.execute("BEGIN IMMEDIATE")
            # Recheck explicit choices in case they changed since the audit.
            changes = pending_changes(conn)
            for change in changes:
                conn.execute(
                    "UPDATE activities SET exclude_from_vdot=?, vdot_exclusion_source=? WHERE id=?",
                    (change["excluded"], change["source"], change["id"]),
                )
        audit = backup.with_suffix(".json")
        audit.write_text(json.dumps({"backup": str(backup), "changes": changes}, indent=2))
        print(f"Updated {len(changes)} activities. Backup: {backup}; audit: {audit}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
