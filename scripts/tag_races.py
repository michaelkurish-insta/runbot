#!/usr/bin/env python3
"""Scan activities for races, compute VDOT from each, populate VDOT timeline.

Consolidates race detection from multiple sources:
1. activities.workout_type = 'race'
2. activities.workout_category = 'race'
3. Strava workout_type=1 (from activity_sources metadata)
4. Name patterns (race, TT, time trial, parkrun)

Usage:
    python scripts/tag_races.py -v              # scan, confirm, compute VDOTs
    python scripts/tag_races.py --dry-run -v    # preview only
    python scripts/tag_races.py --yes -v        # skip confirmation prompt
    python scripts/tag_races.py --re-enrich -v  # also re-enrich all activities
"""

import argparse
import re
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runbase.config import load_config
from runbase.db import get_connection
from runbase.analysis.vdot import race_to_vdot, set_vdot, get_current_vdot, format_pace
from runbase.analysis.interval_enricher import (
    _is_race_name, _parse_race_distance_m, _check_strava_workout_type,
)

METERS_PER_MILE = 1609.344

# Names that disqualify a candidate even if Strava tagged it as race
_NON_RACE_PATTERNS = [
    re.compile(r"\bcool\s*down\b", re.IGNORECASE),
    re.compile(r"\bwarm\s*up\b", re.IGNORECASE),
    re.compile(r"\bshakeout\b", re.IGNORECASE),
    re.compile(r"\beasy\b", re.IGNORECASE),
    re.compile(r"\brecovery\b", re.IGNORECASE),
]


def _is_non_race_name(name: str | None) -> bool:
    """Check if an activity name clearly indicates it is NOT a race."""
    if not name:
        return False
    return any(p.search(name) for p in _NON_RACE_PATTERNS)


def _has_strava_source(conn, activity_id: int) -> bool:
    """Check if the activity has at least one Strava activity_source."""
    row = conn.execute(
        "SELECT COUNT(*) FROM activity_sources WHERE activity_id = ? AND source = 'strava'",
        (activity_id,),
    ).fetchone()
    return row[0] > 0


def find_race_candidates(conn, verbose=False):
    """Scan all activities and build a list of race candidates with reasons.

    Filters:
    - Must have a Strava source (XLSX-only entries have day-total distance, not race distance)
    - Must have duration_s (can't compute VDOT without timing)
    - Must not have a non-race name (cooldown, warmup, etc.)
    - Must have either name_match or strava_workout_type=1 (just workout_category=race
      from XLSX is not enough — those entries often have the full day's distance)
    """
    rows = conn.execute(
        """SELECT a.id, a.date, a.distance_mi, a.adjusted_distance_mi,
                  a.duration_s, a.workout_name, a.workout_type,
                  a.workout_category, a.avg_pace_s_per_mi
           FROM activities a
           ORDER BY a.date""",
    ).fetchall()

    candidates = []
    for r in rows:
        activity_id = r[0]
        name = r[5]
        duration_s = r[4]

        # Must have Strava source and timing
        if not _has_strava_source(conn, activity_id):
            continue
        if not duration_s or duration_s <= 0:
            continue

        # Exclude cooldown/warmup names
        if _is_non_race_name(name):
            if verbose:
                print(f"  Skipping #{activity_id} ({r[1]}): non-race name '{name}'")
            continue

        reasons = []

        if r[6] == "race":  # workout_type
            reasons.append("workout_type=race")
        if r[7] == "race":  # workout_category
            reasons.append("workout_category=race")

        strava_wt = _check_strava_workout_type(conn, activity_id)
        if strava_wt == 1:
            reasons.append("strava_workout_type=1")

        if _is_race_name(name):
            reasons.append("name_match")

        if not reasons:
            continue

        # Require a strong signal: name match or Strava race tag.
        # workout_category=race alone (from XLSX) is not enough — those entries
        # often have the full day's distance, not just the race.
        has_strong_signal = "name_match" in reasons or "strava_workout_type=1" in reasons
        if not has_strong_signal:
            if verbose:
                print(f"  Skipping #{activity_id} ({r[1]}): only weak signal ({', '.join(reasons)})")
            continue

        candidates.append({
            "id": activity_id,
            "date": r[1],
            "distance_mi": r[2],
            "adjusted_distance_mi": r[3],
            "duration_s": duration_s,
            "workout_name": name,
            "workout_type": r[6],
            "workout_category": r[7],
            "avg_pace": r[8],
            "reasons": reasons,
        })

    return candidates


def print_candidates(candidates):
    """Print a table of race candidates for user review."""
    if not candidates:
        print("No race candidates found.")
        return

    print(f"\n{'ID':>5}  {'Date':10}  {'Dist':>6}  {'Duration':>8}  {'Pace':>6}  {'Name':30}  Reasons")
    print("-" * 100)
    for c in candidates:
        dist = c["adjusted_distance_mi"] or c["distance_mi"] or 0
        dur_s = c["duration_s"] or 0
        if dur_s >= 3600:
            h, rem = divmod(int(dur_s), 3600)
            m, s = divmod(rem, 60)
            dur_str = f"{h}:{m:02d}:{s:02d}"
        elif dur_s > 0:
            m, s = divmod(int(dur_s), 60)
            dur_str = f"{m}:{s:02d}"
        else:
            dur_str = ""
        pace_str = format_pace(c["avg_pace"]) if c["avg_pace"] else ""
        name = (c["workout_name"] or "")[:30]
        reasons_str = ", ".join(c["reasons"])
        print(f"{c['id']:>5}  {c['date']:10}  {dist:>6.2f}  {dur_str:>8}  {pace_str:>6}  {name:30}  {reasons_str}")


def compute_race_vdot(candidate):
    """Compute VDOT for a race candidate.

    Distance priority: parsed name distance > adjusted_distance_mi > distance_mi
    Returns (vdot, distance_m, source) or (None, None, None) if can't compute.
    """
    name = candidate["workout_name"]
    duration_s = candidate["duration_s"]

    if not duration_s or duration_s <= 0:
        return None, None, None

    # Try parsed name distance first (most accurate for short races)
    parsed_dist_m = _parse_race_distance_m(name)
    if parsed_dist_m:
        vdot = race_to_vdot(parsed_dist_m, duration_s)
        return vdot, parsed_dist_m, "parsed_name"

    # Fall back to adjusted/raw distance
    dist_mi = candidate["adjusted_distance_mi"] or candidate["distance_mi"]
    if not dist_mi or dist_mi <= 0:
        return None, None, None

    dist_m = dist_mi * METERS_PER_MILE
    vdot = race_to_vdot(dist_m, duration_s)
    return vdot, dist_m, "gps_distance"


def tag_and_compute(conn, candidates, dry_run=False, verbose=False):
    """Tag races, compute VDOTs, insert into vdot_history.

    Returns list of (candidate, vdot) tuples for display.
    """
    results = []

    for c in candidates:
        activity_id = c["id"]

        # Tag the activity
        if not dry_run:
            conn.execute(
                """UPDATE activities SET workout_category = 'race', workout_type = 'race',
                          updated_at = datetime('now')
                   WHERE id = ?""",
                (activity_id,),
            )

        # Compute VDOT
        vdot, dist_m, dist_source = compute_race_vdot(c)
        results.append((c, vdot, dist_m, dist_source))

        if vdot is None:
            if verbose:
                print(f"  #{activity_id} ({c['date']}): cannot compute VDOT (missing distance/duration)")
            continue

        # Sanity check
        if vdot < 30 or vdot > 70:
            if verbose:
                print(f"  #{activity_id} ({c['date']}): VDOT {vdot:.1f} outside 30-70 range — skipping vdot_history insert")
            continue

        if verbose:
            dist_label = f"{dist_m:.0f}m" if dist_m < 1609.344 else f"{dist_m / METERS_PER_MILE:.2f}mi"
            print(f"  #{activity_id} ({c['date']}): {c['workout_name'] or '?'} "
                  f"— {dist_label} ({dist_source}) → VDOT {vdot:.1f}")

        if dry_run:
            continue

        # Check idempotency: skip if activity_id already in vdot_history
        existing = conn.execute(
            "SELECT id FROM vdot_history WHERE activity_id = ?",
            (activity_id,),
        ).fetchone()
        if existing:
            if verbose:
                print(f"    (already in vdot_history, skipping)")
            continue

        set_vdot(conn, vdot, c["date"], source="race", activity_id=activity_id,
                 notes=c["workout_name"])

    if not dry_run:
        conn.commit()

    return results


def print_vdot_timeline(conn):
    """Print the full VDOT timeline."""
    rows = conn.execute(
        "SELECT effective_date, vdot, source, activity_id, notes FROM vdot_history ORDER BY effective_date"
    ).fetchall()

    if not rows:
        print("\nNo VDOT history entries.")
        return

    print(f"\n{'Date':12}  {'VDOT':>6}  {'Source':8}  {'Activity':>8}  Notes")
    print("-" * 70)
    for r in rows:
        notes = (r[4] or "")[:30]
        aid = r[3] or ""
        print(f"{r[0]:12}  {r[1]:>6.1f}  {r[2] or '':8}  {str(aid):>8}  {notes}")


def main():
    parser = argparse.ArgumentParser(
        description="Scan activities for races, compute VDOT, populate timeline")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without writing to DB")
    parser.add_argument("--yes", action="store_true",
                        help="Skip confirmation prompt")
    parser.add_argument("--re-enrich", action="store_true",
                        help="Re-enrich all activities after computing VDOTs")
    args = parser.parse_args()

    config = load_config()
    conn = get_connection(config)
    conn.execute("PRAGMA busy_timeout = 30000")

    # Step 1: Find candidates
    candidates = find_race_candidates(conn, verbose=args.verbose)
    print(f"Found {len(candidates)} race candidates.")
    print_candidates(candidates)

    if not candidates:
        conn.close()
        return

    # Step 2: Confirm
    if not args.dry_run and not args.yes:
        answer = input(f"\nTag {len(candidates)} activities as races and compute VDOTs? [y/N] ")
        if answer.lower() not in ("y", "yes"):
            print("Aborted.")
            conn.close()
            return

    # Step 3: Tag and compute
    prefix = "[DRY RUN] " if args.dry_run else ""
    print(f"\n{prefix}Tagging races and computing VDOTs...")
    results = tag_and_compute(conn, candidates, dry_run=args.dry_run, verbose=args.verbose)

    # Summary
    computed = [(c, v) for c, v, _, _ in results if v is not None]
    valid = [(c, v) for c, v in computed if 30 <= v <= 70]
    print(f"\n{prefix}Results: {len(candidates)} tagged, "
          f"{len(computed)} VDOTs computed, {len(valid)} inserted into timeline")

    # Step 4: Show timeline
    if not args.dry_run:
        print_vdot_timeline(conn)

    # Step 5: Optionally re-enrich
    if not args.dry_run and (args.re_enrich or (not args.yes and valid)):
        if args.re_enrich:
            do_enrich = True
        else:
            answer = input("\nRe-enrich all activities with updated VDOTs? [y/N] ")
            do_enrich = answer.lower() in ("y", "yes")

        if do_enrich:
            from runbase.analysis.interval_enricher import enrich_batch
            print("\nRe-enriching all activities...")
            result = enrich_batch(conn, config, verbose=args.verbose)
            print(f"  Enriched: {result['enriched']}, Skipped: {result['skipped']}")
            print(f"  Zones: {result['zones_assigned']}, Segments: {result['segments_created']}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
