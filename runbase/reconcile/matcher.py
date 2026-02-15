"""Find orphaned Strava activity_sources that match a given date + distance."""

import json
from datetime import datetime, timedelta

METERS_PER_MILE = 1609.344


def _load_orphaned_strava_sources(conn) -> list[dict]:
    """Load all Strava activity_sources with activity_id IS NULL."""
    rows = conn.execute(
        """SELECT id, source_id, distance_mi, duration_s, workout_name, metadata_json
           FROM activity_sources
           WHERE source = 'strava' AND activity_id IS NULL"""
    ).fetchall()

    sources = []
    for r in rows:
        meta = json.loads(r[5]) if r[5] else {}
        sources.append({
            "id": r[0],
            "source_id": r[1],
            "distance_mi": r[2],
            "duration_s": r[3],
            "workout_name": r[4],
            "metadata": meta,
            "start_date": meta.get("start_date"),
            "gear_id": meta.get("gear_id"),
            "strava_name": meta.get("strava_name"),
            "strava_type": meta.get("strava_type"),
        })
    return sources


def find_strava_match(conn, date: str, distance_mi: float,
                      tolerance_pct: float = 5.0) -> dict | None:
    """Find the best orphaned Strava source matching date ± 1 day and distance.

    Returns the matched orphan dict or None.
    """
    orphans = _load_orphaned_strava_sources(conn)

    if not orphans:
        return None

    # Build candidate dates (±1 day)
    dt = datetime.strptime(date, "%Y-%m-%d")
    candidate_dates = {
        date,
        (dt - timedelta(days=1)).strftime("%Y-%m-%d"),
        (dt + timedelta(days=1)).strftime("%Y-%m-%d"),
    }

    best_match = None
    best_diff_pct = float("inf")

    for orphan in orphans:
        orphan_date = orphan["start_date"]
        if not orphan_date:
            continue
        if orphan_date not in candidate_dates:
            continue

        orphan_dist = orphan["distance_mi"]
        if orphan_dist is None or orphan_dist <= 0:
            # Date-only match (low confidence)
            if orphan_date == date and best_match is None:
                best_match = orphan
                best_diff_pct = 100
            continue

        if distance_mi is None or distance_mi <= 0:
            continue

        diff_pct = abs(orphan_dist - distance_mi) / distance_mi * 100
        if diff_pct <= tolerance_pct and diff_pct < best_diff_pct:
            best_match = orphan
            best_diff_pct = diff_pct

    return best_match


def find_strava_group_match(conn, date: str, distance_mi: float,
                            tolerance_pct: float = 10.0) -> list[dict] | None:
    """Find a group of same-day orphans whose summed distance matches.

    For multi-activity days (warm-up + main + cool-down), individual orphans
    won't match the XLSX total, but their sum may.

    Returns the list of orphan dicts (sorted by start_time) if the sum matches,
    or None if no group match.
    """
    if distance_mi is None or distance_mi <= 0:
        return None

    orphans = _load_orphaned_strava_sources(conn)
    if not orphans:
        return None

    # Build candidate dates (±1 day) — same tolerance as 1:1 matcher
    dt = datetime.strptime(date, "%Y-%m-%d")
    candidate_dates = {
        date,
        (dt - timedelta(days=1)).strftime("%Y-%m-%d"),
        (dt + timedelta(days=1)).strftime("%Y-%m-%d"),
    }

    # Filter to orphans within ±1 day, then group by actual date
    nearby = [o for o in orphans if o["start_date"] in candidate_dates]
    # Group by date — all orphans in a group must share the same day
    from collections import defaultdict
    by_date = defaultdict(list)
    for o in nearby:
        by_date[o["start_date"]].append(o)

    # Try each candidate date's group
    best_group = None
    best_diff_pct = float("inf")
    for d, group in by_date.items():
        if len(group) < 2:
            continue
        if any(o["distance_mi"] is None or o["distance_mi"] <= 0 for o in group):
            continue
        total_dist = sum(o["distance_mi"] for o in group)
        diff_pct = abs(total_dist - distance_mi) / distance_mi * 100
        if diff_pct <= tolerance_pct and diff_pct < best_diff_pct:
            best_group = group
            best_diff_pct = diff_pct

    if best_group is None:
        return None
    same_day = best_group

    # Sort by start_time (metadata) so warm-up comes first
    def sort_key(o):
        st = o["metadata"].get("start_time", "")
        return st or ""

    same_day.sort(key=sort_key)
    return same_day


def backfill_strava_dates(config: dict, conn, verbose: bool = False) -> int:
    """One-time backfill: add start_date to orphaned Strava sources missing it.

    Fetches activity list from Strava API to get dates for existing orphans.
    Returns count of sources updated.
    """
    from runbase.ingest.strava_sync import _get_client, _update_rate_limiter, \
        StravaRateLimiter, RUNNING_TYPES, METERS_PER_MILE

    # Find orphans missing start_date
    rows = conn.execute(
        """SELECT id, source_id, metadata_json
           FROM activity_sources
           WHERE source = 'strava' AND activity_id IS NULL"""
    ).fetchall()

    # Build lookup: strava_id -> (row_id, metadata)
    needs_date = {}
    for r in rows:
        meta = json.loads(r[2]) if r[2] else {}
        if not meta.get("start_date"):
            needs_date[r[1]] = {"row_id": r[0], "metadata": meta}

    if not needs_date:
        if verbose:
            print("All orphaned Strava sources already have start_date.")
        return 0

    if verbose:
        print(f"Found {len(needs_date)} orphans missing start_date. Fetching from Strava API...")

    client = _get_client(config)
    rate_limiter = StravaRateLimiter()
    updated = 0

    try:
        activities_iter = client.get_activities()
        for act in activities_iter:
            _update_rate_limiter(client, rate_limiter)
            if not rate_limiter.check(verbose):
                break

            strava_id = str(act.id)
            if strava_id not in needs_date:
                continue

            entry = needs_date[strava_id]
            meta = entry["metadata"]
            meta["start_date"] = act.start_date_local.strftime("%Y-%m-%d")
            meta["start_time"] = act.start_date_local.isoformat()

            # Also grab workout_type if available
            act_type = act.type.root if hasattr(act.type, 'root') else str(act.type)
            if act_type:
                meta["strava_type"] = act_type
            if hasattr(act, 'workout_type') and act.workout_type is not None:
                meta["workout_type"] = int(act.workout_type)

            conn.execute(
                "UPDATE activity_sources SET metadata_json = ? WHERE id = ?",
                (json.dumps(meta), entry["row_id"]),
            )
            updated += 1

            if verbose and updated % 50 == 0:
                print(f"  ...updated {updated} so far")

    except Exception as e:
        if verbose:
            print(f"  Error fetching Strava activities: {e}")

    conn.commit()
    if verbose:
        print(f"Backfilled start_date for {updated} orphaned Strava sources.")
    return updated
