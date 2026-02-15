"""Fastest-segment finder: sliding-window scan of GPS streams."""

from __future__ import annotations

METERS_PER_MILE = 1609.344

# Minimum plausible pace in s/mi — anything faster is a GPS glitch.
# 3:00/mi ≈ world-class sprinter pace over distance.
MIN_PACE_S_PER_MI = 180.0


def find_fastest(conn, target_m: float, top_n: int = 10,
                 verbose: bool = False) -> list[dict]:
    """Find the top-N fastest segments at a given distance.

    Sources:
      1. Existing intervals whose canonical distance matches target_m.
      2. Sliding-window scan over per-second GPS streams.

    Stream windows that overlap snapped-interval time ranges are excluded
    to avoid double-counting measured reps.

    Returns a list of dicts sorted by pace (ascending), capped at *top_n*.
    """
    target_mi = target_m / METERS_PER_MILE
    tol_mi = target_mi * 0.03  # 3% tolerance for matching intervals
    results: list[dict] = []

    # ------------------------------------------------------------------
    # Source 1 — intervals already at the target distance
    # ------------------------------------------------------------------
    interval_rows = conn.execute("""
        SELECT i.id, i.activity_id, a.date, a.workout_name,
               i.duration_s, i.avg_pace_s_per_mi,
               i.canonical_distance_mi, i.source, i.location_type
        FROM intervals i
        JOIN activities a ON a.id = i.activity_id
        WHERE ABS(i.canonical_distance_mi - ?) < ?
          AND i.is_walking = 0 AND i.is_recovery = 0 AND i.is_stride = 0
          AND i.duration_s > 0
    """, (target_mi, tol_mi)).fetchall()

    for r in interval_rows:
        results.append({
            "activity_id": r[1],
            "date": r[2],
            "workout_name": r[3] or "",
            "duration_s": r[4],
            "pace_s_per_mi": r[5],
            "source_type": "interval",
        })

    # ------------------------------------------------------------------
    # Source 2 — sliding-window scan of GPS streams
    # ------------------------------------------------------------------
    act_rows = conn.execute("""
        SELECT DISTINCT s.activity_id, a.date, a.workout_name
        FROM streams s
        JOIN activities a ON a.id = s.activity_id
    """).fetchall()

    scanned = 0
    found = 0

    for activity_id, date, workout_name in act_rows:
        # Exclusion zones: intervals at target distance (avoid double-
        # counting) + snapped intervals at any distance (avoid extracting
        # sub-splits from measured reps).
        excl_rows = conn.execute("""
            SELECT start_timestamp_s, end_timestamp_s
            FROM intervals
            WHERE activity_id = ?
              AND start_timestamp_s IS NOT NULL
              AND end_timestamp_s IS NOT NULL
              AND (
                  ABS(canonical_distance_mi - ?) < ?
                  OR location_type IN ('track', 'measured_course')
              )
        """, (activity_id, target_mi, tol_mi)).fetchall()
        exclusions = [(s, e) for s, e in excl_rows]

        # Load stream points grouped by source_id to avoid mixing
        # sub-activity GPS data from group-matched activities.
        source_ids = conn.execute("""
            SELECT DISTINCT source_id FROM streams
            WHERE activity_id = ? AND source_id IS NOT NULL
        """, (activity_id,)).fetchall()

        # Fall back to ungrouped if no source_id set
        if not source_ids:
            source_ids = [(None,)]

        scanned += 1
        best_elapsed = float("inf")

        for (src_id,) in source_ids:
            if src_id is not None:
                points = conn.execute("""
                    SELECT timestamp_s, distance_mi
                    FROM streams
                    WHERE activity_id = ? AND source_id = ?
                      AND distance_mi IS NOT NULL
                      AND timestamp_s IS NOT NULL
                    ORDER BY timestamp_s
                """, (activity_id, src_id)).fetchall()
            else:
                points = conn.execute("""
                    SELECT timestamp_s, distance_mi
                    FROM streams
                    WHERE activity_id = ?
                      AND distance_mi IS NOT NULL
                      AND timestamp_s IS NOT NULL
                    ORDER BY timestamp_s
                """, (activity_id,)).fetchall()

            if len(points) < 2:
                continue

            elapsed = _fastest_window(points, target_mi, exclusions)
            if elapsed is not None and elapsed < best_elapsed:
                best_elapsed = elapsed

        if best_elapsed < float("inf"):
            pace = best_elapsed / target_mi
            if pace < MIN_PACE_S_PER_MI:
                continue  # GPS glitch
            results.append({
                "activity_id": activity_id,
                "date": date,
                "workout_name": workout_name or "",
                "duration_s": best_elapsed,
                "pace_s_per_mi": pace,
                "source_type": "stream",
            })
            found += 1

    if verbose:
        print(f"  {len(interval_rows)} interval results, "
              f"{found} stream results ({scanned} activities scanned)")

    results.sort(key=lambda r: r["pace_s_per_mi"])
    return results[:top_n]


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _fastest_window(points: list[tuple], target_mi: float,
                    exclusions: list[tuple]) -> float | None:
    """Two-pointer scan for the fastest window covering *target_mi*.

    Interpolates the right edge for sub-second precision.
    Returns elapsed seconds, or None if no valid window exists.
    """
    n = len(points)
    best = float("inf")
    right = 0

    for left in range(n):
        goal = points[left][1] + target_mi

        # Advance right pointer until distance >= goal
        while right < n - 1 and points[right][1] < goal:
            right += 1

        if points[right][1] < goal:
            break  # remaining segment is shorter than target

        # Interpolate exact time at goal distance
        if right > 0 and points[right][1] > points[right - 1][1]:
            frac = ((goal - points[right - 1][1])
                    / (points[right][1] - points[right - 1][1]))
            t_end = (points[right - 1][0]
                     + frac * (points[right][0] - points[right - 1][0]))
        else:
            t_end = points[right][0]

        elapsed = t_end - points[left][0]
        if elapsed <= 0:
            continue

        # Exclude if midpoint falls inside a snapped/target-distance interval
        mid = points[left][0] + elapsed / 2
        if any(s <= mid <= e for s, e in exclusions):
            continue

        if elapsed < best:
            best = elapsed

    return best if best < float("inf") else None
