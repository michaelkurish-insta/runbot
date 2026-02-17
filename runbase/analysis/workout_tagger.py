"""Tag recovery intervals and group rep sets on structured workouts.

Classifies each lap as warmup, cooldown, work, or recovery using VDOT
zone boundaries, then groups contiguous work+recovery blocks into
numbered sets separated by set breaks (walking laps or long recoveries).
"""

from statistics import median

from runbase.analysis.vdot import classify_pace

_WORK_ZONES = {"T", "I", "R", "FR"}
_EASY_ZONES = {"E", "M", "walk"}

# Sources that represent real laps (not auto-generated pace segments)
_LAP_SOURCES = {"fit_lap", "strava_lap", "xlsx_split", None}

# A recovery is a "set break" if its duration exceeds this multiple
# of the median recovery duration.
_SET_BREAK_DURATION_MULTIPLE = 2.0

# A recovery is a "set break" if its distance exceeds this (miles).
_SET_BREAK_DISTANCE_MI = 0.3


def tag_workout_intervals(intervals: list[dict], boundaries: dict | None) -> list[dict]:
    """Tag is_recovery and set_number on structured workout intervals.

    Only operates on real laps (fit_lap, strava_lap, xlsx_split, or NULL source).
    Skips pace_segment intervals entirely.

    Args:
        intervals: List of interval dicts (loaded from DB, sorted by rep_number).
        boundaries: VDOT zone boundaries from vdot_to_boundaries(). If None, skips.

    Returns:
        The same list with is_recovery and set_number updated in-place.
    """
    if not boundaries or len(intervals) < 2:
        return intervals

    # Filter to real laps only (skip pace_segments)
    laps = [iv for iv in intervals if iv.get("source") in _LAP_SOURCES]
    if len(laps) < 2:
        return intervals

    # Step 1: Classify each lap's zone
    for lap in laps:
        pace = lap.get("avg_pace_s_per_mi")
        if pace and pace > 0:
            lap["_zone"] = classify_pace(pace, boundaries)
        else:
            lap["_zone"] = None

    def _is_work(lap):
        return lap.get("_zone") in _WORK_ZONES

    def _is_easy(lap):
        return lap.get("_zone") in _EASY_ZONES or lap.get("_zone") is None

    # Step 2: Find first and last work interval indices
    first_work = None
    last_work = None
    for i, lap in enumerate(laps):
        if _is_work(lap):
            if first_work is None:
                first_work = i
            last_work = i

    if first_work is None:
        # No work intervals found — can't tag
        return intervals

    # Step 3: Tag warmup (before first work), cooldown (after last work)
    for i, lap in enumerate(laps):
        if i < first_work:
            lap["set_number"] = None
            lap["is_recovery"] = False
        elif i > last_work:
            lap["set_number"] = None
            lap["is_recovery"] = False

    # Step 4: Tag work/recovery in the middle section
    middle = laps[first_work:last_work + 1]
    for lap in middle:
        if _is_work(lap):
            lap["is_recovery"] = False
        else:
            lap["is_recovery"] = True

    # Step 5: Detect set breaks among recovery intervals
    recoveries = [lap for lap in middle if lap["is_recovery"]]
    if not recoveries:
        # All work, no recoveries — single set
        for lap in middle:
            lap["set_number"] = 1
        _cleanup_zones(laps)
        return intervals

    recovery_durations = [
        r.get("duration_s", 0) for r in recoveries if r.get("duration_s")
    ]
    med_recovery_dur = median(recovery_durations) if recovery_durations else 0

    set_break_indices = set()
    for i, lap in enumerate(laps):
        if first_work <= i <= last_work and lap.get("is_recovery"):
            is_break = False
            # Walking = set break
            if lap.get("is_walking") or lap.get("_zone") == "walk":
                is_break = True
            # Long recovery = set break
            elif (med_recovery_dur > 0 and lap.get("duration_s")
                  and lap["duration_s"] >= _SET_BREAK_DURATION_MULTIPLE * med_recovery_dur):
                is_break = True
            # Long distance recovery = set break
            elif (lap.get("gps_measured_distance_mi")
                  and lap["gps_measured_distance_mi"] >= _SET_BREAK_DISTANCE_MI):
                is_break = True

            if is_break:
                set_break_indices.add(i)
                lap["set_number"] = None

    # Step 6: Assign set_number to contiguous groups
    set_num = 1
    in_set = False
    for i, lap in enumerate(laps):
        if i < first_work or i > last_work:
            continue  # warmup/cooldown already tagged
        if i in set_break_indices:
            if in_set:
                set_num += 1
                in_set = False
            continue
        lap["set_number"] = set_num
        in_set = True

    _cleanup_zones(laps)
    return intervals


def _cleanup_zones(laps: list[dict]) -> None:
    """Remove temporary _zone keys."""
    for lap in laps:
        lap.pop("_zone", None)
