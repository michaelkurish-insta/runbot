"""Tag recovery intervals on structured workouts.

Classifies each lap as warmup, cooldown, work, or recovery using VDOT
zone boundaries.  All work + recovery intervals between the first and
last work rep are placed in set 1.
"""

from runbase.analysis.vdot import classify_pace

_WORK_ZONES = {"T", "I", "R", "FR"}
_EASY_ZONES = {"E", "M", "walk"}

# Sources that represent real laps (not auto-generated pace segments)
_LAP_SOURCES = {"fit_lap", "strava_lap", "xlsx_split", None}


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

    # Step 2: Find first and last work interval indices
    first_work = None
    last_work = None
    for i, lap in enumerate(laps):
        if _is_work(lap):
            if first_work is None:
                first_work = i
            last_work = i

    if first_work is None:
        _cleanup_zones(laps)
        return intervals

    # Step 3: Tag warmup (before first work), cooldown (after last work)
    for i, lap in enumerate(laps):
        if i < first_work or i > last_work:
            lap["set_number"] = None
            lap["is_recovery"] = False

    # Step 4: Tag work/recovery in the middle section, all in set 1
    middle = laps[first_work:last_work + 1]
    for lap in middle:
        if _is_work(lap):
            lap["is_recovery"] = False
        else:
            lap["is_recovery"] = True
        lap["set_number"] = 1

    _cleanup_zones(laps)
    return intervals


def _cleanup_zones(laps: list[dict]) -> None:
    """Remove temporary _zone keys."""
    for lap in laps:
        lap.pop("_zone", None)
