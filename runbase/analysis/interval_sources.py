"""Select one lap representation while retaining user edits."""

from collections import defaultdict


def is_auto_split(interval):
    return interval.get("source") == "mile_split" or interval.get("lap_kind") == "split"


def effective_intervals(intervals, *, include_pace_segments=False):
    """Resolve the display/scoring view and its automatic final partial mile.

    A final fragment inherits the duration-weighted prevailing zone of full
    miles. It cannot decide the workout type. An explicit zone edit still wins
    for its own score; button-press intervals are never normalized this way.
    """
    selected = [dict(iv) for iv in preferred_intervals(intervals, include_pace_segments=include_pace_segments)]
    recorded = [iv for iv in selected if iv.get('source') not in ('manual_split', 'pace_segment')]
    if len(recorded) < 2:
        return selected
    recorded.sort(key=lambda iv: (iv.get('rep_number') or 0, iv.get('end_timestamp_s') or 0))
    last = recorded[-1]

    def distance(iv):
        return iv.get('gps_measured_distance_mi') or iv.get('canonical_distance_mi') or iv.get('prescribed_distance_mi') or 0

    # Allow FIT distance rounding near one mile; don't mistake a 0.99 mile lap
    # for a leftover fragment, or apply a mile heuristic to automatic km laps.
    full_miles = [iv for iv in recorded[:-1] if is_auto_split(iv) and 0.98 <= distance(iv) <= 1.02]
    if not is_auto_split(last) or not 0 < distance(last) < 0.98 or not full_miles:
        return selected
    last['is_partial_split'] = True
    if last.get('zone_overridden') or last.get('source') == 'manual':
        return selected
    weights = defaultdict(float)
    for iv in full_miles:
        if iv.get('pace_zone') and not any(iv.get(flag) for flag in ('is_walking', 'is_recovery', 'is_stride', 'is_hill_sprint')):
            weights[iv['pace_zone']] += iv.get('duration_s') or 1
    if weights:
        last['pace_zone'] = max(weights, key=weights.get)
        last['zone_inherited'] = True
    return selected


def preferred_intervals(intervals, *, include_pace_segments=False):
    """Prefer recorded laps to generated mile splits and pace segments.

    Older FIT imports use a NULL source; newer imports use ``fit_lap``.
    ``manual`` rows are edited recorded laps, whereas ``manual_split`` rows
    are explicit workout overlays and must remain available to their caller.
    Generated mile splits are a fallback when no recorded laps remain.
    """
    derived = {"mile_split", "pace_segment", "manual_split"}
    laps = [iv for iv in intervals if iv["source"] not in derived]
    if any(iv["source"] in (None, "fit_lap") for iv in laps):
        laps = [iv for iv in laps if iv["source"] != "strava_lap"]
    miles = [iv for iv in intervals if iv["source"] == "mile_split"]
    # Explicitly classified automatic laps are another version of the mile
    # view. Prefer the generated view, which holds saved mile-zone overrides.
    # Legacy/unclassified laps and manually edited laps remain authoritative.
    if miles and laps and all(
        "lap_kind" in iv.keys() and iv["lap_kind"] == "split"
        and iv["source"] != "manual" for iv in laps
    ):
        laps = miles
    if not laps:
        laps = miles
    if not laps and include_pace_segments:
        laps = [iv for iv in intervals if iv["source"] == "pace_segment"]
    selected = {id(iv) for iv in laps}
    return [iv for iv in intervals
            if id(iv) in selected or iv["source"] == "manual_split"]
