"""Default VDOT eligibility without replacing explicit user choices."""

import math

from runbase.analysis.interval_sources import preferred_intervals

STRIDE_DISTANCE_THRESHOLD = 0.40
STRIDE_EXCLUSION_SOURCE = "strides"


def _positive_distance(value):
    return value if value is not None and math.isfinite(value) and value > 0 else 0


def stride_distance_fraction(total_distance_mi, intervals):
    """Stride distance / full activity distance, including recovery distance.

    Use one recording representation. Manual workout overlays are not extra
    distance on top of the recorded laps; use them only if there are no laps.
    Missing activity distance or a stride count alone cannot establish a ratio.
    """
    total = _positive_distance(total_distance_mi)
    if not total:
        return None
    recordings = [iv for iv in intervals if iv.get("source") != "manual_split"]
    selected = preferred_intervals(recordings, include_pace_segments=True)
    if not selected:
        selected = [iv for iv in intervals if iv.get("source") == "manual_split"]
    stride_distance = 0.0
    for iv in selected:
        if not iv.get("is_stride") or iv.get("is_walking"):
            continue
        for field in ("canonical_distance_mi", "gps_measured_distance_mi", "prescribed_distance_mi"):
            distance = _positive_distance(iv.get(field))
            if distance:
                stride_distance += distance
                break
    return stride_distance / total


def stride_exclusion_decision(activity, intervals):
    """Return the effective flag and its automatic origin, preserving overrides."""
    excluded = activity.get("exclude_from_vdot")
    source = activity.get("vdot_exclusion_source")
    if source != STRIDE_EXCLUSION_SOURCE and (excluded is not None or source is not None):
        return excluded, source
    fraction = stride_distance_fraction(activity.get("distance_mi"), intervals)
    qualifies = fraction is not None and (
        fraction >= STRIDE_DISTANCE_THRESHOLD
        or math.isclose(fraction, STRIDE_DISTANCE_THRESHOLD, rel_tol=1e-9, abs_tol=1e-12)
    )
    return (1, STRIDE_EXCLUSION_SOURCE) if qualifies else (None, None)


def refresh_stride_vdot_exclusion(conn, activity_id):
    """Refresh the default in the caller's transaction; never clear stored CVD."""
    cursor = conn.execute("SELECT * FROM activities WHERE id = ?", (activity_id,))
    row = cursor.fetchone()
    if row is None:
        return None
    activity = dict(zip((column[0] for column in cursor.description), row))
    cursor = conn.execute(
        "SELECT source, lap_kind, canonical_distance_mi, gps_measured_distance_mi, "
        "prescribed_distance_mi, is_stride, is_walking FROM intervals WHERE activity_id = ?",
        (activity_id,),
    )
    columns = [column[0] for column in cursor.description]
    intervals = [dict(zip(columns, row)) for row in cursor]
    excluded, source = stride_exclusion_decision(activity, intervals)
    if (excluded, source) != (activity.get("exclude_from_vdot"), activity.get("vdot_exclusion_source")):
        conn.execute(
            "UPDATE activities SET exclude_from_vdot = ?, vdot_exclusion_source = ? WHERE id = ?",
            (excluded, source, activity_id),
        )
    return {"exclude_from_vdot": bool(excluded), "vdot_exclusion_source": source}
