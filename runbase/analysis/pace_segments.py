"""Stream-based pace segmentation for unstructured runs.

For activities without structured workout intervals (easy runs, long runs),
this module segments stream data by pace zone to identify warmup, cooldown,
walking breaks, strides, etc.
"""

from runbase.models import Interval

# Categories that indicate a structured workout (use FIT laps, not segmentation)
STRUCTURED_CATEGORIES = frozenset({
    "interval", "tempo", "repetition", "fartlek", "race", "hills", "workout",
})


def is_structured(activity: dict) -> bool:
    """Determine if an activity should use existing intervals vs pace segmentation.

    An activity is structured if:
    - workout_category is a structured type
    - It has intervals from XLSX splits (source='xlsx_split')
    - Strava workout_type = 3
    - FIT/Strava laps show a workout-like bimodal pace pattern (fast reps + slow recovery)
    """
    category = (activity.get("workout_category") or "").lower()
    if category in STRUCTURED_CATEGORIES:
        return True

    if activity.get("has_xlsx_splits"):
        return True

    if activity.get("strava_workout_type") == 3:
        return True

    if activity.get("has_workout_fit_laps"):
        return True

    return False


def segment_by_pace(streams: list[dict], boundaries: dict,
                    config: dict | None = None) -> list[Interval]:
    """Segment stream data by pace zone.

    Args:
        streams: List of stream dicts sorted by timestamp_s, each with
                 timestamp_s, pace_s_per_mi, heart_rate, cadence, distance_mi, lat, lon.
        boundaries: Zone boundaries from vdot_to_boundaries().
        config: Optional paces config dict.

    Returns:
        List of Interval objects representing pace segments.
    """
    from runbase.analysis.vdot import classify_pace

    cfg = config or {}
    min_segment_duration = cfg.get("min_segment_duration_s", 10)
    smoothing_window = cfg.get("smoothing_window_s", 30)

    if not streams:
        return []

    # Step 1: Extract per-record pace values and apply rolling average
    records = []
    for s in streams:
        if s.get("timestamp_s") is None:
            continue
        records.append(s)

    if not records:
        return []

    # Smooth pace with rolling average
    paces = [r.get("pace_s_per_mi") for r in records]
    smoothed_paces = _rolling_average(paces, smoothing_window, records)

    # Step 2: Classify each record into a zone
    zones = []
    for pace in smoothed_paces:
        if pace is None or pace <= 0:
            zones.append("unknown")
        else:
            zones.append(classify_pace(pace, boundaries))

    # Step 3: Group consecutive same-zone records into segments
    raw_segments = _group_consecutive(records, zones)

    # Step 4: Merge very short segments into neighbors
    merged = _merge_short_segments(raw_segments, min_segment_duration)

    # Step 5: Build Interval objects
    intervals = []
    for i, seg in enumerate(merged):
        seg_records = seg["records"]
        if not seg_records:
            continue

        start_ts = seg_records[0].get("timestamp_s")
        end_ts = seg_records[-1].get("timestamp_s")
        duration = end_ts - start_ts if start_ts and end_ts else 0

        # Distance from stream cumulative distance
        start_dist = seg_records[0].get("distance_mi") or 0
        end_dist = seg_records[-1].get("distance_mi") or 0
        distance = end_dist - start_dist

        # Averages
        hr_values = [r["heart_rate"] for r in seg_records if r.get("heart_rate") is not None]
        cad_values = [r["cadence"] for r in seg_records if r.get("cadence") is not None]
        avg_hr = round(sum(hr_values) / len(hr_values), 2) if hr_values else None
        avg_cad = round(sum(cad_values) / len(cad_values), 2) if cad_values else None

        avg_pace = None
        avg_pace_display = None
        if distance and distance > 0 and duration:
            avg_pace = round(duration / distance, 1)
            minutes = int(avg_pace // 60)
            secs = avg_pace - minutes * 60
            avg_pace_display = f"{minutes}:{secs:04.1f}"

        zone = seg["zone"]
        is_recovery = zone in ("walk", "E") and i > 0 and i < len(merged) - 1

        intervals.append(Interval(
            rep_number=i + 1,
            gps_measured_distance_mi=round(distance, 4) if distance else None,
            duration_s=round(duration, 1) if duration else None,
            avg_pace_s_per_mi=avg_pace,
            avg_pace_display=avg_pace_display,
            avg_hr=avg_hr,
            avg_cadence=avg_cad,
            is_recovery=is_recovery,
            pace_zone=zone if zone != "unknown" else None,
            is_walking=False,
            is_stride=False,
            start_timestamp_s=start_ts,
            end_timestamp_s=end_ts,
            source="pace_segment",
        ))

    return intervals


def _rolling_average(paces: list[float | None], window_s: int,
                     records: list[dict]) -> list[float | None]:
    """Apply index-based rolling average to smooth GPS noise.

    Uses a fixed-width index window (approximately window_s records since
    stream data is ~1 record/second). O(n) with deque-based sliding window.
    """
    from collections import deque

    if not records:
        return paces

    n = len(records)
    half = window_s // 2
    smoothed = [None] * n

    # Build list of valid (index, pace) for the window
    window = deque()  # (index, pace)
    window_sum = 0.0

    for i in range(n):
        # Add current element to window if valid
        if paces[i] is not None and paces[i] > 0:
            window.append((i, paces[i]))
            window_sum += paces[i]

        # Remove elements that have fallen out of the left side
        while window and window[0][0] < i - half:
            window_sum -= window[0][1]
            window.popleft()

        if paces[i] is None:
            smoothed[i] = None
        elif len(window) > 0:
            smoothed[i] = window_sum / len(window)
        else:
            smoothed[i] = paces[i]

    # The above is a trailing window. For a centered window, do a second pass
    # shifting results. Simpler: just use the trailing window — it's good enough
    # for pace smoothing purposes.

    return smoothed


def _group_consecutive(records: list[dict], zones: list[str]) -> list[dict]:
    """Group consecutive records in the same zone."""
    if not records:
        return []

    segments = []
    current_zone = zones[0]
    current_records = [records[0]]

    for i in range(1, len(records)):
        if zones[i] == current_zone:
            current_records.append(records[i])
        else:
            segments.append({"zone": current_zone, "records": current_records})
            current_zone = zones[i]
            current_records = [records[i]]

    segments.append({"zone": current_zone, "records": current_records})
    return segments


def _merge_short_segments(segments: list[dict], min_duration: float) -> list[dict]:
    """Merge segments shorter than min_duration into their neighbors."""
    if len(segments) <= 1:
        return segments

    merged = list(segments)
    changed = True
    while changed:
        changed = False
        new_merged = []
        i = 0
        while i < len(merged):
            seg = merged[i]
            recs = seg["records"]
            if len(recs) >= 2:
                duration = (recs[-1].get("timestamp_s", 0) - recs[0].get("timestamp_s", 0))
            else:
                duration = 0

            if duration < min_duration and len(new_merged) > 0:
                # Merge into previous segment
                new_merged[-1]["records"].extend(recs)
                changed = True
            elif duration < min_duration and i + 1 < len(merged):
                # Merge into next segment
                merged[i + 1]["records"] = recs + merged[i + 1]["records"]
                changed = True
            else:
                new_merged.append(seg)
            i += 1

        merged = new_merged

    return merged
