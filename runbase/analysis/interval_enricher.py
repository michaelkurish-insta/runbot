"""Interval enrichment waterfall orchestrator.

Runs the full enrichment pipeline on an activity:
1. Determine structured vs unstructured → use FIT laps or create pace segments
2. Track detection → snap distances
3. Measured course detection → snap distances
4. Walking scrub → flag walking intervals
5. Stride detection → flag short intervals
6. Pace zone assignment
7. Compute adjusted_distance_mi
8. Store VDOT on activity
"""

import re

from runbase.analysis.vdot import (
    get_current_vdot, vdot_to_boundaries, vdot_to_paces, classify_pace,
)
from runbase.analysis.track_detect import (
    detect_track_activity, snap_to_100m,
)
from runbase.analysis.pace_segments import is_structured, segment_by_pace
from runbase.analysis.locations import find_matching_courses, best_course_for_interval

METERS_PER_MILE = 1609.344


def _recalc_pace(iv: dict) -> None:
    """Recalculate avg pace from canonical distance and duration after snapping."""
    dist = iv.get("canonical_distance_mi")
    dur = iv.get("duration_s")
    if dist and dist > 0 and dur and dur > 0:
        pace = dur / dist
        iv["avg_pace_s_per_mi"] = pace
        mins = int(pace // 60)
        secs = pace - mins * 60
        iv["avg_pace_display"] = f"{mins}:{secs:04.1f}"


# Distance bounds (meters) for snapping when activity name doesn't imply a
# workout or race.  Below min: likely strides.  Above max: likely a warm-up mile.
TRACK_SNAP_MIN_DISTANCE_M = 180
TRACK_SNAP_MAX_DISTANCE_M = 1300

# ---------------------------------------------------------------------------
# Race detection
# ---------------------------------------------------------------------------

_RACE_NAME_PATTERNS = [
    re.compile(r"\brace\b", re.IGNORECASE),
    re.compile(r"\bTT\b"),
    re.compile(r"\btime\s*trial\b", re.IGNORECASE),
    re.compile(r"\bparkrun\b", re.IGNORECASE),
]

# Ordered so longer phrases match first ("2 mile" before "mile").
RACE_DISTANCE_PATTERNS = [
    (re.compile(r"\bhalf\s*marathon\b", re.IGNORECASE), 21097.5),
    (re.compile(r"\bmarathon\b", re.IGNORECASE), 42195),
    (re.compile(r"\bhalf\b", re.IGNORECASE), 21097.5),
    (re.compile(r"\bparkrun\b", re.IGNORECASE), 5000),
    (re.compile(r"\b2\s*mile\b", re.IGNORECASE), 3218.688),
    (re.compile(r"\bmile\b", re.IGNORECASE), 1609.344),
    (re.compile(r"\b10k\b", re.IGNORECASE), 10000),
    (re.compile(r"\b8k\b", re.IGNORECASE), 8000),
    (re.compile(r"\b5k\b", re.IGNORECASE), 5000),
    (re.compile(r"\b3200\b"), 3200),
    (re.compile(r"\b3000\b"), 3000),
    (re.compile(r"\b1500\b"), 1500),
    (re.compile(r"\b800m?\b"), 800),
    (re.compile(r"\b400m?\b"), 400),
    (re.compile(r"\b200m?\b"), 200),
]

COMMON_RACE_DISTANCES_M = [
    200, 400, 800, 1500, 1609.344, 3000, 3200, 3218.688,
    5000, 8000, 10000, 15000, 21097.5, 42195,
]


def _is_race_name(name: str | None) -> bool:
    """Check if an activity name implies a race / time trial."""
    if not name:
        return False
    return any(p.search(name) for p in _RACE_NAME_PATTERNS)


def _parse_race_distance_m(name: str | None) -> float | None:
    """Extract race distance in meters from activity name."""
    if not name:
        return None
    for pattern, dist_m in RACE_DISTANCE_PATTERNS:
        if pattern.search(name):
            return dist_m
    return None


def _closest_race_distance_m(dist_m: float) -> float:
    """Return the common race distance closest to dist_m."""
    return min(COMMON_RACE_DISTANCES_M, key=lambda d: abs(d - dist_m))


def _parse_race_time_s(name: str | None) -> float | None:
    """Extract a race time from the activity name. Returns seconds or None.

    Matches patterns like '5:12', '18:45', '1:05:30'.
    """
    if not name:
        return None
    m = re.search(r"\b(\d{1,2}):(\d{2}):(\d{2})\b", name)
    if m:
        return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
    m = re.search(r"\b(\d{1,2}):(\d{2})\b", name)
    if m and int(m.group(2)) < 60:
        return int(m.group(1)) * 60 + int(m.group(2))
    return None


# ---------------------------------------------------------------------------
# Workout (structured repeats) detection
# ---------------------------------------------------------------------------

_WORKOUT_NAME_PATTERNS = [
    re.compile(r"\d+\s*x\s*[\d(]", re.IGNORECASE),  # "6x400", "3x(2,2,4)"
    re.compile(r"\brepeat", re.IGNORECASE),
    re.compile(r"\binterval", re.IGNORECASE),
]


def _is_workout_name(name: str | None) -> bool:
    """Check if an activity name implies structured repeats (not a race)."""
    if not name:
        return False
    # Race takes priority — don't double-classify
    if _is_race_name(name):
        return False
    return any(p.search(name) for p in _WORKOUT_NAME_PATTERNS)


_TEMPO_NAME_PATTERNS = [
    re.compile(r"\bat\s*T\b", re.IGNORECASE),
    re.compile(r"\b\d+\s*miles?\s*at\s*T\b", re.IGNORECASE),
    re.compile(r"\btempo\b", re.IGNORECASE),
    re.compile(r"\b@\s*t\b", re.IGNORECASE),
]

_HILLS_NAME_PATTERNS = [
    re.compile(r"\bhill", re.IGNORECASE),
    re.compile(r"\bmins?\s*H\b"),
]


def _infer_workout_category(name: str | None) -> str | None:
    """Infer workout_category from the activity name. Returns None if unknown."""
    if not name:
        return None
    if _is_race_name(name):
        return "race"
    if any(p.search(name) for p in _TEMPO_NAME_PATTERNS):
        return "tempo"
    if any(p.search(name) for p in _HILLS_NAME_PATTERNS):
        return "hills"
    if _is_workout_name(name):
        return "repetition"
    return None


def _get_paces_config(config: dict) -> dict:
    """Extract paces config with defaults."""
    paces = config.get("paces", {})
    return {
        "walking_threshold_s_per_mi": paces.get("walking_threshold_s_per_mi", 660),
        "stride_max_duration_s": paces.get("stride_max_duration_s", 30),
        "track_detection": paces.get("track_detection", {}),
        "measured_courses": paces.get("measured_courses", []),
    }


def _load_activity(conn, activity_id: int) -> dict | None:
    """Load activity row as a dict."""
    row = conn.execute(
        """SELECT id, date, distance_mi, workout_category, workout_name
           FROM activities WHERE id = ?""",
        (activity_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0], "date": row[1], "distance_mi": row[2],
        "workout_category": row[3], "workout_name": row[4],
    }


def _load_intervals(conn, activity_id: int) -> list[dict]:
    """Load existing intervals for an activity."""
    rows = conn.execute(
        """SELECT id, rep_number, gps_measured_distance_mi, canonical_distance_mi,
                  duration_s, avg_pace_s_per_mi, avg_pace_display, avg_hr, avg_cadence,
                  is_recovery, start_timestamp_s, end_timestamp_s, source, is_race,
                  set_number
           FROM intervals WHERE activity_id = ? ORDER BY rep_number""",
        (activity_id,),
    ).fetchall()
    return [
        {
            "id": r[0], "rep_number": r[1], "gps_measured_distance_mi": r[2],
            "canonical_distance_mi": r[3], "duration_s": r[4],
            "avg_pace_s_per_mi": r[5], "avg_pace_display": r[6],
            "avg_hr": r[7], "avg_cadence": r[8], "is_recovery": bool(r[9]),
            "start_timestamp_s": r[10], "end_timestamp_s": r[11], "source": r[12],
            "is_race": bool(r[13]) if r[13] else False,
            "set_number": r[14],
        }
        for r in rows
    ]


def _load_streams(conn, activity_id: int) -> list[dict]:
    """Load stream data for an activity."""
    rows = conn.execute(
        """SELECT timestamp_s, lat, lon, altitude_ft, heart_rate, cadence,
                  pace_s_per_mi, distance_mi, source_id
           FROM streams WHERE activity_id = ? ORDER BY timestamp_s""",
        (activity_id,),
    ).fetchall()
    return [
        {
            "timestamp_s": r[0], "lat": r[1], "lon": r[2], "altitude_ft": r[3],
            "heart_rate": r[4], "cadence": r[5], "pace_s_per_mi": r[6],
            "distance_mi": r[7], "source_id": r[8],
        }
        for r in rows
    ]


def _split_streams_by_source(streams: list[dict]) -> list[list[dict]]:
    """Split streams into per-source groups.

    For activities with multiple Strava sub-activities (group-matched),
    each source's streams must be processed independently to avoid
    interleaving GPS data from different locations/times.

    Returns a list of stream lists.  Single-source activities return
    one group containing all streams.
    """
    source_ids = {s.get("source_id") for s in streams}
    source_ids.discard(None)
    if len(source_ids) <= 1:
        return [streams]
    groups = {}
    for s in streams:
        sid = s.get("source_id")
        groups.setdefault(sid, []).append(s)
    return list(groups.values())


def _check_has_xlsx_splits(conn, activity_id: int) -> bool:
    """Check if an activity has intervals from XLSX splits."""
    row = conn.execute(
        "SELECT COUNT(*) FROM intervals WHERE activity_id = ? AND source = 'xlsx_split'",
        (activity_id,),
    ).fetchone()
    return row[0] > 0 if row else False


def _check_strava_workout_type(conn, activity_id: int) -> int | None:
    """Get Strava workout_type from activity source metadata."""
    import json
    row = conn.execute(
        """SELECT metadata_json FROM activity_sources
           WHERE activity_id = ? AND source = 'strava'""",
        (activity_id,),
    ).fetchone()
    if row and row[0]:
        meta = json.loads(row[0])
        wt = meta.get("workout_type")
        if wt is not None:
            return int(wt)
    return None


def _compute_centroid(streams: list[dict]) -> tuple[float, float] | None:
    """Compute GPS centroid from stream data."""
    lats = [s["lat"] for s in streams if s.get("lat") is not None]
    lons = [s["lon"] for s in streams if s.get("lon") is not None]
    if not lats:
        return None
    return (sum(lats) / len(lats), sum(lons) / len(lons))


_WORK_PACE_ZONES = {"T", "I", "R", "FR"}


_TRUSTED_INTERVAL_SOURCES = {"fit_lap", "strava_lap"}


def _compute_work_group_centroids(
    intervals: list[dict],
    streams: list[dict],
    boundaries: dict | None,
) -> dict[int, tuple[float, float]]:
    """Compute GPS centroids for groups of work intervals by distance bucket.

    Uses interval timestamps to extract stream points for centroid computation.
    Trusts timestamps from FIT laps and Strava laps.  Falls back to filtering
    stream points by work-pace zone for activities without any trusted laps
    (pre-Strava XLSX-only activities).

    Returns:
        Dict mapping distance_bucket_m → (lat, lon) centroid.
    """
    if not boundaries or not streams:
        return {}

    import bisect

    # Build sorted geo points for timestamp-based lookup
    geo_pts = sorted(
        ((s["timestamp_s"], s["lat"], s["lon"]) for s in streams
         if s.get("timestamp_s") is not None and s.get("lat") is not None),
        key=lambda x: x[0],
    )
    if not geo_pts:
        return {}

    geo_ts = [p[0] for p in geo_pts]

    def _stream_points_in_range(t_start: float, t_end: float):
        lo = bisect.bisect_left(geo_ts, t_start)
        hi = bisect.bisect_right(geo_ts, t_end)
        return geo_pts[lo:hi]

    # Separate work intervals into trusted-timestamp vs no-timestamp
    ts_groups: dict[int, list[dict]] = {}
    no_ts_buckets: set[int] = set()

    for iv in intervals:
        if iv.get("is_recovery"):
            continue
        if iv.get("source") == "pace_segment":
            continue
        pace = iv.get("avg_pace_s_per_mi")
        gps_dist = iv.get("gps_measured_distance_mi")
        if not pace or pace <= 0 or not gps_dist:
            continue
        zone = classify_pace(pace, boundaries)
        if zone not in _WORK_PACE_ZONES:
            continue
        bucket = round(gps_dist * METERS_PER_MILE / 100) * 100

        ts_start = iv.get("start_timestamp_s")
        ts_end = iv.get("end_timestamp_s")
        if (ts_start is not None and ts_end is not None
                and iv.get("source") in _TRUSTED_INTERVAL_SOURCES):
            ts_groups.setdefault(bucket, []).append(iv)
        else:
            no_ts_buckets.add(bucket)

    # Compute per-group centroids from trusted-timestamp intervals
    centroids: dict[int, tuple[float, float]] = {}
    for bucket, ivs in ts_groups.items():
        lats: list[float] = []
        lons: list[float] = []
        for iv in ivs:
            for _, lat, lon in _stream_points_in_range(
                iv["start_timestamp_s"], iv["end_timestamp_s"]
            ):
                lats.append(lat)
                lons.append(lon)
        if lats:
            centroids[bucket] = (sum(lats) / len(lats), sum(lons) / len(lons))

    # For buckets without trusted timestamps, fall back to stream-pace filtering.
    # This only applies to pre-Strava XLSX-only activities (no Strava laps).
    if no_ts_buckets:
        work_lats: list[float] = []
        work_lons: list[float] = []
        for s in streams:
            if s.get("lat") is None or s.get("pace_s_per_mi") is None:
                continue
            pace = s["pace_s_per_mi"]
            if pace <= 0:
                continue
            zone = classify_pace(pace, boundaries)
            if zone in _WORK_PACE_ZONES:
                work_lats.append(s["lat"])
                work_lons.append(s["lon"])

        if work_lats:
            work_centroid = (
                sum(work_lats) / len(work_lats),
                sum(work_lons) / len(work_lons),
            )
            for bucket in no_ts_buckets:
                if bucket not in centroids:
                    centroids[bucket] = work_centroid

    return centroids


def _estimate_interval_timestamps(intervals: list[dict], streams: list[dict]) -> None:
    """Estimate start/end timestamps for intervals that lack them.

    Uses cumulative stream distance to map interval distance boundaries to
    stream timestamps. Modifies intervals in-place (only those missing timestamps).
    """
    # Only process if some intervals are missing timestamps
    needs_estimation = [
        iv for iv in intervals
        if iv.get("start_timestamp_s") is None and iv.get("gps_measured_distance_mi")
    ]
    if not needs_estimation:
        return

    # Build cumulative distance → timestamp mapping from streams
    stream_pts = [
        (s["timestamp_s"], s["distance_mi"])
        for s in streams
        if s.get("timestamp_s") is not None and s.get("distance_mi") is not None
    ]
    if len(stream_pts) < 2:
        return

    stream_pts.sort(key=lambda x: x[0])
    stream_ts = [p[0] for p in stream_pts]
    stream_dist = [p[1] for p in stream_pts]

    def _find_timestamp_for_distance(target_dist: float) -> float | None:
        """Find the stream timestamp closest to target cumulative distance."""
        best_idx = 0
        best_diff = abs(stream_dist[0] - target_dist)
        for i in range(1, len(stream_dist)):
            diff = abs(stream_dist[i] - target_dist)
            if diff < best_diff:
                best_diff = diff
                best_idx = i
        return stream_ts[best_idx]

    # Walk intervals in rep_number order, accumulating distance
    sorted_ivs = sorted(needs_estimation, key=lambda iv: iv.get("rep_number", 0))
    cumulative_dist = 0.0
    for iv in sorted_ivs:
        iv_dist = iv.get("gps_measured_distance_mi") or 0
        start_ts = _find_timestamp_for_distance(cumulative_dist)
        cumulative_dist += iv_dist
        end_ts = _find_timestamp_for_distance(cumulative_dist)
        if start_ts is not None:
            iv["start_timestamp_s"] = start_ts
        if end_ts is not None:
            iv["end_timestamp_s"] = end_ts


def enrich_activity(conn, activity_id: int, config: dict,
                    verbose: bool = False) -> dict:
    """Run the full enrichment waterfall on an activity.

    Returns:
        Summary dict with enrichment results.
    """
    summary = {
        "activity_id": activity_id,
        "track_intervals": 0,
        "measured_intervals": 0,
        "recovery_intervals": 0,
        "sets_tagged": 0,
        "walking_intervals": 0,
        "stride_intervals": 0,
        "zones_assigned": 0,
        "segments_created": 0,
        "skipped": False,
        "skip_reason": None,
    }

    paces_cfg = _get_paces_config(config)
    walking_threshold = paces_cfg["walking_threshold_s_per_mi"]
    stride_max = paces_cfg["stride_max_duration_s"]
    track_cfg = paces_cfg["track_detection"]

    # Load activity
    activity = _load_activity(conn, activity_id)
    if not activity:
        summary["skipped"] = True
        summary["skip_reason"] = "not found"
        return summary

    # Infer workout_category from name if not set
    if not activity["workout_category"]:
        inferred = _infer_workout_category(activity["workout_name"])
        if inferred:
            activity["workout_category"] = inferred
            conn.execute(
                "UPDATE activities SET workout_category = ?, updated_at = datetime('now') WHERE id = ?",
                (inferred, activity_id),
            )
            if verbose:
                print(f"    Category inferred: '{inferred}' from '{activity['workout_name']}'")

    # Load current VDOT
    vdot = get_current_vdot(conn, activity["date"])
    boundaries = None
    if vdot:
        boundaries = vdot_to_boundaries(vdot, walking_threshold)

    # Load streams
    streams = _load_streams(conn, activity_id)

    # Determine structured vs unstructured
    activity_info = {
        "workout_category": activity["workout_category"],
        "has_xlsx_splits": _check_has_xlsx_splits(conn, activity_id),
        "strava_workout_type": _check_strava_workout_type(conn, activity_id),
    }

    intervals = _load_intervals(conn, activity_id)

    if not is_structured(activity_info) and streams and boundaries:
        # Unstructured: create pace segments from streams
        # Delete old pace_segment intervals first
        conn.execute(
            "DELETE FROM intervals WHERE activity_id = ? AND source = 'pace_segment'",
            (activity_id,),
        )

        segments = segment_by_pace(streams, boundaries, paces_cfg)
        if segments:
            for seg in segments:
                seg.activity_id = activity_id
                conn.execute(
                    """INSERT INTO intervals
                       (activity_id, rep_number, gps_measured_distance_mi, duration_s,
                        avg_pace_s_per_mi, avg_pace_display, avg_hr, avg_cadence,
                        is_recovery, pace_zone, is_walking, is_stride,
                        start_timestamp_s, end_timestamp_s, source)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (activity_id, seg.rep_number, seg.gps_measured_distance_mi,
                     seg.duration_s, seg.avg_pace_s_per_mi, seg.avg_pace_display,
                     seg.avg_hr, seg.avg_cadence, seg.is_recovery,
                     seg.pace_zone, seg.is_walking, seg.is_stride,
                     seg.start_timestamp_s, seg.end_timestamp_s, seg.source),
                )
            summary["segments_created"] = len(segments)
            if verbose:
                print(f"    Created {len(segments)} pace segments")

            # Reload intervals after segmentation
            intervals = _load_intervals(conn, activity_id)

    # --- Track detection (Step 1) ---
    # Run per source group to avoid mixing GPS from group-matched sub-activities.
    if streams and intervals:
        stream_groups = _split_streams_by_source(streams)
        track_result = {"is_track": False}
        for sg in stream_groups:
            r = detect_track_activity(conn, activity_id, intervals, sg, track_cfg)
            if r["is_track"] and (not track_result["is_track"]
                                  or r["fit_score"] < track_result.get("fit_score", 1)):
                track_result = r
        if track_result["is_track"]:
            snap_m = track_cfg.get("distance_snap_m", 100)
            win_start = track_result.get("window_start_ts")
            win_end = track_result.get("window_end_ts")
            workout_name = activity.get("workout_name")
            is_race = _is_race_name(workout_name)
            is_workout = _is_workout_name(workout_name)

            # Estimate timestamps for intervals that lack them (e.g. XLSX splits)
            _estimate_interval_timestamps(intervals, streams)

            # First pass: label all overlapping intervals as track
            track_intervals = []
            for interval in intervals:
                if interval["is_recovery"] or not interval.get("gps_measured_distance_mi"):
                    continue
                iv_start = interval.get("start_timestamp_s")
                iv_end = interval.get("end_timestamp_s")
                if iv_start is not None and iv_end is not None and win_start is not None and win_end is not None:
                    if iv_end < win_start or iv_start > win_end:
                        continue
                elif win_start is not None:
                    continue

                interval["location_type"] = "track"
                interval["canonical_distance_mi"] = None  # clear stale
                interval["is_race"] = False                # clear stale
                summary["track_intervals"] += 1
                track_intervals.append(interval)

            # Second pass: snap distances based on activity type
            if is_race and track_intervals:
                # --- Race: snap to the race distance ---
                race_dist_m = _parse_race_distance_m(workout_name)
                race_time_s = _parse_race_time_s(workout_name)

                # Pick the interval closest to the race distance.
                # If no distance parsed, use the longest interval and
                # snap to the closest common race distance.
                if race_dist_m:
                    best = min(track_intervals, key=lambda iv:
                               abs(iv["gps_measured_distance_mi"] * METERS_PER_MILE - race_dist_m))
                else:
                    best = max(track_intervals, key=lambda iv:
                               iv["gps_measured_distance_mi"])
                    best_dist_m = best["gps_measured_distance_mi"] * METERS_PER_MILE
                    race_dist_m = _closest_race_distance_m(best_dist_m)

                best["canonical_distance_mi"] = round(race_dist_m / METERS_PER_MILE, 4)
                best["is_race"] = True
                if verbose:
                    parsed = "parsed" if _parse_race_distance_m(workout_name) else "closest"
                    print(f"    Race interval: {round(best['gps_measured_distance_mi'] * METERS_PER_MILE)}m"
                          f" → {round(race_dist_m)}m ({parsed})")
                    if race_time_s:
                        mins = int(race_time_s // 60)
                        secs = int(race_time_s % 60)
                        print(f"    Race time from name: {mins}:{secs:02d}")

            elif is_workout and track_intervals:
                # --- Workout: only snap work sets (faster than avg pace) ---
                paces = [
                    iv["avg_pace_s_per_mi"]
                    for iv in intervals  # all intervals, not just track
                    if iv.get("avg_pace_s_per_mi") and iv["avg_pace_s_per_mi"] > 0
                    and not iv.get("is_recovery")
                ]
                avg_pace = sum(paces) / len(paces) if paces else None

                for iv in track_intervals:
                    pace = iv.get("avg_pace_s_per_mi")
                    if avg_pace and pace and pace < avg_pace:
                        iv["canonical_distance_mi"] = snap_to_100m(
                            iv["gps_measured_distance_mi"], snap_m)
                        _recalc_pace(iv)

            else:
                # --- Generic: snap if 180m < distance <= 1300m ---
                for iv in track_intervals:
                    dist_m = iv["gps_measured_distance_mi"] * METERS_PER_MILE
                    if TRACK_SNAP_MIN_DISTANCE_M < dist_m <= TRACK_SNAP_MAX_DISTANCE_M:
                        iv["canonical_distance_mi"] = snap_to_100m(
                            iv["gps_measured_distance_mi"], snap_m)
                        _recalc_pace(iv)

            if verbose:
                method = track_result["method"]
                score = track_result["fit_score"]
                print(f"    Track detected ({method}, score={score})")

    # --- Measured course detection (Step 2) ---
    # Only apply to structured workouts — skip easy runs whose FIT auto-laps
    # happen to be near measured course distances.
    # Uses work-rep centroids per distance group (not activity centroid) to avoid
    # false matches when warmup/cooldown shifts the overall centroid.
    is_structured_activity = is_structured(activity_info)
    if is_structured_activity and streams and boundaries:
        # Ensure all intervals have estimated timestamps for centroid calc
        _estimate_interval_timestamps(intervals, streams)

        group_centroids = _compute_work_group_centroids(
            intervals, streams, boundaries
        )

        # For each distance group, check if its centroid matches a course
        matched_buckets: dict[int, list[dict]] = {}  # bucket → matching courses
        for bucket, (glat, glon) in group_centroids.items():
            courses = find_matching_courses(glat, glon, config)
            if courses:
                matched_buckets[bucket] = courses
                if verbose:
                    print(f"    {bucket}m group centroid ({glat:.5f}, {glon:.5f})"
                          f" near {[c['name'] for c in courses]}")

        if matched_buckets:
            for interval in intervals:
                if interval["is_recovery"] or interval.get("location_type"):
                    continue
                if interval.get("source") == "pace_segment":
                    continue
                gps_dist = interval.get("gps_measured_distance_mi")
                if not gps_dist:
                    continue
                bucket = round(gps_dist * METERS_PER_MILE / 100) * 100
                courses = matched_buckets.get(bucket)
                if not courses:
                    continue
                course = best_course_for_interval(gps_dist, courses)
                if course:
                    snap_m = course["snap_distance_m"]
                    interval["location_type"] = "measured_course"
                    interval["canonical_distance_mi"] = round(snap_m / METERS_PER_MILE, 4)
                    _recalc_pace(interval)
                    summary["measured_intervals"] += 1
                    if verbose:
                        raw_m = round(gps_dist * METERS_PER_MILE)
                        print(f"    Interval {raw_m}m → {round(snap_m)}m"
                              f" ({course.get('name', 'measured')})")

    # --- Workout tagging: recovery + set grouping (Step 2b) ---
    if is_structured_activity and boundaries:
        from runbase.analysis.workout_tagger import tag_workout_intervals
        tag_workout_intervals(intervals, boundaries)
        recovery_count = sum(1 for iv in intervals if iv.get("is_recovery"))
        set_count = len({iv.get("set_number") for iv in intervals if iv.get("set_number") is not None})
        summary["recovery_intervals"] = recovery_count
        summary["sets_tagged"] = set_count
        if verbose and (recovery_count or set_count):
            print(f"    Tagged {recovery_count} recoveries, {set_count} sets")

    # --- Walking scrub (Step 3) ---
    for interval in intervals:
        pace = interval.get("avg_pace_s_per_mi")
        if pace and pace >= walking_threshold:
            interval["is_walking"] = True
            summary["walking_intervals"] += 1

    # --- Stride detection (Step 4) ---
    # Only flag manually entered intervals (FIT/Strava laps, XLSX splits),
    # not auto-generated pace segments whose short duration is just a
    # transition between pace changes.
    for interval in intervals:
        if interval.get("source") == "pace_segment":
            continue
        duration = interval.get("duration_s")
        if duration and duration < stride_max and not interval["is_recovery"]:
            interval["is_stride"] = True
            summary["stride_intervals"] += 1

    # --- Pace zone assignment (Step 5) ---
    if boundaries:
        for interval in intervals:
            pace = interval.get("avg_pace_s_per_mi")
            if pace and pace > 0 and not interval.get("pace_zone"):
                zone = classify_pace(pace, boundaries)
                interval["pace_zone"] = zone
                summary["zones_assigned"] += 1

    # --- Update intervals in DB ---
    for interval in intervals:
        conn.execute(
            """UPDATE intervals
               SET pace_zone = ?, is_walking = ?, is_stride = ?,
                   is_race = ?, location_type = ?, canonical_distance_mi = ?,
                   avg_pace_s_per_mi = ?, avg_pace_display = ?,
                   is_recovery = ?, set_number = ?
               WHERE id = ?""",
            (interval.get("pace_zone"), interval.get("is_walking", False),
             interval.get("is_stride", False), interval.get("is_race", False),
             interval.get("location_type"),
             interval.get("canonical_distance_mi"),
             interval.get("avg_pace_s_per_mi"),
             interval.get("avg_pace_display"),
             interval.get("is_recovery", False),
             interval.get("set_number"),
             interval["id"]),
        )

    # --- Compute adjusted_distance_mi (Step 6) ---
    # Use pace_segment intervals if they exist, otherwise use original intervals.
    # This avoids double-counting when both FIT laps and pace segments exist.
    # Strides always count (even when their distance is absorbed into walking
    # pace_segments), so add stride distance from FIT/Strava laps separately.
    segment_intervals = [i for i in intervals if i.get("source") == "pace_segment"]
    distance_intervals = segment_intervals if segment_intervals else intervals
    non_walking_distance = sum(
        i.get("gps_measured_distance_mi") or 0
        for i in distance_intervals
        if not i.get("is_walking")
    )
    if segment_intervals:
        stride_distance = sum(
            i.get("gps_measured_distance_mi") or 0
            for i in intervals
            if i.get("is_stride") and i.get("source") != "pace_segment"
        )
        non_walking_distance += stride_distance
    adjusted_distance = round(non_walking_distance, 2) if distance_intervals else activity["distance_mi"]

    # --- Store VDOT + adjusted distance on activity ---
    conn.execute(
        "UPDATE activities SET adjusted_distance_mi = ?, vdot = ? WHERE id = ?",
        (adjusted_distance, vdot, activity_id),
    )

    conn.commit()

    if verbose:
        parts = []
        if summary["track_intervals"]:
            parts.append(f"{summary['track_intervals']} track")
        if summary["measured_intervals"]:
            parts.append(f"{summary['measured_intervals']} measured")
        if summary["recovery_intervals"]:
            parts.append(f"{summary['recovery_intervals']} recov")
        if summary["sets_tagged"]:
            parts.append(f"{summary['sets_tagged']} sets")
        if summary["walking_intervals"]:
            parts.append(f"{summary['walking_intervals']} walk")
        if summary["stride_intervals"]:
            parts.append(f"{summary['stride_intervals']} stride")
        if summary["zones_assigned"]:
            parts.append(f"{summary['zones_assigned']} zones")
        detail = ", ".join(parts) if parts else "no enrichment"
        print(f"  Activity #{activity_id} ({activity['date']}): {detail}")

    return summary


def enrich_batch(conn, config: dict, dry_run: bool = False,
                 verbose: bool = False) -> dict:
    """Batch enrich all activities.

    Returns:
        Summary dict with counts.
    """
    rows = conn.execute(
        "SELECT id FROM activities ORDER BY date"
    ).fetchall()

    result = {
        "total": len(rows),
        "enriched": 0,
        "skipped": 0,
        "track_intervals": 0,
        "measured_intervals": 0,
        "recovery_intervals": 0,
        "sets_tagged": 0,
        "walking_intervals": 0,
        "stride_intervals": 0,
        "zones_assigned": 0,
        "segments_created": 0,
    }

    if verbose:
        print(f"Enriching {len(rows)} activities...")

    for row in rows:
        activity_id = row[0]
        if dry_run:
            result["enriched"] += 1
            continue

        summary = enrich_activity(conn, activity_id, config, verbose=verbose)
        if summary["skipped"]:
            result["skipped"] += 1
        else:
            result["enriched"] += 1
            result["track_intervals"] += summary["track_intervals"]
            result["measured_intervals"] += summary["measured_intervals"]
            result["recovery_intervals"] += summary["recovery_intervals"]
            result["sets_tagged"] += summary["sets_tagged"]
            result["walking_intervals"] += summary["walking_intervals"]
            result["stride_intervals"] += summary["stride_intervals"]
            result["zones_assigned"] += summary["zones_assigned"]
            result["segments_created"] += summary["segments_created"]

    return result
