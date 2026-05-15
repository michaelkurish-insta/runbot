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
    effective_vdot_from_gap_and_hr,
)
from runbase.analysis.track_detect import (
    detect_track_activity, snap_to_100m,
)
from runbase.analysis.pace_segments import is_structured, segment_by_pace
from runbase.analysis.locations import find_matching_courses, best_course_for_interval

METERS_PER_MILE = 1609.344
FT_PER_M = 3.28084


def _minetti_cost_ratio(grade: float) -> float:
    """Minetti 2002 energy cost ratio vs flat running.

    grade: decimal gradient (0.10 = 10%).
    Returns cost_ratio clamped to a minimum of 0.785 (Strava's revised
    downhill cap, ~12% max benefit peaking around -9% grade).
    """
    i = max(-0.5, min(0.5, grade))
    cr = 155.4*i**5 - 30.4*i**4 - 43.3*i**3 + 46.3*i**2 + 19.5*i + 3.6
    ratio = cr / 3.6
    return max(0.785, ratio)


def _compute_gap(streams: list[dict]) -> float | None:
    """Compute grade-adjusted pace from stream altitude + distance data.

    Uses 30-meter distance windows for grade smoothing and the Minetti 2002
    energy cost polynomial with a capped downhill benefit.

    Returns gap_s_per_mi or None if insufficient data.
    """
    # Filter to points with valid altitude and distance
    pts = [
        s for s in streams
        if s.get("altitude_ft") is not None and s.get("distance_mi") is not None
            and s.get("timestamp_s") is not None
    ]
    if len(pts) < 10:
        return None

    total_time = 0.0
    total_adj_dist = 0.0
    WINDOW_M = 30.0  # grade smoothing window in meters

    for idx in range(1, len(pts)):
        d_mi = pts[idx]["distance_mi"] - pts[idx - 1]["distance_mi"]
        dt = pts[idx]["timestamp_s"] - pts[idx - 1]["timestamp_s"]
        if d_mi <= 0 or dt <= 0:
            continue

        d_m = d_mi * METERS_PER_MILE

        # Compute grade over ~30m window ending at this point
        alt_m = pts[idx]["altitude_ft"] / FT_PER_M
        # Walk backwards to find a point ~30m earlier
        window_start = idx
        accum_m = 0.0
        for j in range(idx - 1, -1, -1):
            seg_m = (pts[j + 1]["distance_mi"] - pts[j]["distance_mi"]) * METERS_PER_MILE
            accum_m += seg_m
            window_start = j
            if accum_m >= WINDOW_M:
                break

        if accum_m > 0:
            alt_start_m = pts[window_start]["altitude_ft"] / FT_PER_M
            grade = (alt_m - alt_start_m) / accum_m
            grade = max(-0.5, min(0.5, grade))
        else:
            grade = 0.0

        cost_ratio = _minetti_cost_ratio(grade)
        total_adj_dist += d_mi * cost_ratio
        total_time += dt

    if total_adj_dist <= 0 or total_time <= 0:
        return None

    return total_time / total_adj_dist  # seconds per adjusted mile


def _compute_elevation(streams: list[dict]) -> tuple[float | None, float | None]:
    """Compute total elevation gain and loss from stream altitude data.

    Returns (gain_ft, loss_ft) or (None, None) if insufficient data.
    Uses a 3-point smoothing to reduce GPS altitude noise.
    """
    alts = [s["altitude_ft"] for s in streams if s.get("altitude_ft") is not None]
    if len(alts) < 10:
        return None, None

    # 3-point moving average to smooth GPS noise
    smoothed = []
    for i in range(len(alts)):
        if i == 0:
            smoothed.append((alts[0] + alts[1]) / 2)
        elif i == len(alts) - 1:
            smoothed.append((alts[-2] + alts[-1]) / 2)
        else:
            smoothed.append((alts[i - 1] + alts[i] + alts[i + 1]) / 3)

    gain = 0.0
    loss = 0.0
    for i in range(1, len(smoothed)):
        diff = smoothed[i] - smoothed[i - 1]
        if diff > 0:
            gain += diff
        else:
            loss -= diff  # loss stored as positive

    return round(gain, 1), round(loss, 1)


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
        "walking_threshold_s_per_mi": paces.get("walking_threshold_s_per_mi", 840),
        "stride_max_duration_s": paces.get("stride_max_duration_s", 30),
        "track_detection": paces.get("track_detection", {}),
        "measured_courses": paces.get("measured_courses", []),
    }


def _load_activity(conn, activity_id: int) -> dict | None:
    """Load activity row as a dict."""
    row = conn.execute(
        """SELECT id, date, distance_mi, duration_s, workout_category, workout_name,
                  total_ascent_ft, total_descent_ft, avg_hr, start_time,
                  temperature_f, humidity_pct, weather_conditions, cloud_cover_pct,
                  suppress_hr
           FROM activities WHERE id = ?""",
        (activity_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0], "date": row[1], "distance_mi": row[2],
        "duration_s": row[3], "workout_category": row[4], "workout_name": row[5],
        "total_ascent_ft": row[6], "total_descent_ft": row[7], "avg_hr": row[8],
        "start_time": row[9], "temperature_f": row[10], "humidity_pct": row[11],
        "weather_conditions": row[12], "cloud_cover_pct": row[13],
        "suppress_hr": row[14],
    }


def _load_intervals(conn, activity_id: int) -> list[dict]:
    """Load existing intervals for an activity."""
    rows = conn.execute(
        """SELECT id, rep_number, gps_measured_distance_mi, canonical_distance_mi,
                  duration_s, avg_pace_s_per_mi, avg_pace_display, avg_hr, avg_cadence,
                  is_recovery, start_timestamp_s, end_timestamp_s, source, is_race,
                  set_number, elapsed_pace_zone, is_stride, is_walking, is_hill_sprint,
                  pace_zone, location_type
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
            "elapsed_pace_zone": r[15],
            "is_stride": bool(r[16]) if r[16] else False,
            "is_walking": bool(r[17]) if r[17] else False,
            "is_hill_sprint": bool(r[18]) if r[18] else False,
            "pace_zone": r[19],
            "location_type": r[20],
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


def _has_workout_fit_laps(conn, activity_id: int) -> bool:
    """Check if an activity has FIT/Strava laps with a workout-like pace pattern.

    Detects structured workouts by looking for bimodal pace distribution
    (alternating fast reps + slow recoveries, or fast reps + walking).
    Requires >=4 laps and slowest >= 1.5x fastest pace.
    """
    rows = conn.execute(
        """SELECT avg_pace_s_per_mi FROM intervals
           WHERE activity_id = ?
             AND source IN ('fit_lap', 'strava_lap')
             AND avg_pace_s_per_mi IS NOT NULL
             AND avg_pace_s_per_mi > 0""",
        (activity_id,),
    ).fetchall()
    if len(rows) < 4:
        return False
    paces = [r[0] for r in rows]
    return max(paces) >= min(paces) * 1.5


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
        "hill_sprint_intervals": 0,
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
        "has_workout_fit_laps": _has_workout_fit_laps(conn, activity_id),
    }

    intervals = _load_intervals(conn, activity_id)

    # Reset enrichment fields for non-pace-segment intervals so re-enrichment
    # starts clean (e.g. stale canonical_distance_mi from a previous run).
    # Preserve is_walking on manual intervals — user edits are canonical.
    conn.execute(
        """UPDATE intervals SET
               canonical_distance_mi = NULL, location_type = NULL,
               is_recovery = 0, set_number = NULL,
               is_walking = 0, is_stride = 0, is_hill_sprint = 0
           WHERE activity_id = ? AND source NOT IN ('pace_segment', 'manual')""",
        (activity_id,),
    )
    conn.execute(
        """UPDATE intervals SET
               location_type = NULL,
               is_recovery = 0, set_number = NULL
           WHERE activity_id = ? AND source = 'manual'""",
        (activity_id,),
    )
    # Refresh in-memory intervals after reset
    intervals = _load_intervals(conn, activity_id)

    # Always clean up stale pace segments before deciding whether to regenerate.
    # A previous run may have classified this activity as unstructured, but a
    # code change (e.g. improved workout detection) may now classify it as
    # structured — stale segments must not linger.
    conn.execute(
        "DELETE FROM intervals WHERE activity_id = ? AND source = 'pace_segment'",
        (activity_id,),
    )

    if not is_structured(activity_info) and streams and boundaries:
        # Unstructured: create pace segments from streams

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

    # --- Workout tagging: recovery + set grouping (Step 1) ---
    # Must run before track detection (needs is_recovery / set_number)
    # and measured course detection (needs is_recovery).
    is_structured_activity = is_structured(activity_info)
    if is_structured_activity and boundaries:
        from runbase.analysis.workout_tagger import tag_workout_intervals
        tag_workout_intervals(intervals, boundaries)
        recovery_count = sum(1 for iv in intervals if iv.get("is_recovery"))
        set_count = len({iv.get("set_number") for iv in intervals if iv.get("set_number") is not None})
        summary["recovery_intervals"] = recovery_count
        summary["sets_tagged"] = set_count
        if verbose and (recovery_count or set_count):
            print(f"    Tagged {recovery_count} recoveries, {set_count} sets")

    # --- Track detection (Step 2) ---
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

            # First pass: label track intervals.
            # Tag intervals that either temporally overlap the detected window
            # OR are part of a workout set (set_number is set).  When you run
            # to a track, the detection window may only cover the warmup laps,
            # but the work sets are also on the track.
            track_intervals = []
            for interval in intervals:
                if interval["is_recovery"] or not interval.get("gps_measured_distance_mi"):
                    continue
                in_set = interval.get("set_number") is not None
                iv_start = interval.get("start_timestamp_s")
                iv_end = interval.get("end_timestamp_s")
                overlaps_window = False
                if iv_start is not None and iv_end is not None and win_start is not None and win_end is not None:
                    overlaps_window = not (iv_end < win_start or iv_start > win_end)
                if not overlaps_window and not in_set:
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

                best["canonical_distance_mi"] = round(race_dist_m / METERS_PER_MILE, 6)
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

    # --- Measured course detection (Step 3) ---
    # Only apply to structured workouts — skip easy runs whose FIT auto-laps
    # happen to be near measured course distances.
    # Uses work-rep centroids per distance group (not activity centroid) to avoid
    # false matches when warmup/cooldown shifts the overall centroid.
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
                    interval["canonical_distance_mi"] = round(snap_m / METERS_PER_MILE, 6)
                    _recalc_pace(interval)
                    summary["measured_intervals"] += 1
                    if verbose:
                        raw_m = round(gps_dist * METERS_PER_MILE)
                        print(f"    Interval {raw_m}m → {round(snap_m)}m"
                              f" ({course.get('name', 'measured')})")

    # --- Walking scrub (Step 4) ---
    # Skip pace segments (instantaneous pace unreliable) and manual intervals
    # (user edits are canonical — their is_walking state was preserved above).
    for interval in intervals:
        if interval.get("source") in ("pace_segment", "manual"):
            continue
        pace = interval.get("avg_pace_s_per_mi")
        if pace and pace >= walking_threshold:
            interval["is_walking"] = True
            summary["walking_intervals"] += 1

    # --- Hill sprint detection (Step 5) ---
    # Very short intervals (< 50m) with uphill elevation gain.
    # Runs BEFORE stride detection so hills take priority.
    # Overrides is_recovery so the workout tagger's pace-based
    # classification doesn't gray them out.
    if streams:
        hill_sprint_max_mi = 0.035  # ~56m
        for interval in intervals:
            if interval.get("source") == "pace_segment":
                continue
            dist = interval.get("gps_measured_distance_mi") or 0
            if dist <= 0 or dist > hill_sprint_max_mi:
                continue
            pace = interval.get("avg_pace_s_per_mi")
            if not pace or pace >= walking_threshold:
                continue  # walking, not a sprint
            # Check elevation gain from stream data
            iv_start = interval.get("start_timestamp_s")
            iv_end = interval.get("end_timestamp_s")
            if iv_start is None or iv_end is None:
                continue
            iv_alts = [s["altitude_ft"] for s in streams
                       if s.get("altitude_ft") is not None
                       and s.get("timestamp_s") is not None
                       and iv_start <= s["timestamp_s"] <= iv_end]
            if len(iv_alts) >= 2 and iv_alts[-1] > iv_alts[0]:
                interval["is_hill_sprint"] = True
                interval["is_recovery"] = False
                summary["hill_sprint_intervals"] += 1

    # --- Stride detection (Step 5b) ---
    # Only flag manually lapped intervals, not pace segments, hill sprints,
    # or auto-lap trailing runts.  Minimum distance of 0.015 mi (~24m)
    # excludes tiny stubs from watch start/stop.
    # Deduplicate: if both null-source (FIT) and strava_lap exist for the
    # same laps, prefer FIT — its higher GPS resolution gives more accurate
    # pace for very short intervals like strides.
    stride_min_mi = 0.015  # ~24m — excludes watch start/stop stubs
    # Skip pace_segment and manual intervals — user edits are canonical.
    non_seg = [i for i in intervals if i.get("source") not in ("pace_segment", "manual")]
    has_null_src = any(i.get("source") is None for i in non_seg)
    has_strava_src = any(i.get("source") == "strava_lap" for i in non_seg)
    if has_null_src and has_strava_src:
        stride_candidates = [i for i in non_seg if i.get("source") is None]
    else:
        stride_candidates = non_seg

    # Exclude auto-lap trailing runts: if the non-last laps have uniform
    # distance (> 0.5 mi median), the final short segment is an artifact.
    if len(stride_candidates) >= 3:
        by_time = sorted(stride_candidates, key=lambda i: i.get("start_timestamp_s") or 0)
        other_dists = [i.get("gps_measured_distance_mi") or 0 for i in by_time[:-1]]
        median_dist = sorted(other_dists)[len(other_dists) // 2]
        last_dist = by_time[-1].get("gps_measured_distance_mi") or 0
        if median_dist > 0.5 and last_dist < median_dist * 0.5:
            stride_candidates = [i for i in stride_candidates if i is not by_time[-1]]

    for interval in stride_candidates:
        duration = interval.get("duration_s")
        dist = interval.get("gps_measured_distance_mi") or 0
        if (duration and duration < stride_max
                and dist >= stride_min_mi
                and not interval["is_recovery"]
                and not interval.get("is_hill_sprint")
                and not interval.get("is_walking")):
            interval["is_stride"] = True
            summary["stride_intervals"] += 1

    # Propagate stride/hill flags to duplicate Strava laps.
    # Stride detection prefers FIT laps (higher GPS resolution), but the
    # corresponding Strava laps represent the same intervals and need
    # matching flags so they don't leak into workout type computation.
    if has_null_src and has_strava_src:
        stride_reps = {i["rep_number"] for i in non_seg
                       if i.get("source") is None and i.get("is_stride")}
        hill_reps = {i["rep_number"] for i in non_seg
                     if i.get("source") is None and i.get("is_hill_sprint")}
        for i in non_seg:
            if i.get("source") == "strava_lap":
                if i["rep_number"] in stride_reps:
                    i["is_stride"] = True
                if i["rep_number"] in hill_reps:
                    i["is_hill_sprint"] = True

    # --- Pace zone assignment (Step 6) ---
    if boundaries:
        for interval in intervals:
            pace = interval.get("avg_pace_s_per_mi")
            if pace and pace > 0 and not interval.get("pace_zone"):
                zone = classify_pace(pace, boundaries)
                interval["pace_zone"] = zone
                summary["zones_assigned"] += 1

    # --- Elapsed pace zone for pace segments (Step 6b) ---
    # Compute overall elapsed pace (total distance / total time) and classify it.
    # This gives a more accurate effort score for pace segments than instantaneous
    # pace, which can be distorted by hills, GPS noise, wind, etc.
    if boundaries:
        total_dist = activity.get("distance_mi")
        total_dur = activity.get("duration_s")
        if total_dist and total_dur and total_dist > 0:
            elapsed_pace = total_dur / total_dist
            elapsed_zone = classify_pace(elapsed_pace, boundaries)
            for interval in intervals:
                if interval.get("source") == "pace_segment":
                    interval["elapsed_pace_zone"] = elapsed_zone

    # --- Update intervals in DB ---
    for interval in intervals:
        conn.execute(
            """UPDATE intervals
               SET pace_zone = ?, is_walking = ?, is_stride = ?,
                   is_race = ?, location_type = ?, canonical_distance_mi = ?,
                   avg_pace_s_per_mi = ?, avg_pace_display = ?,
                   is_recovery = ?, set_number = ?, elapsed_pace_zone = ?,
                   is_hill_sprint = ?
               WHERE id = ?""",
            (interval.get("pace_zone"), interval.get("is_walking", False),
             interval.get("is_stride", False), interval.get("is_race", False),
             interval.get("location_type"),
             interval.get("canonical_distance_mi"),
             interval.get("avg_pace_s_per_mi"),
             interval.get("avg_pace_display"),
             interval.get("is_recovery", False),
             interval.get("set_number"),
             interval.get("elapsed_pace_zone"),
             interval.get("is_hill_sprint", False),
             interval["id"]),
        )

    # --- Compute adjusted_distance_mi (Step 7) ---
    # Use pace_segment intervals if they exist and cover most of the activity,
    # otherwise fall back to FIT/Strava laps.
    # When both NULL-source (FIT) and strava_lap intervals exist for the same
    # reps, prefer strava_lap to avoid double-counting.
    segment_intervals = [i for i in intervals if i.get("source") == "pace_segment"]
    seg_total = sum(i.get("gps_measured_distance_mi") or 0 for i in segment_intervals)
    act_dist = activity.get("distance_mi") or 0
    # Only use pace_segments for adjusted distance when they cover most of the
    # activity.  Group-matched activities may have streams (and thus pace
    # segments) for only some sub-activities, producing a fraction of the real
    # distance.  In that case fall back to strava_laps / original distance.
    seg_ratio = (seg_total / act_dist) if act_dist > 0 else 0
    if segment_intervals and 0.7 <= seg_ratio <= 1.15:
        distance_intervals = segment_intervals
    else:
        # Exclude pace_segments — they only cover part of the activity
        non_seg = [i for i in intervals if i.get("source") != "pace_segment"]
        # Deduplicate: if both NULL-source and strava_lap exist, use strava_lap
        has_null = any(i.get("source") is None for i in non_seg)
        has_strava = any(i.get("source") == "strava_lap" for i in non_seg)
        if has_null and has_strava:
            distance_intervals = [i for i in non_seg if i.get("source") == "strava_lap"]
        else:
            distance_intervals = non_seg if non_seg else intervals
    non_walking_distance = sum(
        i.get("gps_measured_distance_mi") or 0
        for i in distance_intervals
        if not i.get("is_walking")
    )
    walking_duration = sum(
        i.get("duration_s") or 0
        for i in distance_intervals
        if i.get("is_walking")
    )
    adjusted_distance = round(non_walking_distance, 2) if distance_intervals else activity["distance_mi"]

    # --- Fix duration for multi-source activities (Step 7b) ---
    # Group-matched activities may have duration_s from only one sub-activity.
    # Recompute as the sum of all Strava source durations when that sum exceeds
    # the stored value (i.e. the stored value is partial or zero).
    src_dur_row = conn.execute(
        """SELECT SUM(duration_s) FROM activity_sources
           WHERE activity_id = ? AND source = 'strava' AND duration_s > 0""",
        (activity_id,),
    ).fetchone()
    src_dur_sum = src_dur_row[0] if src_dur_row and src_dur_row[0] else 0
    act_dur = activity.get("duration_s") or 0
    if src_dur_sum > act_dur * 1.1:
        act_dur = src_dur_sum

    # --- Store VDOT + adjusted distance + pace on activity ---
    # --- Update activity strides count from detected stride intervals ---
    # Count from preferred source only (FIT > Strava), matching the UI display logic.
    non_seg = [i for i in intervals if i.get("source") != "pace_segment"]
    _has_fit = any(i.get("source") is None for i in non_seg)
    _has_strava = any(i.get("source") == "strava_lap" for i in non_seg)
    if _has_fit and _has_strava:
        visible = [i for i in non_seg if i.get("source") != "strava_lap"]
    else:
        visible = non_seg
    stride_count = sum(1 for i in visible if i.get("is_stride"))
    stride_count = stride_count or None  # store NULL if zero

    # Also count hill sprints for the activity record
    hill_sprint_count = sum(1 for i in intervals if i.get("is_hill_sprint"))
    hill_sprint_count = hill_sprint_count or None

    # Respect manual overrides from the GUI — they are canonical.
    overrides = {
        r[0]: r[1] for r in conn.execute(
            "SELECT field_name, override_value FROM activity_overrides WHERE activity_id = ?",
            (activity_id,),
        ).fetchall()
    }

    # Respect overrides — they are canonical and must not be overwritten.
    if "distance_mi" in overrides:
        adjusted_distance = float(overrides["distance_mi"])
    if "strides" in overrides:
        stride_count = int(overrides["strides"]) or None

    # Compute running pace = running_time / adjusted_distance.
    # Running time excludes walking duration so the pace reflects actual running.
    running_time = act_dur - walking_duration
    if "avg_pace_s_per_mi" in overrides:
        avg_pace = float(overrides["avg_pace_s_per_mi"])
        tenths = round(avg_pace * 10)
        mins, secs_tenths = divmod(tenths, 600)
        avg_pace_display = f"{mins}:{secs_tenths / 10:04.1f}"
    elif adjusted_distance and adjusted_distance > 0 and running_time > 0:
        avg_pace = running_time / adjusted_distance
        tenths = round(avg_pace * 10)
        mins, secs_tenths = divmod(tenths, 600)
        avg_pace_display = f"{mins}:{secs_tenths / 10:04.1f}"
    else:
        avg_pace = activity.get("avg_pace_s_per_mi")
        avg_pace_display = None

    # --- Compute grade-adjusted pace (GAP) from stream altitude data ---
    gap = _compute_gap(streams)

    # --- Compute elevation gain/loss ---
    # No ingest source provides descent (column doesn't exist on
    # activity_sources), so any value already on the activity was written by a
    # prior enrichment from noisy GPS stream altitude.  Use the device's
    # barometric ascent as the canonical source; approximate descent = ascent
    # (valid for loop / out-and-back runs).  Only fall back to stream-computed
    # values when the device didn't report ascent at all.
    stream_gain, stream_loss = _compute_elevation(streams)
    src_ascent = activity.get("total_ascent_ft")
    if src_ascent is not None:
        ascent = src_ascent
        descent = src_ascent  # best available proxy
    else:
        ascent = stream_gain
        descent = stream_loss

    # --- Compute per-activity VDOT estimate from GAP + HR ---
    # Skip if HR is suppressed (sensor failure) — CVD would be unreliable.
    hr_max = config.get("athlete", {}).get("hr_max")
    if activity.get("suppress_hr"):
        computed_vdot = None
    else:
        computed_vdot = effective_vdot_from_gap_and_hr(
            gap, activity.get("avg_hr"), hr_max,
        )

    # --- Weather enrichment ---
    # Fetch if any weather field is missing (not just temperature).
    temperature_f = activity.get("temperature_f")
    humidity_pct = activity.get("humidity_pct")
    weather_conditions = activity.get("weather_conditions")
    cloud_cover_pct = activity.get("cloud_cover_pct")
    weather_incomplete = (temperature_f is None or humidity_pct is None
                          or cloud_cover_pct is None)
    if weather_incomplete and activity.get("start_time") and activity.get("duration_s"):
        from datetime import datetime, timedelta
        from runbase.analysis.weather import fetch_weather

        try:
            start_dt = datetime.fromisoformat(activity["start_time"])
        except (ValueError, TypeError):
            start_dt = None

        if start_dt and streams:
            target_offset = activity["duration_s"] * 2 / 3
            # Find GPS coords from streams at closest timestamp to 2/3 offset
            first_ts = streams[0].get("timestamp_s") or 0
            target_ts = first_ts + target_offset
            best_pt = None
            best_diff = float("inf")
            for s in streams:
                if s.get("lat") is None or s.get("timestamp_s") is None:
                    continue
                diff = abs(s["timestamp_s"] - target_ts)
                if diff < best_diff:
                    best_diff = diff
                    best_pt = s

            if best_pt and best_pt.get("lat") is not None:
                target_dt = start_dt + timedelta(seconds=target_offset)
                weather = fetch_weather(
                    best_pt["lat"], best_pt["lon"], target_dt, verbose=verbose,
                )
                if weather:
                    temperature_f = weather.get("temperature_f") or temperature_f
                    humidity_pct = weather.get("humidity_pct") or humidity_pct
                    weather_conditions = weather.get("weather_conditions") or weather_conditions
                    cloud_cover_pct = weather.get("cloud_cover_pct") or cloud_cover_pct

    if "distance_mi" in overrides:
        conn.execute(
            "UPDATE activities SET vdot = ?, duration_s = ?, "
            "avg_pace_s_per_mi = ?, avg_pace_display = ?, strides = ?, "
            "gap_s_per_mi = ?, total_ascent_ft = ?, total_descent_ft = ?, "
            "computed_vdot = ?, temperature_f = ?, humidity_pct = ?, "
            "weather_conditions = ?, cloud_cover_pct = ? WHERE id = ?",
            (vdot, act_dur or activity.get("duration_s"), avg_pace, avg_pace_display,
             stride_count, gap, ascent, descent, computed_vdot,
             temperature_f, humidity_pct, weather_conditions, cloud_cover_pct,
             activity_id),
        )
    else:
        conn.execute(
            "UPDATE activities SET adjusted_distance_mi = ?, vdot = ?, duration_s = ?, "
            "avg_pace_s_per_mi = ?, avg_pace_display = ?, strides = ?, "
            "gap_s_per_mi = ?, total_ascent_ft = ?, total_descent_ft = ?, "
            "computed_vdot = ?, temperature_f = ?, humidity_pct = ?, "
            "weather_conditions = ?, cloud_cover_pct = ? WHERE id = ?",
            (adjusted_distance, vdot, act_dur or activity.get("duration_s"),
             avg_pace, avg_pace_display, stride_count, gap, ascent, descent,
             computed_vdot, temperature_f, humidity_pct, weather_conditions,
             cloud_cover_pct, activity_id),
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
        if summary["hill_sprint_intervals"]:
            parts.append(f"{summary['hill_sprint_intervals']} hill")
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
        "hill_sprint_intervals": 0,
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
            result["hill_sprint_intervals"] += summary["hill_sprint_intervals"]
            result["zones_assigned"] += summary["zones_assigned"]
            result["segments_created"] += summary["segments_created"]

    return result
