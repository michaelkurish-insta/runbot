"""GPS-based track detection using sliding window + OpenCV shape matching.

Detects whether an activity includes a portion run on a standard 400m track
by scanning a sliding window over the GPS stream, computing convex hulls,
and comparing them to a template oval contour via cv2.matchShapes.

Known track locations are cached in detected_tracks for fast lookup.
"""

import math
import numpy as np
import cv2

METERS_PER_MILE = 1609.344
METERS_PER_DEGREE_LAT = 111320.0

# Standard 400m track lane-1 dimensions
STRAIGHT_LENGTH = 84.39  # meters
TURN_RADIUS = 36.5       # meters


def _build_template_oval(S: float = STRAIGHT_LENGTH, R: float = TURN_RADIUS,
                         n_per_segment: int = 50) -> np.ndarray:
    """Build a 200-point contour of the standard 400m track oval.

    Returns an (N, 1, 2) float32 array suitable for cv2.matchShapes.
    """
    half_s = S / 2
    pts = []
    # Top straight: left to right
    for x in np.linspace(-half_s, half_s, n_per_segment):
        pts.append([x, R])
    # Right semicircle: top to bottom (center at half_s, 0)
    for angle in np.linspace(math.radians(90), math.radians(-90), n_per_segment):
        pts.append([half_s + R * math.cos(angle), R * math.sin(angle)])
    # Bottom straight: right to left
    for x in np.linspace(half_s, -half_s, n_per_segment):
        pts.append([x, -R])
    # Left semicircle: bottom to top (center at -half_s, 0)
    for angle in np.linspace(math.radians(270), math.radians(90), n_per_segment):
        pts.append([-half_s + R * math.cos(angle), R * math.sin(angle)])

    arr = np.array(pts, dtype=np.float32).reshape(-1, 1, 2)
    return arr


# Module-level template (computed once)
_TEMPLATE_OVAL = _build_template_oval()


def latlon_to_local_m(lat: float, lon: float,
                      center_lat: float, center_lon: float) -> tuple[float, float]:
    """Convert lat/lon to local meters relative to a center point."""
    dy = (lat - center_lat) * METERS_PER_DEGREE_LAT
    dx = (lon - center_lon) * METERS_PER_DEGREE_LAT * math.cos(math.radians(center_lat))
    return (dx, dy)


def snap_to_100m(distance_mi: float, snap_m: int = 100) -> float:
    """Snap a distance to the nearest N meters, return in miles."""
    distance_m = distance_mi * METERS_PER_MILE
    snapped_m = round(distance_m / snap_m) * snap_m
    return round(snapped_m / METERS_PER_MILE, 4)


def _check_known_tracks(conn, centroid_lat: float, centroid_lon: float,
                        radius_m: float = 200) -> dict | None:
    """Check if centroid is within radius_m of a known detected track.

    Returns the detected_tracks row as a dict, or None.
    """
    rows = conn.execute(
        "SELECT id, lat, lon, orientation_deg, fit_score FROM detected_tracks"
    ).fetchall()

    for row in rows:
        track_lat, track_lon = row[1], row[2]
        dx, dy = latlon_to_local_m(centroid_lat, centroid_lon, track_lat, track_lon)
        dist = math.sqrt(dx ** 2 + dy ** 2)
        if dist <= radius_m:
            return {
                "id": row[0], "lat": track_lat, "lon": track_lon,
                "orientation_deg": row[3], "fit_score": row[4],
            }
    return None


def _save_detected_track(conn, centroid_lat: float, centroid_lon: float,
                         orientation_deg: float, fit_score: float,
                         activity_id: int) -> None:
    """Save a newly detected track location."""
    conn.execute(
        """INSERT INTO detected_tracks
           (lat, lon, orientation_deg, fit_score, detected_by_activity_id)
           VALUES (?, ?, ?, ?, ?)""",
        (centroid_lat, centroid_lon, orientation_deg, fit_score, activity_id),
    )


def _score_window(points_m: np.ndarray, cfg: dict) -> dict | None:
    """Score a single window of GPS points (in local meters) against the oval template.

    Args:
        points_m: (N, 2) float32 array of points in local meters.
        cfg: track_detection config dict.

    Returns:
        Dict with score, dims, angle if passes all checks; None otherwise.
    """
    match_score_max = cfg.get("match_score_max", 0.15)
    min_short = cfg.get("min_short_axis_m", 50)
    max_short = cfg.get("max_short_axis_m", 120)
    min_long = cfg.get("min_long_axis_m", 120)
    max_long = cfg.get("max_long_axis_m", 220)
    min_aspect = cfg.get("min_aspect_ratio", 1.5)
    max_aspect = cfg.get("max_aspect_ratio", 3.0)
    min_fill = cfg.get("min_fill_ratio", 0.75)

    # Compute convex hull
    pts_cv = points_m.reshape(-1, 1, 2)
    hull = cv2.convexHull(pts_cv)

    if len(hull) < 5:
        return None

    # matchShapes score (lower = better match)
    score = cv2.matchShapes(_TEMPLATE_OVAL, hull, cv2.CONTOURS_MATCH_I1, 0)
    if score > match_score_max:
        return None

    # Dimension check via minAreaRect
    rect = cv2.minAreaRect(hull)
    w, h = rect[1]  # (width, height) — not necessarily ordered
    short_axis = min(w, h)
    long_axis = max(w, h)
    angle = rect[2]

    if short_axis < min_short or short_axis > max_short:
        return None
    if long_axis < min_long or long_axis > max_long:
        return None

    if short_axis > 0:
        aspect = long_axis / short_axis
        if aspect < min_aspect or aspect > max_aspect:
            return None
    else:
        return None

    # Fill ratio: hull area / bounding rect area
    # Real track ovals fill ~0.88; narrow straight-line shapes fill ~0.57
    hull_area = cv2.contourArea(hull)
    rect_area = short_axis * long_axis
    if rect_area > 0:
        fill_ratio = hull_area / rect_area
        if fill_ratio < min_fill:
            return None
    else:
        return None

    return {
        "score": score,
        "short_axis": short_axis,
        "long_axis": long_axis,
        "angle": angle,
    }


def detect_track_activity(conn, activity_id: int, intervals: list[dict],
                          streams: list[dict], config: dict | None = None
                          ) -> dict:
    """Detect if an activity includes a track portion using sliding window + OpenCV.

    Uses a sliding window to scan the GPS stream, computing convex hull shape
    matching against a standard 400m oval template. This approach handles
    activities with warmup/cooldown segments by isolating the track portion.

    Args:
        conn: SQLite connection (for known-track lookup and saving).
        activity_id: The activity ID.
        intervals: List of interval dicts.
        streams: List of stream dicts with timestamp_s, lat, lon.
        config: Optional paces.track_detection config dict.

    Returns:
        Dict with keys: is_track, fit_score, orientation_deg, method,
        window_start_ts, window_end_ts.
    """
    result = {
        "is_track": False, "fit_score": 0.0, "orientation_deg": None,
        "method": None, "window_start_ts": None, "window_end_ts": None,
    }

    cfg = config or {}
    max_bbox_m = cfg.get("max_bbox_m", 300)
    known_radius = cfg.get("known_track_radius_m", 200)
    window_size = cfg.get("window_size", 300)
    window_step = cfg.get("window_step", 50)

    # Extract GPS points with timestamps
    gps_points = [
        (s["timestamp_s"], s["lat"], s["lon"])
        for s in streams
        if (s.get("lat") is not None and s.get("lon") is not None
            and s.get("timestamp_s") is not None)
    ]

    if len(gps_points) < window_size:
        return result

    best_window = None
    best_score = float("inf")

    for start_idx in range(0, len(gps_points) - window_size + 1, window_step):
        window = gps_points[start_idx:start_idx + window_size]
        lats = [p[1] for p in window]
        lons = [p[2] for p in window]

        # Window centroid
        c_lat = sum(lats) / len(lats)
        c_lon = sum(lons) / len(lons)

        # Convert to local meters
        local_pts = np.array(
            [latlon_to_local_m(lat, lon, c_lat, c_lon) for lat, lon in zip(lats, lons)],
            dtype=np.float32,
        )

        # Bbox pre-filter
        xs = local_pts[:, 0]
        ys = local_pts[:, 1]
        bbox_x = float(xs.max() - xs.min())
        bbox_y = float(ys.max() - ys.min())
        if bbox_x > max_bbox_m or bbox_y > max_bbox_m:
            continue

        # Check known tracks
        known = _check_known_tracks(conn, c_lat, c_lon, known_radius)
        if known:
            result["is_track"] = True
            result["fit_score"] = known["fit_score"]
            result["orientation_deg"] = known["orientation_deg"]
            result["method"] = "known"
            result["window_start_ts"] = window[0][0]
            result["window_end_ts"] = window[-1][0]
            return result

        # OpenCV shape matching
        win_result = _score_window(local_pts, cfg)
        if win_result and win_result["score"] < best_score:
            best_score = win_result["score"]
            best_window = {
                "start_ts": window[0][0],
                "end_ts": window[-1][0],
                "centroid_lat": c_lat,
                "centroid_lon": c_lon,
                "score": win_result["score"],
                "angle": win_result["angle"],
                "short_axis": win_result["short_axis"],
                "long_axis": win_result["long_axis"],
            }

    if best_window:
        result["is_track"] = True
        result["fit_score"] = round(best_window["score"], 4)
        result["orientation_deg"] = best_window["angle"]
        result["method"] = "fitted"
        result["window_start_ts"] = best_window["start_ts"]
        result["window_end_ts"] = best_window["end_ts"]

        _save_detected_track(
            conn, best_window["centroid_lat"], best_window["centroid_lon"],
            best_window["angle"], result["fit_score"], activity_id,
        )

    return result
