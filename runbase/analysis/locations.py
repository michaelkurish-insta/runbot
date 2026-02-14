"""Workout location clustering and measured course detection.

Clusters workout GPS centroids to help identify common workout locations.
Also checks if a given lat/lon is within a whitelisted measured course.
"""

import math

METERS_PER_DEGREE_LAT = 111320.0


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute distance in meters between two lat/lon points."""
    R = 6371000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def cluster_workout_locations(conn, min_intervals: int = 3,
                               cluster_radius_m: float = 500) -> list[dict]:
    """Cluster workout GPS centroids from activities with intervals.

    Args:
        conn: SQLite connection.
        min_intervals: Minimum number of work intervals for an activity to be included.
        cluster_radius_m: Maximum distance between centroids to form a cluster.

    Returns:
        List of cluster dicts: {center_lat, center_lon, count, activities: [{id, date, name}]}
    """
    # Get activities with enough intervals and GPS data
    rows = conn.execute("""
        SELECT a.id, a.date, a.workout_name,
               AVG(s.lat) as avg_lat, AVG(s.lon) as avg_lon
        FROM activities a
        JOIN intervals i ON i.activity_id = a.id
        JOIN streams s ON s.activity_id = a.id
        WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
          AND i.is_recovery = 0
        GROUP BY a.id
        HAVING COUNT(DISTINCT i.id) >= ?
           AND AVG(s.lat) IS NOT NULL
    """, (min_intervals,)).fetchall()

    if not rows:
        return []

    # Build list of workout points
    points = []
    for row in rows:
        points.append({
            "id": row[0],
            "date": row[1],
            "name": row[2],
            "lat": row[3],
            "lon": row[4],
        })

    # Simple greedy clustering
    clusters = []
    assigned = set()

    for p in points:
        if p["id"] in assigned:
            continue

        # Start a new cluster with this point
        cluster_points = [p]
        assigned.add(p["id"])

        for q in points:
            if q["id"] in assigned:
                continue
            if haversine_m(p["lat"], p["lon"], q["lat"], q["lon"]) <= cluster_radius_m:
                cluster_points.append(q)
                assigned.add(q["id"])

        # Compute cluster center
        center_lat = sum(cp["lat"] for cp in cluster_points) / len(cluster_points)
        center_lon = sum(cp["lon"] for cp in cluster_points) / len(cluster_points)

        clusters.append({
            "center_lat": round(center_lat, 4),
            "center_lon": round(center_lon, 4),
            "count": len(cluster_points),
            "activities": [{"id": cp["id"], "date": cp["date"], "name": cp["name"]}
                          for cp in sorted(cluster_points, key=lambda x: x["date"])],
        })

    # Sort by count descending
    clusters.sort(key=lambda c: c["count"], reverse=True)
    return clusters


def is_measured_course(lat: float, lon: float, config: dict) -> bool:
    """Check if a lat/lon falls within any whitelisted measured course.

    Args:
        lat: Latitude of the activity/interval centroid.
        lon: Longitude of the activity/interval centroid.
        config: Full config dict (expects paces.measured_courses list).

    Returns:
        True if within a measured course radius.
    """
    return len(find_matching_courses(lat, lon, config)) > 0


def find_matching_courses(lat: float, lon: float, config: dict) -> list[dict]:
    """Return all measured courses whose radius covers the given lat/lon.

    Each course dict has: name, lat, lon, radius_m, snap_distance_m.
    """
    courses = config.get("paces", {}).get("measured_courses", [])
    matches = []
    for course in courses:
        course_lat = course.get("lat")
        course_lon = course.get("lon")
        radius = course.get("radius_m", 500)
        if course_lat is None or course_lon is None:
            continue
        if haversine_m(lat, lon, course_lat, course_lon) <= radius:
            matches.append(course)
    return matches


METERS_PER_MILE = 1609.344


def best_course_for_interval(gps_distance_mi: float, courses: list[dict],
                             tolerance_pct: float = 20.0) -> dict | None:
    """Find the measured course whose snap_distance_m best matches the interval.

    Args:
        gps_distance_mi: GPS-measured interval distance in miles.
        courses: List of course dicts (from find_matching_courses).
        tolerance_pct: Max percentage difference to accept a match.

    Returns:
        Best matching course dict, or None.
    """
    if not gps_distance_mi or not courses:
        return None

    gps_m = gps_distance_mi * METERS_PER_MILE
    best = None
    best_pct = tolerance_pct

    for course in courses:
        snap_m = course.get("snap_distance_m")
        if not snap_m:
            continue
        pct = abs(gps_m - snap_m) / snap_m * 100
        if pct <= best_pct:
            best_pct = pct
            best = course

    return best
