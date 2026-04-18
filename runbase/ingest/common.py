"""Shared sync infrastructure used by Strava and Garmin importers."""

from datetime import timedelta

from runbase.ingest.fit_parser import format_pace

METERS_PER_MILE = 1609.344
METERS_TO_FEET = 3.28084

# Fields that secondary sources can fill (only when NULL on canonical activity)
FILLABLE_FIELDS = [
    "start_time", "max_hr", "total_ascent_ft", "total_descent_ft",
    "calories", "duration_s", "avg_hr", "avg_cadence",
]

# Generic FIT/Strava default names that should be replaced by real names
_GENERIC_NAME_PATTERNS = [
    "Outdoor Running", "Indoor Running", "Treadmill Running",
    "Morning Run", "Afternoon Run", "Evening Run", "Lunch Run",
    "Night Run",
]


def build_activity_lookup(conn) -> dict:
    """Load all activities into a dict keyed by date -> list of activity rows.

    Each row is a dict with id, distance_mi, duration_s, start_time, and
    fields needed for NULL-check merging.
    """
    rows = conn.execute(
        """SELECT id, date, distance_mi, duration_s, start_time,
                  avg_hr, max_hr, avg_cadence, total_ascent_ft, total_descent_ft,
                  calories, shoe_id
           FROM activities"""
    ).fetchall()

    lookup = {}
    for r in rows:
        date = r[1]
        entry = {
            "id": r[0],
            "date": r[1],
            "distance_mi": r[2],
            "duration_s": r[3],
            "start_time": r[4],
            "avg_hr": r[5],
            "max_hr": r[6],
            "avg_cadence": r[7],
            "total_ascent_ft": r[8],
            "total_descent_ft": r[9],
            "calories": r[10],
            "shoe_id": r[11],
        }
        lookup.setdefault(date, []).append(entry)

    return lookup


def is_generic_name(name: str | None) -> bool:
    """Check if a workout name is a generic default that should be overridden."""
    if not name:
        return True
    name_lower = name.lower().strip()
    for pattern in _GENERIC_NAME_PATTERNS:
        if name_lower == pattern.lower():
            return True
        # "Monday Morning Run", "Tuesday Afternoon Run", etc.
        if name_lower.endswith(pattern.lower()):
            return True
    return False


def match_activity(date_str: str, distance_mi: float, lookup: dict,
                   tolerance_pct: float) -> dict | None:
    """Match an incoming activity to a DB activity by date + distance.

    Args:
        date_str: Activity date as YYYY-MM-DD string.
        distance_mi: Activity distance in miles.
        lookup: Dict from build_activity_lookup().
        tolerance_pct: Max distance difference percentage for matching.

    Returns the matched DB activity dict or None.
    """
    from datetime import datetime

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    candidate_dates = [
        date_str,
        (dt - timedelta(days=1)).strftime("%Y-%m-%d"),
        (dt + timedelta(days=1)).strftime("%Y-%m-%d"),
    ]

    candidates = []
    for d in candidate_dates:
        candidates.extend(lookup.get(d, []))

    if not candidates:
        return None

    best_match = None
    best_diff_pct = float("inf")

    for cand in candidates:
        db_dist = cand["distance_mi"]
        if db_dist is None or db_dist <= 0:
            if cand["date"] == date_str and best_match is None:
                best_match = cand
                best_diff_pct = 100
            continue

        diff_pct = abs(distance_mi - db_dist) / db_dist * 100
        if diff_pct <= tolerance_pct and diff_pct < best_diff_pct:
            best_match = cand
            best_diff_pct = diff_pct

    return best_match


def merge_fields(conn, activity_id: int, incoming_data: dict, verbose: bool) -> list:
    """Fill NULL fields on canonical activity with incoming data.

    Args:
        conn: DB connection.
        activity_id: Canonical activity ID.
        incoming_data: Dict with keys matching FILLABLE_FIELDS + "name".
        verbose: Print progress.

    Returns list of filled field names.
    """
    field_map = {
        "start_time": "start_time",
        "max_hr": "max_hr",
        "total_ascent_ft": "total_ascent_ft",
        "calories": "calories",
        "duration_s": "duration_s",
        "avg_hr": "avg_hr",
        "avg_cadence": "avg_cadence",
    }

    filled = []
    for src_key, db_col in field_map.items():
        src_val = incoming_data.get(src_key)
        if src_val is None:
            continue

        row = conn.execute(
            f"SELECT {db_col} FROM activities WHERE id = ?", (activity_id,)
        ).fetchone()
        if row and row[0] is None:
            conn.execute(
                f"UPDATE activities SET {db_col} = ?, updated_at = datetime('now') WHERE id = ?",
                (src_val, activity_id),
            )
            filled.append(db_col)
            if verbose:
                print(f"    FILL {db_col} = {src_val}")

    # Replace generic workout names with real names
    incoming_name = incoming_data.get("name")
    if incoming_name and not is_generic_name(incoming_name):
        row = conn.execute(
            "SELECT workout_name FROM activities WHERE id = ?", (activity_id,)
        ).fetchone()
        current_name = row[0] if row else None
        if is_generic_name(current_name):
            conn.execute(
                "UPDATE activities SET workout_name = ?, updated_at = datetime('now') WHERE id = ?",
                (incoming_name, activity_id),
            )
            filled.append("workout_name")
            if verbose:
                print(f"    NAME '{current_name}' → '{incoming_name}'")

    return filled


def activity_has_intervals(conn, activity_id: int) -> bool:
    """Check if an activity already has intervals."""
    row = conn.execute(
        "SELECT COUNT(*) FROM intervals WHERE activity_id = ?", (activity_id,)
    ).fetchone()
    return row[0] > 0


def activity_has_streams(conn, activity_id: int) -> bool:
    """Check if an activity already has stream data."""
    row = conn.execute(
        "SELECT COUNT(*) FROM streams WHERE activity_id = ?", (activity_id,)
    ).fetchone()
    return row[0] > 0
