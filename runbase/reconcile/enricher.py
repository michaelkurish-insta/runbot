"""Apply shoe, workout name, and category from a matched Strava source to an activity."""

import re


# Strava workout_type mapping (for runs):
#   0 = default/unspecified, 1 = race, 2 = long run, 3 = workout
STRAVA_WORKOUT_TYPE_MAP = {
    1: "race",
    2: "long",
    3: "workout",  # refined below from name patterns
}

# Name-based category patterns (checked against Strava activity name)
CATEGORY_PATTERNS = [
    (re.compile(r"\brace\b", re.IGNORECASE), "race"),
    (re.compile(r"\btempo\b", re.IGNORECASE), "tempo"),
    (re.compile(r"\binterval", re.IGNORECASE), "intervals"),
    (re.compile(r"\bfartlek\b", re.IGNORECASE), "fartlek"),
    (re.compile(r"\bhill", re.IGNORECASE), "hills"),
    (re.compile(r"\blong\s+run\b", re.IGNORECASE), "long"),
    (re.compile(r"\beasy\b", re.IGNORECASE), "easy"),
    (re.compile(r"\brecovery\b", re.IGNORECASE), "easy"),
    (re.compile(r"\bstride", re.IGNORECASE), "strides"),
    (re.compile(r"\bspeed", re.IGNORECASE), "intervals"),
    (re.compile(r"\btrack\b", re.IGNORECASE), "intervals"),
    (re.compile(r"\bwarmup\b|\bwarm.up\b", re.IGNORECASE), "easy"),
    (re.compile(r"\bcooldown\b|\bcool.down\b", re.IGNORECASE), "easy"),
]


def _infer_category(strava_source: dict) -> str | None:
    """Infer workout_category from Strava workout_type int and name patterns."""
    meta = strava_source.get("metadata", {})
    workout_type = meta.get("workout_type")
    name = strava_source.get("strava_name") or strava_source.get("workout_name") or ""

    # First try the Strava workout_type int
    if workout_type is not None:
        category = STRAVA_WORKOUT_TYPE_MAP.get(workout_type)
        if category and category != "workout":
            return category
        # workout_type=3 ("workout") — refine from name
        if category == "workout":
            for pattern, cat in CATEGORY_PATTERNS:
                if pattern.search(name):
                    return cat
            return "workout"

    # Fall back to name pattern matching
    for pattern, cat in CATEGORY_PATTERNS:
        if pattern.search(name):
            return cat

    return None


def enrich_from_strava(conn, activity_id: int, strava_source: dict,
                       verbose: bool = False) -> dict:
    """Enrich an activity with data from a matched orphaned Strava source.

    Actions:
      1. Link the orphan source to the activity
      2. Set shoe_id if missing
      3. Set workout_name if missing or generic
      4. Set workout_category if missing

    Returns dict with keys describing what was updated.
    """
    result = {"linked": False, "shoe_set": False, "name_set": False, "category_set": False}
    source_id = strava_source["id"]

    # 1. Link the orphan to this activity
    conn.execute(
        "UPDATE activity_sources SET activity_id = ? WHERE id = ?",
        (activity_id, source_id),
    )
    result["linked"] = True

    # Load current activity state
    row = conn.execute(
        "SELECT shoe_id, workout_name, workout_category FROM activities WHERE id = ?",
        (activity_id,),
    ).fetchone()
    if not row:
        return result

    current_shoe_id, current_name, current_category = row

    # 2. Shoe: look up by Strava gear_id
    gear_id = strava_source.get("gear_id")
    if gear_id and current_shoe_id is None:
        shoe_row = conn.execute(
            "SELECT id FROM shoes WHERE strava_gear_id = ?", (gear_id,)
        ).fetchone()
        if shoe_row:
            conn.execute(
                "UPDATE activities SET shoe_id = ?, updated_at = datetime('now') WHERE id = ?",
                (shoe_row[0], activity_id),
            )
            result["shoe_set"] = True
            if verbose:
                print(f"    SHOE → shoe #{shoe_row[0]}")

    # 3. Workout name: replace if NULL or generic
    strava_name = strava_source.get("strava_name") or strava_source.get("workout_name")
    generic_names = {None, "", "Outdoor Running", "Running"}
    if current_name in generic_names and strava_name and strava_name not in generic_names:
        conn.execute(
            "UPDATE activities SET workout_name = ?, updated_at = datetime('now') WHERE id = ?",
            (strava_name, activity_id),
        )
        result["name_set"] = True
        if verbose:
            print(f"    NAME → \"{strava_name}\"")

    # 4. Workout category
    if current_category is None:
        category = _infer_category(strava_source)
        if category:
            conn.execute(
                "UPDATE activities SET workout_category = ?, updated_at = datetime('now') WHERE id = ?",
                (category, activity_id),
            )
            result["category_set"] = True
            if verbose:
                print(f"    CATEGORY → {category}")

    return result


def enrich_group_from_strava(conn, activity_id: int, group: list[dict],
                              verbose: bool = False) -> dict:
    """Enrich an activity from a group of orphaned Strava sources.

    Picks the "primary" orphan (highest workout_type, or longest distance)
    for enrichment (name/category/shoe). Links all orphans to the activity.

    Returns dict with keys: linked_count, shoe_set, name_set, category_set.
    """
    # Rank workout_type: race=1 > long=2 > workout=3 > default/None=99
    WORKOUT_TYPE_PRIORITY = {1: 0, 2: 1, 3: 2}

    def primary_sort_key(orphan):
        meta = orphan.get("metadata", {})
        wt = meta.get("workout_type")
        priority = WORKOUT_TYPE_PRIORITY.get(wt, 99)
        # Negative distance so larger distance sorts first within same priority
        dist = -(orphan.get("distance_mi") or 0)
        return (priority, dist)

    sorted_group = sorted(group, key=primary_sort_key)
    primary = sorted_group[0]

    # Enrich from primary (sets name, category, shoe, and links it)
    result = enrich_from_strava(conn, activity_id, primary, verbose=verbose)

    # Link remaining orphans (just set activity_id, don't re-enrich)
    for orphan in sorted_group[1:]:
        conn.execute(
            "UPDATE activity_sources SET activity_id = ? WHERE id = ?",
            (activity_id, orphan["id"]),
        )

    result["linked_count"] = len(group)
    return result
