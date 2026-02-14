"""VDOT calculator using the Daniels-Gilbert formula.

Provides race-to-VDOT conversion, training pace derivation, and pace zone
classification for Jack Daniels training zones (E/M/T/I/R/FR).
"""

import math

METERS_PER_MILE = 1609.344

# Zone %VO2max targets — calibrated against Daniels tables (VDOT 40/50/60)
ZONE_PCT_VO2MAX = {
    "E": 0.70,
    "M": 0.82,
    "T": 0.88,
    "I": 0.98,      # ~97.5-98.1% across VDOTs, not 100%
    "R": 1.075,     # ~107-108% across VDOTs (faster than VO2max velocity)
    "FR": 1.15,     # fast reps: roughly midway between R and sprint
}

# Zone boundary %VO2max (midpoints between adjacent zones)
BOUNDARY_PCT_VO2MAX = {
    "E_M": 0.76,   # below = E, above = M
    "M_T": 0.85,   # below = M, above = T
    "T_I": 0.93,   # below = T, above = I
}


def race_to_vdot(distance_m: float, time_s: float) -> float:
    """Calculate VDOT from race performance using Daniels-Gilbert formula.

    Args:
        distance_m: Race distance in meters.
        time_s: Race time in seconds.

    Returns:
        VDOT value.
    """
    time_min = time_s / 60.0
    velocity = distance_m / time_min  # m/min

    vo2 = -4.60 + 0.182258 * velocity + 0.000104 * velocity ** 2
    pct_vo2max = (0.8 + 0.1894393 * math.exp(-0.012778 * time_min)
                  + 0.2989558 * math.exp(-0.1932605 * time_min))

    return round(vo2 / pct_vo2max, 2)


def _velocity_from_vo2(vo2: float) -> float:
    """Solve the VO2-velocity quadratic for velocity (m/min).

    VO2 = -4.60 + 0.182258*V + 0.000104*V²
    Rearranged: 0.000104*V² + 0.182258*V + (-4.60 - VO2) = 0
    """
    a = 0.000104
    b = 0.182258
    c = -(vo2 + 4.60)
    discriminant = b ** 2 - 4 * a * c
    return (-b + math.sqrt(discriminant)) / (2 * a)


def _velocity_to_pace(velocity_m_per_min: float) -> float:
    """Convert velocity (m/min) to pace (s/mi)."""
    return METERS_PER_MILE / velocity_m_per_min * 60


def vdot_to_paces(vdot: float) -> dict[str, float]:
    """Derive training paces for each zone from VDOT.

    Returns:
        Dict mapping zone name to pace in seconds per mile.
    """
    paces = {}
    for zone, pct in ZONE_PCT_VO2MAX.items():
        target_vo2 = vdot * pct
        velocity = _velocity_from_vo2(target_vo2)
        paces[zone] = round(_velocity_to_pace(velocity), 1)

    return paces


def vdot_to_boundaries(vdot: float, walking_threshold: float = 660.0) -> dict[str, float]:
    """Derive pace zone boundaries from VDOT.

    Boundaries are placed at midpoints between adjacent zone centers,
    except for walk which uses the walking_threshold.

    Returns:
        Dict mapping zone name to the slow (high pace value) boundary.
        A pace belongs to a zone if it is <= that zone's boundary and
        > the next faster zone's boundary.
    """
    paces = vdot_to_paces(vdot)

    # Boundary paces derived from %VO2max midpoints
    boundaries_pct = {
        "E_M": BOUNDARY_PCT_VO2MAX["E_M"],
        "M_T": BOUNDARY_PCT_VO2MAX["M_T"],
        "T_I": BOUNDARY_PCT_VO2MAX["T_I"],
    }

    boundary_paces = {}
    for key, pct in boundaries_pct.items():
        target_vo2 = vdot * pct
        velocity = _velocity_from_vo2(target_vo2)
        boundary_paces[key] = _velocity_to_pace(velocity)

    # I/R boundary: midpoint of I and R paces
    ir_boundary = (paces["I"] + paces["R"]) / 2
    # R/FR boundary: midpoint of R and FR paces
    rfr_boundary = (paces["R"] + paces["FR"]) / 2

    return {
        "walk": walking_threshold,
        "E": boundary_paces["E_M"],       # slower than this = E (or walk)
        "M": boundary_paces["M_T"],       # slower than this but faster than E boundary = M
        "T": boundary_paces["T_I"],       # slower = T, faster = I
        "I": ir_boundary,                 # slower = I, faster = R
        "R": rfr_boundary,               # slower = R, faster = FR
    }


def classify_pace(pace_s_per_mi: float, boundaries: dict) -> str:
    """Classify a pace into a training zone.

    Args:
        pace_s_per_mi: Pace in seconds per mile.
        boundaries: Zone boundaries from vdot_to_boundaries().

    Returns:
        Zone string: 'walk', 'E', 'M', 'T', 'I', 'R', or 'FR'.
    """
    if pace_s_per_mi >= boundaries["walk"]:
        return "walk"
    if pace_s_per_mi >= boundaries["E"]:
        return "E"
    if pace_s_per_mi >= boundaries["M"]:
        return "M"
    if pace_s_per_mi >= boundaries["T"]:
        return "T"
    if pace_s_per_mi >= boundaries["I"]:
        return "I"
    if pace_s_per_mi >= boundaries["R"]:
        return "R"
    return "FR"


def format_pace(seconds_per_mile: float) -> str:
    """Format pace as M:SS per mile (e.g. '5:16')."""
    minutes = int(seconds_per_mile // 60)
    secs = int(seconds_per_mile % 60)
    return f"{minutes}:{secs:02d}"


def get_current_vdot(conn, date: str) -> float | None:
    """Get the most recent VDOT entry on or before the given date."""
    row = conn.execute(
        "SELECT vdot FROM vdot_history WHERE effective_date <= ? ORDER BY effective_date DESC LIMIT 1",
        (date,),
    ).fetchone()
    return row[0] if row else None


def set_vdot(conn, vdot: float, effective_date: str, source: str = "manual",
             activity_id: int | None = None, notes: str | None = None):
    """Insert a new VDOT history entry."""
    conn.execute(
        """INSERT INTO vdot_history (effective_date, vdot, source, activity_id, notes)
           VALUES (?, ?, ?, ?, ?)""",
        (effective_date, vdot, source, activity_id, notes),
    )
    conn.commit()
