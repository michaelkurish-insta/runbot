"""Grade adjustment on continuous recordings with distance-based smoothing."""

from collections import defaultdict
from math import isfinite

import numpy as np

METERS_PER_MILE = 1609.344
FT_PER_M = 3.28084
GRADE_WINDOW_M = 100.0
SAMPLE_DISTANCE_M = 10.0


def _minetti_cost_ratio(grade):
    """Existing Minetti cost model, retaining the downhill floor of 0.785."""
    grade = np.clip(grade, -0.5, 0.5)
    cost = (155.4*grade**5 - 30.4*grade**4 - 43.3*grade**3
            + 46.3*grade**2 + 19.5*grade + 3.6)
    return np.maximum(0.785, cost / 3.6)


def continuous_recordings(streams):
    """Do not join sources, distance resets, pauses, or implausible GPS jumps.

    Input is in timestamp order. Missing samples break the grade window too,
    rather than allowing elevation on opposite sides of a gap to form a hill.
    """
    sources = defaultdict(list)
    for point in streams:
        sources[point.get('source_id')].append(point)
    for points in sources.values():
        chunk = []
        for point in points:
            values = [point.get(k) for k in ('timestamp_s', 'distance_mi', 'altitude_ft')]
            if any(v is None or not isfinite(v) for v in values):
                if len(chunk) >= 2:
                    yield chunk
                chunk = []
                continue
            t, distance, elevation = values
            sample = (t, distance * METERS_PER_MILE, elevation / FT_PER_M)
            if chunk:
                dt, dx = t - chunk[-1][0], sample[1] - chunk[-1][1]
                if dt <= 0 or dt > 30 or dx <= 0 or dx / dt > 15:
                    if len(chunk) >= 2:
                        yield chunk
                    chunk = []
            chunk.append(sample)
        if len(chunk) >= 2:
            yield chunk


def compute_gap(streams, base_pace_s_per_mi=None):
    """Apply a terrain factor to actual pace, without changing its time basis.

    Estimate slope over centered 100 m windows on a 10 m distance grid. Short
    sample-to-sample altitude noise otherwise creates large fictitious climbs
    whose cost isn't cancelled by the capped downhill benefit. Windows never
    cross a recording boundary. The distance-weighted factor is independent of
    recording frequency and of the number of laps in the activity.
    """
    total_distance = adjusted_distance = moving_time = 0.0
    for chunk in continuous_recordings(streams):
        points = np.asarray(chunk)
        times, distances, elevations = points.T
        distance = distances[-1] - distances[0]
        if distance < 30:
            continue
        grid = np.append(np.arange(distances[0], distances[-1], SAMPLE_DISTANCE_M), distances[-1])
        altitude = np.interp(grid, distances, elevations)
        midpoints = (grid[1:] + grid[:-1]) / 2
        left = np.maximum(grid[0], midpoints - GRADE_WINDOW_M / 2)
        right = np.minimum(grid[-1], midpoints + GRADE_WINDOW_M / 2)
        grade = (np.interp(right, grid, altitude) - np.interp(left, grid, altitude)) / (right - left)
        adjusted_distance += float(np.sum(np.diff(grid) * _minetti_cost_ratio(grade)))
        total_distance += distance
        moving_time += times[-1] - times[0]
    if total_distance <= 0 or adjusted_distance <= 0:
        return None
    pace = base_pace_s_per_mi
    if pace is None:
        pace = moving_time * METERS_PER_MILE / total_distance
    if not isfinite(pace) or pace <= 0:
        return None
    return float(pace * total_distance / adjusted_distance)
