"""Parse .fit files into RunBase data structures using fitparse."""

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path

from fitparse import FitFile

from runbase.models import Activity, ActivitySource, Interval, Stream

SEMICIRCLE_TO_DEGREES = 180.0 / (2 ** 31)
METERS_PER_MILE = 1609.344
FEET_PER_METER = 3.28084


def format_pace(seconds_per_mile: float) -> str:
    """Format pace as M:SS.s per mile (e.g. '5:16.5')."""
    minutes = int(seconds_per_mile // 60)
    secs = seconds_per_mile - minutes * 60
    return f"{minutes}:{secs:04.1f}"


@dataclass
class ParsedFitFile:
    activity: Activity
    source: ActivitySource
    streams: list[Stream]
    laps: list[Interval]
    device_info: dict
    file_path: str
    file_hash: str


def parse_fit_file(file_path: str) -> ParsedFitFile:
    """Parse a .fit file and return all extracted data."""
    file_path = str(file_path)
    fit = FitFile(file_path)
    messages = list(fit.get_messages())

    file_hash = _compute_file_hash(file_path)
    workout_name = _extract_workout_name(file_path)
    device_info = _extract_device_info(messages)

    activity = _extract_session(messages)
    if workout_name and not activity.workout_name:
        activity.workout_name = workout_name

    streams = _extract_records(messages, activity.start_time)
    laps = _extract_laps(messages, streams)

    # Recompute avg HR and cadence from per-second stream data
    _apply_stream_averages(activity, streams)

    source = ActivitySource(
        source="healthfit",
        raw_file_path=file_path,
        distance_mi=activity.distance_mi,
        duration_s=activity.duration_s,
        avg_pace_s_per_mi=activity.avg_pace_s_per_mi,
        avg_hr=activity.avg_hr,
        max_hr=activity.max_hr,
        avg_cadence=activity.avg_cadence,
        total_ascent_ft=activity.total_ascent_ft,
        calories=activity.calories,
        workout_name=activity.workout_name,
    )

    return ParsedFitFile(
        activity=activity,
        source=source,
        streams=streams,
        laps=laps,
        device_info=device_info,
        file_path=file_path,
        file_hash=file_hash,
    )


def _extract_session(messages) -> Activity:
    """Extract activity-level data from the first FIT session message."""
    session = None
    for msg in messages:
        if msg.name == "session":
            session = msg
            break

    if session is None:
        raise ValueError("No session message found in .fit file")

    def get(field_name, default=None):
        val = session.get_value(field_name)
        return val if val is not None else default

    start_time = get("start_time")
    start_time_iso = start_time.isoformat() if start_time else None
    date_str = start_time.strftime("%Y-%m-%d") if start_time else ""

    distance_m = get("total_distance")
    duration_s = get("total_timer_time")

    distance_mi = round(distance_m / METERS_PER_MILE, 2) if distance_m else None

    avg_pace = None
    avg_pace_display = None
    if distance_mi and distance_mi > 0 and duration_s:
        avg_pace = round(duration_s / distance_mi, 1)
        avg_pace_display = format_pace(avg_pace)

    avg_cadence = get("avg_cadence")
    if avg_cadence is not None:
        sport = get("sport")
        if sport and str(sport).lower() == "running":
            avg_cadence = avg_cadence * 2
        avg_cadence = round(float(avg_cadence), 2)

    avg_hr = get("avg_heart_rate")
    if avg_hr is not None:
        avg_hr = round(float(avg_hr), 2)
    max_hr = get("max_heart_rate")
    if max_hr is not None:
        max_hr = round(float(max_hr), 2)

    total_ascent_m = get("total_ascent")
    total_descent_m = get("total_descent")
    total_ascent_ft = round(total_ascent_m * FEET_PER_METER, 1) if total_ascent_m is not None else None
    total_descent_ft = round(total_descent_m * FEET_PER_METER, 1) if total_descent_m is not None else None

    return Activity(
        date=date_str,
        start_time=start_time_iso,
        distance_mi=distance_mi,
        duration_s=duration_s,
        avg_pace_s_per_mi=avg_pace,
        avg_pace_display=avg_pace_display,
        avg_hr=avg_hr,
        max_hr=max_hr,
        avg_cadence=avg_cadence,
        total_ascent_ft=total_ascent_ft,
        total_descent_ft=total_descent_ft,
        calories=get("total_calories"),
        workout_type=str(get("sport", "running")).lower(),
    )


def _avg_from_streams(streams: list[Stream], field: str) -> float | None:
    """Compute average of a stream field, ignoring None values."""
    values = [getattr(s, field) for s in streams if getattr(s, field) is not None]
    if not values:
        return None
    return sum(values) / len(values)


def _apply_stream_averages(activity: Activity, streams: list[Stream]) -> None:
    """Recompute avg HR, cadence, and pace from per-second stream data."""
    avg_hr = _avg_from_streams(streams, "heart_rate")
    if avg_hr is not None:
        activity.avg_hr = round(avg_hr, 2)

    avg_cadence = _avg_from_streams(streams, "cadence")
    if avg_cadence is not None:
        activity.avg_cadence = round(avg_cadence, 2)

    # Recompute pace from stream speed data for better precision
    pace_values = [s.pace_s_per_mi for s in streams if s.pace_s_per_mi is not None]
    if pace_values and activity.distance_mi and activity.distance_mi > 0 and activity.duration_s:
        activity.avg_pace_s_per_mi = round(activity.duration_s / activity.distance_mi, 1)
        activity.avg_pace_display = format_pace(activity.avg_pace_s_per_mi)


def _extract_records(messages, start_time_iso: str | None) -> list[Stream]:
    """Extract per-second stream data from FIT record messages."""
    streams = []
    for msg in messages:
        if msg.name != "record":
            continue

        def get(field_name, default=None):
            val = msg.get_value(field_name)
            return val if val is not None else default

        timestamp = get("timestamp")
        timestamp_s = timestamp.timestamp() if timestamp else None

        lat_semi = get("position_lat")
        lon_semi = get("position_long")
        lat = lat_semi * SEMICIRCLE_TO_DEGREES if lat_semi is not None else None
        lon = lon_semi * SEMICIRCLE_TO_DEGREES if lon_semi is not None else None

        altitude = get("enhanced_altitude")
        if altitude is None:
            altitude = get("altitude")
        altitude_ft = round(altitude * FEET_PER_METER, 1) if altitude is not None else None

        speed = get("enhanced_speed")
        if speed is None:
            speed = get("speed")

        pace = None
        if speed is not None and speed > 0:
            pace = round(METERS_PER_MILE / speed, 1)

        cadence = get("cadence")
        if cadence is not None:
            cadence = cadence * 2

        distance_m = get("distance")
        distance_mi = round(distance_m / METERS_PER_MILE, 4) if distance_m is not None else None

        streams.append(Stream(
            timestamp_s=timestamp_s,
            lat=lat,
            lon=lon,
            altitude_ft=altitude_ft,
            heart_rate=get("heart_rate"),
            cadence=cadence,
            pace_s_per_mi=pace,
            distance_mi=distance_mi,
        ))

    return streams


def _extract_laps(messages, streams: list[Stream]) -> list[Interval]:
    """Extract lap/interval data from FIT lap messages.

    Computes avg HR and cadence from per-second stream data using each
    lap's start_time/timestamp to slice the relevant stream records.
    """
    laps = []
    for i, msg in enumerate(messages):
        if msg.name != "lap":
            continue

        def get(field_name, default=None):
            val = msg.get_value(field_name)
            return val if val is not None else default

        distance_m = get("total_distance")
        duration = get("total_timer_time")

        distance_mi = round(distance_m / METERS_PER_MILE, 2) if distance_m else None

        avg_pace = None
        avg_pace_display = None
        if distance_mi and distance_mi > 0 and duration:
            avg_pace = round(duration / distance_mi, 1)
            avg_pace_display = format_pace(avg_pace)

        # Compute avg HR and cadence from stream data within this lap's time range
        lap_start = get("start_time")
        lap_end = get("timestamp")
        lap_start_ts = lap_start.timestamp() if lap_start else None
        lap_end_ts = lap_end.timestamp() if lap_end else None

        avg_hr = None
        avg_cadence = None
        if lap_start_ts is not None and lap_end_ts is not None:
            lap_streams = [
                s for s in streams
                if s.timestamp_s is not None and lap_start_ts <= s.timestamp_s <= lap_end_ts
            ]
            avg_hr = _avg_from_streams(lap_streams, "heart_rate")
            avg_cadence = _avg_from_streams(lap_streams, "cadence")

        if avg_hr is not None:
            avg_hr = round(avg_hr, 2)
        if avg_cadence is not None:
            avg_cadence = round(avg_cadence, 2)

        laps.append(Interval(
            rep_number=len(laps) + 1,
            gps_measured_distance_mi=distance_mi,
            duration_s=duration,
            avg_pace_s_per_mi=avg_pace,
            avg_pace_display=avg_pace_display,
            avg_hr=avg_hr,
            avg_cadence=avg_cadence,
            start_timestamp_s=lap_start_ts,
            end_timestamp_s=lap_end_ts,
            source="fit_lap",
        ))

    return laps


def _extract_device_info(messages) -> dict:
    """Extract device metadata from FIT device_info messages."""
    info = {}
    for msg in messages:
        if msg.name == "device_info":
            manufacturer = msg.get_value("manufacturer")
            product = msg.get_value("garmin_product") or msg.get_value("product_name")
            serial = msg.get_value("serial_number")
            sw_version = msg.get_value("software_version")

            if manufacturer and "manufacturer" not in info:
                info["manufacturer"] = str(manufacturer)
            if product and "product" not in info:
                info["product"] = str(product)
            if serial and "serial_number" not in info:
                info["serial_number"] = str(serial)
            if sw_version and "software_version" not in info:
                info["software_version"] = str(sw_version)

        elif msg.name == "file_id":
            time_created = msg.get_value("time_created")
            if time_created and "time_created" not in info:
                info["time_created"] = str(time_created)

    return info


def _extract_workout_name(file_path: str) -> str | None:
    """Try to parse workout name from HealthFit filename convention.

    Expected: YYYY-MM-DD-HHMMSS-[Activity Type]-[App].fit
    Example: 2024-05-11-061200-Running-HealthFit.fit
    """
    stem = Path(file_path).stem
    match = re.match(r"\d{4}-\d{2}-\d{2}-\d{6}-(.+?)(?:-[^-]+)?$", stem)
    if match:
        return match.group(1).replace("-", " ")
    return None


def _compute_file_hash(file_path: str) -> str:
    """Compute SHA-256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()
