from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Activity:
    id: Optional[int] = None
    date: str = ""
    start_time: Optional[str] = None
    distance_m: Optional[float] = None
    duration_s: Optional[float] = None
    avg_pace_s_per_km: Optional[float] = None
    avg_hr: Optional[float] = None
    max_hr: Optional[float] = None
    avg_cadence: Optional[float] = None
    total_ascent_m: Optional[float] = None
    total_descent_m: Optional[float] = None
    calories: Optional[float] = None
    workout_type: Optional[str] = None
    workout_name: Optional[str] = None
    intensity_score: Optional[float] = None
    notes: Optional[str] = None
    shoe_id: Optional[int] = None
    rpe: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass
class ActivitySource:
    id: Optional[int] = None
    activity_id: Optional[int] = None
    source: str = ""
    source_id: Optional[str] = None
    raw_file_path: Optional[str] = None
    distance_m: Optional[float] = None
    duration_s: Optional[float] = None
    avg_pace_s_per_km: Optional[float] = None
    avg_hr: Optional[float] = None
    max_hr: Optional[float] = None
    avg_cadence: Optional[float] = None
    total_ascent_m: Optional[float] = None
    calories: Optional[float] = None
    workout_name: Optional[str] = None
    notes: Optional[str] = None
    metadata_json: Optional[str] = None
    imported_at: Optional[str] = None


@dataclass
class Interval:
    id: Optional[int] = None
    activity_id: Optional[int] = None
    rep_number: Optional[int] = None
    prescribed_distance_m: Optional[float] = None
    actual_distance_m: Optional[float] = None
    canonical_distance_m: Optional[float] = None
    duration_s: Optional[float] = None
    avg_pace_s_per_km: Optional[float] = None
    avg_hr: Optional[float] = None
    avg_cadence: Optional[float] = None
    is_recovery: bool = False


@dataclass
class Stream:
    id: Optional[int] = None
    activity_id: Optional[int] = None
    timestamp_s: Optional[float] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    altitude_m: Optional[float] = None
    heart_rate: Optional[int] = None
    cadence: Optional[int] = None
    pace_s_per_km: Optional[float] = None
    distance_m: Optional[float] = None


@dataclass
class Shoe:
    id: Optional[int] = None
    name: str = ""
    brand: Optional[str] = None
    model: Optional[str] = None
    strava_gear_id: Optional[str] = None
    purchase_date: Optional[str] = None
    retired: bool = False
    notes: Optional[str] = None


@dataclass
class Conflict:
    id: Optional[int] = None
    activity_id: Optional[int] = None
    field_name: str = ""
    source_values_json: str = ""
    resolved_value: Optional[str] = None
    resolved_source: Optional[str] = None
    status: str = "pending"
    created_at: Optional[str] = None
    resolved_at: Optional[str] = None


@dataclass
class RunalyzeMetrics:
    id: Optional[int] = None
    activity_id: Optional[int] = None
    trimp: Optional[float] = None
    vdot: Optional[float] = None
    effective_vo2max: Optional[float] = None
    training_effect: Optional[float] = None
    hr_reserve_pct: Optional[float] = None
    fitness: Optional[float] = None
    fatigue: Optional[float] = None
    form: Optional[float] = None
    raw_csv_json: Optional[str] = None


@dataclass
class SyncState:
    id: Optional[int] = None
    source: str = ""
    last_sync_at: Optional[str] = None
    last_activity_date: Optional[str] = None
    metadata_json: Optional[str] = None


@dataclass
class ProcessedFile:
    id: Optional[int] = None
    file_path: str = ""
    file_hash: Optional[str] = None
    source: Optional[str] = None
    processed_at: Optional[str] = None
    activity_id: Optional[int] = None
