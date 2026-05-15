import math
import re
import sqlite3
import subprocess
import threading
from calendar import monthrange
from datetime import date, datetime, timedelta
from pathlib import Path

from flask import Flask, jsonify, render_template, request

from runbase.config import load_config
from runbase.db import get_connection, _migrate_schema


def create_app(config=None):
    app = Flask(
        __name__,
        template_folder=str(Path(__file__).parent / "templates"),
        static_folder=str(Path(__file__).parent / "static"),
    )

    if config is None:
        config = load_config()
    app.config["RUNBASE"] = config

    def get_db():
        conn = get_connection(config)
        conn.row_factory = sqlite3.Row
        _migrate_schema(conn)
        return conn

    # ── Helpers ──────────────────────────────────────────────────────

    OVERRIDABLE_FIELDS = {
        "distance_mi", "duration_s", "avg_pace_s_per_mi", "workout_name",
        "workout_category", "shoe_id", "notes", "strides", "workout_type_zone",
        "avg_hr", "max_hr", "avg_cadence",
    }

    MONTH_NAMES = [
        "", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ]

    # ── AVD (Adjusted VDOT) helpers ─────────────────────────────────

    AVD_WINDOW_DAYS = 21
    AVD_HALF_LIFE = 7
    _AVD_LN2 = math.log(2)

    def _avd_ewa(target_date_str, cvd_list, inclusive=False):
        """Duration-weighted exponential moving average of CVDs.

        Args:
            target_date_str: date to compute EWA for.
            cvd_list: sorted list of (date_str, computed_vdot, duration_s).
            inclusive: if True, include target_date (for AVD);
                       if False, exclude it (for race prediction).
        Returns:
            (ewa_value, count) or (None, 0).
        """
        td = date.fromisoformat(target_date_str)
        ws = td - timedelta(days=AVD_WINDOW_DAYS)
        tw = 0.0
        twv = 0.0
        count = 0
        for ds, cvd, dur in cvd_list:
            d = date.fromisoformat(ds)
            if d < ws:
                continue
            if d > td:
                break
            if not inclusive and d >= td:
                break
            days_before = (td - d).days
            w = math.exp(-_AVD_LN2 * days_before / AVD_HALF_LIFE) * max(dur, 60)
            tw += w
            twv += w * cvd
            count += 1
        if tw == 0 or count == 0:
            return None, 0
        return twv / tw, count

    def _avd_multiplier(conn):
        """Compute the least-squares calibrated multiplier from race history."""
        races = conn.execute(
            "SELECT vh.effective_date, vh.vdot FROM vdot_history vh "
            "WHERE vh.source = 'race' ORDER BY vh.effective_date",
        ).fetchall()
        all_cvd = conn.execute(
            "SELECT date, computed_vdot, duration_s FROM activities "
            "WHERE computed_vdot IS NOT NULL AND NOT COALESCE(exclude_from_vdot, 0) ORDER BY date",
        ).fetchall()
        cvd_list = [(r[0], r[1], r[2] or 0) for r in all_cvd]

        pairs = []
        for r in races:
            ewa, _ = _avd_ewa(r[0], cvd_list, inclusive=False)
            if ewa is not None:
                pairs.append((ewa, r[1]))
        if len(pairs) >= 2:
            num = sum(a * e for e, a in pairs)
            den = sum(e * e for e, _ in pairs)
            return num / den if den > 0 else 1.0
        return 1.0

    def _format_pace(seconds_per_mile):
        if seconds_per_mile is None:
            return ""
        m, s = divmod(int(seconds_per_mile), 60)
        return f"{m}:{s:02d}"

    def _format_pace_tenths(seconds_per_mile):
        """Format pace as M:SS.s with one decimal."""
        if seconds_per_mile is None:
            return ""
        m = int(seconds_per_mile // 60)
        s = seconds_per_mile - m * 60
        return f"{m}:{s:04.1f}"

    def _format_duration(seconds):
        if seconds is None:
            return ""
        total = int(seconds)
        if total >= 3600:
            h, rem = divmod(total, 3600)
            m, s = divmod(rem, 60)
            return f"{h}:{m:02d}:{s:02d}"
        m, s = divmod(total, 60)
        return f"{m}:{s:02d}"

    def _format_duration_precise(seconds):
        """Format duration with tenths of a second (for interval detail)."""
        if seconds is None:
            return ""
        # Round to tenths first, then decompose — avoids truncation bugs
        # where e.g. 76.968 would show as 1:16.0 instead of 1:17.0
        rounded = round(seconds, 1)
        total = int(rounded)
        tenths = round((rounded - total) * 10)
        if total >= 3600:
            h, rem = divmod(total, 3600)
            m, s = divmod(rem, 60)
            return f"{h}:{m:02d}:{s:02d}.{tenths}"
        m, s = divmod(total, 60)
        return f"{m}:{s:02d}.{tenths}"

    def _apply_overrides(activity_dict, overrides):
        overridden = set()
        for field_name, value in overrides.items():
            if field_name in activity_dict or field_name in OVERRIDABLE_FIELDS:
                overridden.add(field_name)
                if field_name in ("distance_mi", "duration_s", "avg_pace_s_per_mi",
                                  "avg_hr", "max_hr", "avg_cadence"):
                    activity_dict[field_name] = float(value)
                elif field_name in ("shoe_id", "strides"):
                    activity_dict[field_name] = int(value)
                else:
                    activity_dict[field_name] = value
        return overridden

    def _get_overrides_for_activities(conn, activity_ids):
        if not activity_ids:
            return {}
        placeholders = ",".join("?" * len(activity_ids))
        rows = conn.execute(
            f"SELECT activity_id, field_name, override_value "
            f"FROM activity_overrides WHERE activity_id IN ({placeholders})",
            activity_ids,
        ).fetchall()
        result = {}
        for r in rows:
            result.setdefault(r["activity_id"], {})[r["field_name"]] = r["override_value"]
        return result

    def _get_shoes(conn):
        rows = conn.execute("SELECT id, name FROM shoes").fetchall()
        return {r["id"]: r["name"] for r in rows}

    def _week_of_month(dt):
        """1-based week number within the month (Sun-Sat weeks)."""
        first = dt.replace(day=1)
        # Sunday = 0 for week start: shift weekday so Sun=0
        first_day_offset = (first.weekday() + 1) % 7  # Mon=0..Sun=6 → Sun=0..Sat=6
        return ((dt.day - 1 + first_day_offset) // 7) + 1

    ZONE_PRIORITY = {"FR": 6, "R": 5, "I": 4, "T": 3, "M": 2, "E": 1}

    def _get_workout_types(conn, activity_ids):
        """Get predominant pace zone per activity from qualifying intervals.

        Uses FIT-preferred logic: when an activity has both FIT (NULL source)
        and strava_lap intervals, only FIT intervals are considered.

        When an activity has work intervals at multiple distinct distances,
        returns a combo type (e.g. "I/R") with zones sorted slowest-first.
        """
        if not activity_ids:
            return {}
        placeholders = ",".join("?" * len(activity_ids))
        rows = conn.execute(
            f"""SELECT activity_id, pace_zone, source,
                       COALESCE(canonical_distance_mi, gps_measured_distance_mi,
                                prescribed_distance_mi) as dist
                FROM intervals
                WHERE activity_id IN ({placeholders})
                  AND pace_zone IS NOT NULL
                  AND NOT is_recovery
                  AND NOT is_walking
                  AND NOT is_stride
                  AND NOT is_hill_sprint
                  AND gps_measured_distance_mi > 0.015
                  AND (source IS NULL OR source NOT IN ('pace_segment'))""",
            activity_ids,
        ).fetchall()

        # Find which activities have ANY FIT (NULL-source) non-pace-segment intervals.
        # Check all intervals, not just qualifying ones — an activity with only
        # stride FIT laps still has FIT data, so its Strava laps should be hidden.
        fit_check = conn.execute(
            f"""SELECT DISTINCT activity_id FROM intervals
                WHERE activity_id IN ({placeholders})
                  AND source IS NULL
                  AND (source IS NULL OR source != 'pace_segment')""",
            activity_ids,
        ).fetchall()
        has_fit = {r["activity_id"] for r in fit_check}

        # Group qualifying intervals per activity (skip strava_lap when FIT exists)
        by_act = {}
        for r in rows:
            aid = r["activity_id"]
            if r["source"] == "strava_lap" and aid in has_fit:
                continue
            by_act.setdefault(aid, []).append(r)

        result = {}
        for aid, intervals in by_act.items():
            # Cluster by distance: round to nearest 50m bucket
            dist_clusters = {}
            for iv in intervals:
                dist = iv["dist"]
                if dist and dist > 0:
                    dist_m = dist * 1609.344
                    bucket = round(dist_m / 50) * 50
                else:
                    bucket = 0
                dist_clusters.setdefault(bucket, []).append(iv["pace_zone"])

            # Find highest-priority zone per cluster
            cluster_zones = {}
            for bucket, zones in dist_clusters.items():
                best = max(zones, key=lambda z: ZONE_PRIORITY.get(z, 0))
                cluster_zones[bucket] = best

            unique_zones = set(cluster_zones.values())
            # E laps in a mixed workout are warmup/cooldown, not a distinct
            # work type.  Drop E (and M) from combos — "E/I" is just "I".
            # Keep E or M only when it's the sole zone present.
            if len(unique_zones) > 1:
                unique_zones -= {"E", "M"}
            if len(unique_zones) <= 1:
                # Single zone — use it directly
                result[aid] = next(iter(unique_zones)) if unique_zones else ""
            else:
                # Multiple zones — combo sorted slowest first (ascending priority)
                sorted_zones = sorted(unique_zones, key=lambda z: ZONE_PRIORITY.get(z, 0))
                result[aid] = "/".join(sorted_zones)

        return result

    def _zone_for_distance_m(dist_m, combo_zone):
        """Pick the appropriate zone from a combo type based on rep distance.

        Heuristic: ≤600m → R (or FR if combo contains FR), 601-1200m → I, 1201m+ → T.
        Only returns zones present in the combo.
        """
        parts = combo_zone.split("/")
        has_fr = "FR" in parts
        if dist_m <= 600:
            preferred = "FR" if has_fr else "R"
        elif dist_m <= 1200:
            preferred = "I"
        else:
            preferred = "T"
        return preferred if preferred in parts else parts[-1]  # fallback to fastest

    # ── Intensity Score ────────────────────────────────────────────
    INTENSITY_PTS_PER_MIN = {
        "walk": 0, "E": 0.2, "M": 0.4, "T": 0.6,
        "I": 1.0, "R": 1.5, "FR": 2.0,
    }
    STRIDE_PTS = 0.2   # flat per stride
    HILL_PTS = 0.25     # flat per hill sprint

    def _compute_intensity_scores(conn, activity_ids):
        """Compute I-score per activity from interval pace zones and durations.

        Rules:
        - Walking intervals: 0 points
        - Strides: 0.2 pts each (flat, ignoring duration/pace)
        - Hill sprints: 0.25 pts each (flat, ignoring duration/pace)
        - Pace zone == "walk" but NOT is_walking: treat as E (0.2 pts/min)
        - Otherwise: zone multiplier * duration_minutes
        - Fallback: if no intervals, use activity-level pace to classify zone
          and multiply by activity duration
        Uses FIT-preferred logic (skip strava_laps when FIT intervals exist).
        """
        from runbase.analysis.vdot import classify_pace, vdot_to_boundaries, get_current_vdot

        if not activity_ids:
            return {}
        placeholders = ",".join("?" * len(activity_ids))
        rows = conn.execute(
            f"""SELECT activity_id, pace_zone, duration_s, source,
                       is_stride, is_hill_sprint, is_walking, is_recovery
                FROM intervals
                WHERE activity_id IN ({placeholders})""",
            activity_ids,
        ).fetchall()

        # Group by activity
        by_act = {}
        for r in rows:
            by_act.setdefault(r["activity_id"], []).append(r)

        # Determine which activities have manual laps (FIT, strava_lap, xlsx_split)
        # vs only pace_segments. When manual laps exist, skip pace_segments
        # to avoid double-counting the same time period.
        has_manual_laps = set()
        has_fit = set()
        has_manual_splits = set()
        for aid, intervals in by_act.items():
            for iv in intervals:
                if iv["source"] == "manual_split":
                    has_manual_splits.add(aid)
                elif iv["source"] != "pace_segment":
                    has_manual_laps.add(aid)
                if iv["source"] is None:
                    has_fit.add(aid)

        # Pre-fetch activity durations for interval coverage checks
        act_dur_map = {}
        if by_act:
            dur_ids = list(by_act.keys())
            dur_ph = ",".join("?" * len(dur_ids))
            for ar in conn.execute(
                f"SELECT id, duration_s FROM activities WHERE id IN ({dur_ph})",
                dur_ids,
            ).fetchall():
                act_dur_map[ar["id"]] = ar["duration_s"] or 0

        # Coverage check: if manual laps cover < 10% of activity duration,
        # treat as if no manual laps exist (use pace_segments or fallback).
        # Handles cases like a FIT file that only captured a few seconds.
        for aid in list(has_manual_laps):
            manual_dur = sum(
                (iv["duration_s"] or 0)
                for iv in by_act[aid]
                if iv["source"] != "pace_segment"
            )
            act_dur = act_dur_map.get(aid, 0)
            if act_dur > 60 and manual_dur / act_dur < 0.10:
                has_manual_laps.discard(aid)

        # Pre-fetch activity distance/duration for manual_split remainder calculation
        act_info = {}
        if has_manual_splits:
            ms_ph = ",".join("?" * len(has_manual_splits))
            ms_ids = list(has_manual_splits)
            for ar in conn.execute(
                f"""SELECT id, COALESCE(adjusted_distance_mi, distance_mi, 0) as dist,
                           duration_s, date
                    FROM activities WHERE id IN ({ms_ph})""",
                ms_ids,
            ).fetchall():
                act_info[ar["id"]] = dict(ar)

        result = {}
        for aid, intervals in by_act.items():
            # Manual splits override all other interval sources for I-score
            if aid in has_manual_splits:
                splits = [iv for iv in intervals if iv["source"] == "manual_split"]
                # Also collect strides from other intervals
                other = [iv for iv in intervals if iv["source"] != "manual_split"
                         and iv["source"] != "pace_segment"]
                if aid in has_fit:
                    other = [iv for iv in other if iv["source"] != "strava_lap"]

                score = 0.0
                split_dist = 0.0
                split_dur = 0.0

                # Score each split
                for iv in splits:
                    dur_min = (iv["duration_s"] or 0) / 60.0
                    dist = iv.get("canonical_distance_mi") or 0
                    split_dist += dist
                    split_dur += iv["duration_s"] or 0
                    if dur_min <= 0:
                        continue
                    zone = iv["pace_zone"] or "E"
                    if zone == "walk":
                        zone = "E"
                    score += INTENSITY_PTS_PER_MIN.get(zone, 0.2) * dur_min

                # Strides from other intervals
                for iv in other:
                    if iv["is_stride"]:
                        score += STRIDE_PTS
                    elif iv["is_hill_sprint"]:
                        score += HILL_PTS

                # Compute remainder (warmup/cooldown not covered by splits)
                info = act_info.get(aid)
                if info:
                    remainder_dist = (info["dist"] or 0) - split_dist
                    remainder_dur = (info["duration_s"] or 0) - split_dur
                    if remainder_dist > 0 and remainder_dur > 0:
                        remainder_pace = remainder_dur / remainder_dist
                        # Classify remainder pace
                        act_date = info["date"]
                        vdot = get_current_vdot(conn, act_date)
                        if vdot:
                            bounds = vdot_to_boundaries(vdot)
                            r_zone = classify_pace(remainder_pace, bounds)
                        else:
                            r_zone = "E"
                        if r_zone == "walk":
                            r_zone = "E"
                        score += INTENSITY_PTS_PER_MIN.get(r_zone, 0.2) * (remainder_dur / 60.0)

                result[aid] = round(score, 1) if score > 0 else None
                continue

            # Skip pace_segments when manual laps exist (avoid double-counting)
            if aid in has_manual_laps:
                visible = [iv for iv in intervals if iv["source"] != "pace_segment"]
            else:
                visible = intervals
            # FIT-preferred: skip strava_lap when FIT exists
            if aid in has_fit:
                visible = [iv for iv in visible if iv["source"] != "strava_lap"]

            score = 0.0
            scored_dur = 0.0
            for iv in visible:
                if iv["is_walking"]:
                    continue  # 0 points
                if iv["is_stride"]:
                    score += STRIDE_PTS
                    continue
                if iv["is_hill_sprint"]:
                    score += HILL_PTS
                    continue
                duration_min = (iv["duration_s"] or 0) / 60.0
                if duration_min <= 0:
                    continue
                scored_dur += iv["duration_s"] or 0
                zone = iv["pace_zone"]
                if zone == "walk":
                    # Manually marked as running (not is_walking) — count as E
                    score += INTENSITY_PTS_PER_MIN["E"] * duration_min
                else:
                    score += INTENSITY_PTS_PER_MIN.get(zone, 0) * duration_min

            # If intervals cover < 10% of activity duration, skip to fallback
            act_dur = act_dur_map.get(aid, 0)
            if act_dur > 60 and scored_dur / act_dur < 0.10:
                continue  # result[aid] not set → fallback will handle
            result[aid] = round(score, 1) if score > 0 else None

        # Fallback: activities with no intervals — use activity-level pace + duration
        missing = [aid for aid in activity_ids if aid not in result or result[aid] is None]
        if missing:
            ph2 = ",".join("?" * len(missing))
            act_rows = conn.execute(
                f"""SELECT id, date, avg_pace_s_per_mi, duration_s,
                           COALESCE(adjusted_distance_mi, distance_mi) as dist
                    FROM activities WHERE id IN ({ph2})""",
                missing,
            ).fetchall()
            # Cache VDOT boundaries + E-pace by date to avoid repeated lookups
            boundary_cache = {}
            for ar in act_rows:
                pace = ar["avg_pace_s_per_mi"]
                dur = ar["duration_s"]
                dist = ar["dist"]
                act_date = ar["date"]

                if act_date not in boundary_cache:
                    vdot = get_current_vdot(conn, act_date)
                    if vdot:
                        bounds = vdot_to_boundaries(vdot)
                        # E-pace = boundary between E and M (slower side of M)
                        e_pace = bounds.get("E", 570)
                    else:
                        bounds = None
                        e_pace = 570  # ~9:30/mi default
                    boundary_cache[act_date] = (bounds, e_pace)
                bounds, e_pace = boundary_cache[act_date]

                if dist and dist > 0 and dur and dur > 0:
                    # Always derive pace from distance/duration (canonical)
                    pace = dur / dist
                    if bounds:
                        zone = classify_pace(pace, bounds)
                    else:
                        zone = "E"
                    if zone == "walk":
                        zone = "E"  # a real activity is running, not walking
                    score = INTENSITY_PTS_PER_MIN.get(zone, 0.2) * (dur / 60.0)
                elif dist and dist > 0:
                    # Distance only, no duration: assume 9:00/mi pace
                    est_dur_min = (dist * 540) / 60.0
                    score = INTENSITY_PTS_PER_MIN["E"] * est_dur_min
                else:
                    continue
                result[ar["id"]] = round(score, 1)

        return result

    # ── Planned Activity Estimation ──────────────────────────────

    def _parse_workout_structure(name):
        """Parse workout name into structured reps.

        Supports:
        - Distance-based: "8x200m", "6x400", "4x1mi", "3x1k"
        - Time-based: "6x(3min I, 2min E)", "4x(5min T)"
        """
        if not name:
            return None

        # Time-based: "6x(3min I, 2min E)" or "6x(3min on, 2min off)"
        ZONE_ALIASES = {"ON": None, "OFF": "E"}  # ON = use row's zone
        ZONE_NAMES = r'E|M|T|I|FR|R|on|off'
        time_pat = re.compile(
            r'(\d+)\s*x\s*\(?\s*(\d+\.?\d*)\s*min(?:s|utes?)?\s+(' + ZONE_NAMES + r')'
            r'(?:\s*[,/]\s*(\d+\.?\d*)\s*min(?:s|utes?)?\s+(' + ZONE_NAMES + r'))?\s*\)?',
            re.IGNORECASE,
        )
        m = time_pat.search(name)
        if m:
            work_z = m.group(3).upper()
            result = {
                "type": "time",
                "reps": int(m.group(1)),
                "work_time_min": float(m.group(2)),
                "work_zone": ZONE_ALIASES.get(work_z, work_z),  # None = defer to row zone
            }
            if m.group(4) and m.group(5):
                rec_z = m.group(5).upper()
                result["recovery_time_min"] = float(m.group(4))
                result["recovery_zone"] = ZONE_ALIASES.get(rec_z, rec_z)
            return result

        # Distance-based: "8x200m", "6x400", "4x1mi @R", "3x1k @I"
        # Finds ALL NxD groups (e.g. "2x600 @R, 3x400 @I, 4x200 @FR") and sums them
        dist_pat = re.compile(
            r'(\d+)\s*x\s*(\d+\.?\d*)\s*(mile|mi|k|km|m)?(?:\s*@\s*(FR|R|I|T|M|E))?',
            re.IGNORECASE,
        )
        matches = dist_pat.findall(name)
        if matches:
            total_work_mi = 0.0
            groups = []
            for reps_s, val_s, unit_s, zone_s in matches:
                reps = int(reps_s)
                val = float(val_s)
                unit = unit_s.lower() if unit_s else ""
                if unit in ("mile", "mi"):
                    dist_mi = val
                    dist_m = val * 1609.344
                elif unit in ("k", "km"):
                    dist_m = val * 1000
                    dist_mi = dist_m / 1609.344
                elif unit == "m":
                    dist_m = val
                    dist_mi = val / 1609.344
                else:
                    if val <= 10:
                        dist_mi = val
                        dist_m = val * 1609.344
                    else:
                        dist_m = val
                        dist_mi = val / 1609.344
                total_work_mi += reps * dist_mi
                groups.append({
                    "reps": reps,
                    "dist_mi": dist_mi,
                    "dist_m": dist_m,
                    "zone_override": zone_s.upper() if zone_s else None,
                })

            return {
                "type": "distance",
                "groups": groups,
                "total_work_dist_mi": total_work_mi,
            }

        return None

    def _get_avg_e_pace(conn, target_date, vdot=None):
        """Get average E-pace (s/mi) from recent E-zone activities.

        Fallback chain: 30-day weighted average -> VDOT E-pace -> 570 s/mi.
        """
        window_start = (date.fromisoformat(target_date) - timedelta(days=30)).isoformat()
        rows = conn.execute(
            """SELECT id, COALESCE(adjusted_distance_mi, distance_mi, 0) as dist, duration_s
               FROM activities
               WHERE date BETWEEN ? AND ? AND duration_s > 0""",
            (window_start, target_date),
        ).fetchall()

        if rows:
            act_ids = [r["id"] for r in rows]
            wt = _get_workout_types(conn, act_ids)
            total_dur = 0.0
            total_dist = 0.0
            for r in rows:
                if wt.get(r["id"], "E") == "E" and r["dist"] > 0:
                    total_dur += r["duration_s"]
                    total_dist += r["dist"]
            if total_dist > 0:
                return total_dur / total_dist

        if vdot:
            from runbase.analysis.vdot import vdot_to_paces
            return vdot_to_paces(vdot).get("E", 570)

        return 570  # ~9:30/mi default

    def _estimate_planned_iscore(distance_mi, name, zone, strides, vdot, e_pace):
        """Estimate I-score for a planned activity.

        Handles three cases:
        1. Time-based structured: "6x(3min I, 2min E)"
        2. Distance-based structured: "8x200m" with non-E zone + VDOT
        3. E fallback: all distance at E pace + strides
        """
        if not distance_mi or distance_mi <= 0:
            return None

        strides = strides or 0
        stride_dist = strides * 0.05  # ~80m per stride
        structure = _parse_workout_structure(name)

        # ── Time-based structured workout ──
        if structure and structure["type"] == "time":
            reps = structure["reps"]
            work_zone = structure["work_zone"] or zone or "E"  # "on" defers to row zone
            total_work_min = reps * structure["work_time_min"]
            work_mult = INTENSITY_PTS_PER_MIN.get(work_zone, 0.2)
            score = total_work_min * work_mult

            # Recovery
            if "recovery_time_min" in structure:
                rec_time_min = reps * structure["recovery_time_min"]
                rec_zone = structure.get("recovery_zone") or "E"
                rec_mult = INTENSITY_PTS_PER_MIN.get(rec_zone, 0.2)
                score += rec_time_min * rec_mult
                # Estimate recovery distance for WU/CD calc
                rec_pace = e_pace
                if vdot and rec_zone != "E":
                    from runbase.analysis.vdot import vdot_to_paces
                    rec_pace = vdot_to_paces(vdot).get(rec_zone, e_pace)
                rec_dist = rec_time_min * 60 / rec_pace
            else:
                # Default recovery by zone
                if work_zone == "T":
                    rec_time_min = 0
                elif work_zone == "FR":
                    rec_time_min = total_work_min * 2
                elif work_zone in ("I", "M"):
                    rec_time_min = total_work_min * (2 / 3)
                else:  # R, E
                    rec_time_min = total_work_min
                score += rec_time_min * 0.2
                rec_dist = rec_time_min * 60 / e_pace if rec_time_min else 0

            # Estimate work distance for WU/CD calc
            work_pace = e_pace
            if vdot:
                from runbase.analysis.vdot import vdot_to_paces
                work_pace = vdot_to_paces(vdot).get(work_zone, e_pace)
            work_dist = total_work_min * 60 / work_pace

            wucd_dist = max(distance_mi - work_dist - rec_dist - stride_dist, 0)
            wucd_time_min = wucd_dist * e_pace / 60
            score += wucd_time_min * 0.2
            score += strides * STRIDE_PTS

            return round(score, 1) if score > 0 else None

        # ── Distance-based structured workout ──
        if structure and structure["type"] == "distance" and zone and zone != "E" and vdot:
            from runbase.analysis.vdot import vdot_to_paces
            paces = vdot_to_paces(vdot)
            is_combo = "/" in zone
            groups = structure.get("groups", [])

            score = 0.0
            total_work_dist = 0.0
            total_rec_dist = 0.0

            for g in groups:
                # Determine zone for this group
                if g.get("zone_override"):
                    g_zone = g["zone_override"]
                elif is_combo:
                    g_zone = _zone_for_distance_m(g["dist_m"], zone)
                else:
                    g_zone = zone

                g_work_dist = g["reps"] * g["dist_mi"]
                g_pace = paces.get(g_zone, e_pace)
                g_work_min = g_work_dist * g_pace / 60
                g_mult = INTENSITY_PTS_PER_MIN.get(g_zone, 0.2)
                score += g_work_min * g_mult
                total_work_dist += g_work_dist

                # Recovery by zone
                if g_zone == "T":
                    g_rec_min = 0
                    g_rec_dist = 0
                elif g_zone == "R":
                    g_rec_dist = g_work_dist
                    g_rec_min = g_rec_dist * e_pace / 60
                elif g_zone == "FR":
                    g_rec_min = g_work_min * 2
                    g_rec_dist = g_rec_min * 60 / e_pace
                else:  # I, M
                    g_rec_min = g_work_min * (2 / 3)
                    g_rec_dist = g_rec_min * 60 / e_pace

                score += g_rec_min * 0.2
                total_rec_dist += g_rec_dist

            wucd_dist = max(distance_mi - total_work_dist - total_rec_dist - stride_dist, 0)
            wucd_time_min = wucd_dist * e_pace / 60
            score += wucd_time_min * 0.2
            score += strides * STRIDE_PTS

            return round(score, 1) if score > 0 else None

        # ── E workout (fallback) ──
        e_dist = max(distance_mi - stride_dist, 0)
        e_time_min = e_dist * e_pace / 60
        score = e_time_min * 0.2 + strides * STRIDE_PTS

        return round(score, 1) if score > 0 else None

    def _build_activity(r, overrides_map, shoes, has_streams_set, workout_types, intensity_scores=None):
        a = dict(r)
        ovr = overrides_map.get(a["id"], {})
        overridden = _apply_overrides(a, ovr)

        display_dist = a.get("adjusted_distance_mi") or a.get("distance_mi")
        a["display_distance"] = f"{display_dist:.2f}" if display_dist else ""
        a["display_duration"] = _format_duration(a.get("duration_s"))
        a["display_pace"] = _format_pace(a.get("avg_pace_s_per_mi"))
        a["display_hr"] = f"{a['avg_hr']:.0f}" if a.get("avg_hr") else ""
        a["display_cadence"] = f"{a['avg_cadence']:.0f}" if a.get("avg_cadence") else ""
        a["shoe_name"] = shoes.get(a.get("shoe_id"), "")
        a["has_streams"] = a["id"] in has_streams_set
        a["overridden_fields"] = list(overridden)
        a["workout_type_zone"] = ovr.get("workout_type_zone", workout_types.get(a["id"], ""))
        wtz = a["workout_type_zone"]
        a["zone_class"] = ("zone-" + wtz.split("/")[0]) if wtz else ""
        a["intensity_score"] = (intensity_scores or {}).get(a["id"])

        dt = datetime.strptime(a["date"], "%Y-%m-%d")
        a["day_of_week"] = dt.strftime("%a")
        a["date_short"] = dt.strftime("%-m/%-d")
        a["month_num"] = dt.month
        a["week_of_month"] = min(_week_of_month(dt), 5)

        return a

    def _merge_day(day_acts):
        """Merge multiple activities on the same day into one summary row.

        Distance/duration/strides are summed. Pace/HR/cadence/shoe/name/notes/vdot
        come from the primary (largest distance) activity. All activity IDs are
        kept for detail expansion.
        """
        if len(day_acts) == 1:
            a = day_acts[0].copy()
            a["activity_ids"] = [a["id"]]
            return a

        # Primary = largest distance
        primary = max(day_acts, key=lambda a: (a.get("adjusted_distance_mi") or a.get("distance_mi") or 0))
        merged = primary.copy()
        merged["activity_ids"] = [a["id"] for a in day_acts]

        total_dist = sum((a.get("adjusted_distance_mi") or a.get("distance_mi") or 0) for a in day_acts)
        total_dur = sum((a.get("duration_s") or 0) for a in day_acts)
        total_strides = sum((a.get("strides") or 0) for a in day_acts)

        merged["adjusted_distance_mi"] = total_dist
        merged["distance_mi"] = total_dist
        merged["duration_s"] = total_dur
        merged["display_distance"] = f"{total_dist:.2f}" if total_dist else ""
        merged["display_duration"] = _format_duration(total_dur) if total_dur else ""
        merged["strides"] = total_strides or None
        total_iscore = sum((a.get("intensity_score") or 0) for a in day_acts)
        merged["intensity_score"] = round(total_iscore, 1) if total_iscore > 0 else None
        # Keep primary's pace/HR/cadence/shoe/name/vdot
        merged["has_streams"] = any(a["has_streams"] for a in day_acts)
        merged["overridden_fields"] = list(set().union(*(a["overridden_fields"] for a in day_acts)))

        # Pick highest-priority workout type zone across all activities.
        # For combo types like "I/R", use the fastest (last) component for priority.
        def _max_zone_prio(z):
            if not z:
                return 0
            return max(ZONE_PRIORITY.get(part, 0) for part in z.split("/"))

        best_zone = ""
        for a in day_acts:
            z = a.get("workout_type_zone", "")
            if _max_zone_prio(z) > _max_zone_prio(best_zone):
                best_zone = z
        merged["workout_type_zone"] = best_zone
        merged["zone_class"] = ("zone-" + best_zone.split("/")[0]) if best_zone else ""

        # Collect names if multiple
        names = [a.get("workout_name") or "" for a in day_acts if a.get("workout_name")]
        if len(names) > 1:
            merged["workout_name"] = " + ".join(names)

        return merged

    # ── Jinja2 Filters ─────────────────────────────────────────────

    @app.template_filter("parse_date")
    def _filter_parse_date(value):
        return datetime.strptime(value, "%Y-%m-%d")

    @app.template_filter("monday_of_week")
    def _filter_monday_of_week(dt):
        return dt - timedelta(days=dt.weekday())

    # ── Routes ───────────────────────────────────────────────────────

    @app.route("/")
    def index():
        year = request.args.get("year", type=int, default=date.today().year)
        conn = get_db()
        today = date.today()

        start = f"{year}-01-01"
        end = f"{year}-12-31"

        # Fetch prior days for 7d MA (6 days) and ATL/CTL EMA seeding (~90 days)
        seed_start = (date(year, 1, 1) - timedelta(days=90)).isoformat()
        prior_rows = conn.execute(
            """SELECT id, date, COALESCE(adjusted_distance_mi, distance_mi, 0) as dist
               FROM activities
               WHERE date BETWEEN ? AND ?""",
            (seed_start, f"{year - 1}-12-31"),
        ).fetchall()
        daily_dist = {}
        prior_ids = [r["id"] for r in prior_rows]
        for r in prior_rows:
            daily_dist[r["date"]] = daily_dist.get(r["date"], 0) + r["dist"]

        # Compute I-scores for prior activities (for ATL/CTL seeding)
        prior_iscores = _compute_intensity_scores(conn, prior_ids) if prior_ids else {}
        daily_iscore = {}
        for r in prior_rows:
            iscore = prior_iscores.get(r["id"], 0) or 0
            daily_iscore[r["date"]] = daily_iscore.get(r["date"], 0) + iscore

        rows = conn.execute(
            """SELECT a.*, s.name as shoe_name
               FROM activities a
               LEFT JOIN shoes s ON a.shoe_id = s.id
               WHERE a.date BETWEEN ? AND ?
               ORDER BY a.date ASC, a.start_time ASC""",
            (start, end),
        ).fetchall()

        activity_ids = [r["id"] for r in rows]
        overrides_map = _get_overrides_for_activities(conn, activity_ids)
        shoes = _get_shoes(conn)

        has_streams = set()
        if activity_ids:
            ph = ",".join("?" * len(activity_ids))
            stream_rows = conn.execute(
                f"SELECT DISTINCT activity_id FROM streams "
                f"WHERE activity_id IN ({ph}) AND lat IS NOT NULL",
                activity_ids,
            ).fetchall()
            has_streams = {r["activity_id"] for r in stream_rows}

        workout_types = _get_workout_types(conn, activity_ids)
        intensity_scores = _compute_intensity_scores(conn, activity_ids)

        # Fetch planned activities for the year (used in blank calendar rows + 7d MA)
        planned_rows = conn.execute(
            "SELECT date, distance_mi, workout_name, workout_type_zone, strides "
            "FROM planned_activities WHERE date BETWEEN ? AND ?",
            (start, end),
        ).fetchall()
        planned_map = {r["date"]: dict(r) for r in planned_rows}

        # Build activities grouped by date
        activities = []
        months_with_data = set()
        by_date = {}
        for r in rows:
            a = _build_activity(r, overrides_map, shoes, has_streams, workout_types, intensity_scores)
            activities.append(a)
            months_with_data.add(a["month_num"])
            by_date.setdefault(a["date"], []).append(a)
            # Accumulate daily distances and I-scores
            dist = a.get("adjusted_distance_mi") or a.get("distance_mi") or 0
            daily_dist[a["date"]] = daily_dist.get(a["date"], 0) + dist
            daily_iscore[a["date"]] = daily_iscore.get(a["date"], 0) + (a.get("intensity_score") or 0)

        # Compute AVD (Adjusted VDOT) for each activity
        mult = _avd_multiplier(conn)
        # Load CVDs: seed 21 days before Jan 1 for early-year activities
        avd_seed = (date(year, 1, 1) - timedelta(days=AVD_WINDOW_DAYS)).isoformat()
        avd_cvd_rows = conn.execute(
            "SELECT date, computed_vdot, duration_s FROM activities "
            "WHERE computed_vdot IS NOT NULL AND NOT COALESCE(exclude_from_vdot, 0) AND date >= ? ORDER BY date",
            (avd_seed,),
        ).fetchall()
        avd_cvd_list = [(r[0], r[1], r[2] or 0) for r in avd_cvd_rows]
        for a in activities:
            ewa, _ = _avd_ewa(a["date"], avd_cvd_list, inclusive=True)
            a["adjusted_vdot"] = round(ewa * mult, 1) if ewa is not None else None

        # Add planned distances and estimated I-scores to daily totals
        # (only for dates without real activities)
        _vdot_cache = {}
        _epace_cache = {}
        for p_date, p_data in planned_map.items():
            if p_date not in by_date and p_data.get("distance_mi"):
                daily_dist[p_date] = daily_dist.get(p_date, 0) + p_data["distance_mi"]
                # Compute estimated I-score
                if p_date not in _vdot_cache:
                    from runbase.analysis.vdot import get_current_vdot
                    _vdot_cache[p_date] = get_current_vdot(conn, p_date)
                if p_date not in _epace_cache:
                    _epace_cache[p_date] = _get_avg_e_pace(conn, p_date, _vdot_cache[p_date])
                est = _estimate_planned_iscore(
                    p_data["distance_mi"],
                    p_data.get("workout_name"),
                    p_data.get("workout_type_zone"),
                    p_data.get("strides"),
                    _vdot_cache[p_date],
                    _epace_cache[p_date],
                )
                if est:
                    daily_iscore[p_date] = daily_iscore.get(p_date, 0) + est
                    p_data["estimated_iscore"] = est

        # Pre-compute ATL/CTL EMAs by walking all days from seed through year end
        lambda_a = 2.0 / (7 + 1)    # ATL: 7-day decay
        lambda_f = 2.0 / (42 + 1)   # CTL: 42-day decay
        atl = 0.0
        ctl = 0.0
        seed_dt = date.fromisoformat(seed_start)
        year_end = date(year, 12, 31)
        atl_ctl_map = {}  # date_str -> (atl, ctl)
        d_cursor = seed_dt
        while d_cursor <= year_end:
            trimp = daily_iscore.get(d_cursor.isoformat(), 0)
            atl = trimp * lambda_a + (1 - lambda_a) * atl
            ctl = trimp * lambda_f + (1 - lambda_f) * ctl
            if d_cursor.year == year:
                atl_ctl_map[d_cursor.isoformat()] = (round(atl, 1), round(ctl, 1))
            d_cursor += timedelta(days=1)

        # Build full calendar: one row per day (merged if multiple activities)
        month_calendars = {}
        for m in range(1, 13):
            days_in_month = monthrange(year, m)[1]
            cal = []
            for d in range(1, days_in_month + 1):
                dt = date(year, m, d)
                date_str = dt.isoformat()
                day_acts = by_date.get(date_str, [])
                wom = min(_week_of_month(datetime(year, m, d)), 5)

                # 7-day trailing mileage sum
                seven_day = 0.0
                for offset in range(7):
                    dd = dt - timedelta(days=offset)
                    seven_day += daily_dist.get(dd.isoformat(), 0)
                is_saturday = dt.weekday() == 5  # Saturday
                ma_display = f"{seven_day:.1f}"

                # 7-day trailing intensity sum
                seven_day_i = 0.0
                for offset in range(7):
                    dd = dt - timedelta(days=offset)
                    seven_day_i += daily_iscore.get(dd.isoformat(), 0)
                i7_display = f"{seven_day_i:.1f}" if seven_day_i > 0 else ""

                # ATL / CTL
                day_atl, day_ctl = atl_ctl_map.get(date_str, (0, 0))
                atl_display = f"{day_atl:.1f}" if day_atl >= 0.05 else ""
                ctl_display = f"{day_ctl:.1f}" if day_ctl >= 0.05 else ""

                if day_acts:
                    merged = _merge_day(day_acts)
                    merged["week_of_month"] = wom
                    merged["seven_day_ma"] = ma_display
                    merged["seven_day_i"] = i7_display
                    merged["atl"] = atl_display
                    merged["ctl"] = ctl_display
                    merged["is_saturday"] = is_saturday
                    cal.append({"blank": False, "activity": merged})
                else:
                    planned = planned_map.get(date_str)
                    cal.append({
                        "blank": True,
                        "date_str": date_str,
                        "day_of_week": dt.strftime("%a"),
                        "date_short": dt.strftime("%-m/%-d"),
                        "month_num": m,
                        "week_of_month": wom,
                        "seven_day_ma": ma_display,
                        "seven_day_i": i7_display,
                        "atl": atl_display,
                        "ctl": ctl_display,
                        "is_saturday": is_saturday,
                        "planned": planned,
                        "is_future": dt >= today,
                    })
            month_calendars[m] = cal

        # Weekly aggregates (keyed by Sunday start)
        weeks = {}
        for a in activities:
            dt = datetime.strptime(a["date"], "%Y-%m-%d")
            # Sunday-start week: shift so Sunday=0
            days_since_sunday = (dt.weekday() + 1) % 7
            sunday = dt - timedelta(days=days_since_sunday)
            key = sunday.isoformat()
            if key not in weeks:
                weeks[key] = {
                    "monday": sunday,  # kept as "monday" for sort compat
                    "label": f"{sunday.strftime('%-m/%-d')} - {(sunday + timedelta(days=6)).strftime('%-m/%-d')}",
                    "distance": 0.0, "duration": 0.0, "count": 0,
                }
            dist = a.get("adjusted_distance_mi") or a.get("distance_mi") or 0
            weeks[key]["distance"] += dist
            weeks[key]["duration"] += a.get("duration_s") or 0
            weeks[key]["count"] += 1

        for w in weeks.values():
            w["display_distance"] = f"{w['distance']:.1f}"
            w["display_duration"] = _format_duration(w["duration"])

        sorted_weeks = sorted(weeks.values(), key=lambda w: w["monday"])

        # Monthly stats for bottom panel
        monthly_stats = []
        yearly_distance = 0.0
        yearly_duration = 0.0
        yearly_pace_distance = 0.0
        yearly_pace_duration = 0.0
        yearly_count = 0
        longest_run = 0.0
        max_week_dist = 0.0

        for m in range(1, 13):
            month_acts = [day["activity"] for day in month_calendars.get(m, []) if not day["blank"]]
            dist = sum((a.get("adjusted_distance_mi") or a.get("distance_mi") or 0) for a in month_acts)
            dur = sum((a.get("duration_s") or 0) for a in month_acts)
            count = len(month_acts)
            # Pace: distance-weighted average of per-activity pace (which
            # already excludes walking time), so stride workouts don't inflate
            # the monthly average with their walking duration.
            pace_dist = 0.0
            pace_dur = 0.0
            for a in month_acts:
                d = a.get("adjusted_distance_mi") or a.get("distance_mi") or 0
                p = a.get("avg_pace_s_per_mi")
                if a.get("duration_s") and p and d:
                    pace_dist += d
                    pace_dur += p * d
            avg_pace = (pace_dur / pace_dist) if pace_dist > 0 else None

            # Average weekly mileage: total miles / number of weeks elapsed
            days_in_m = monthrange(year, m)[1]
            if year == today.year and m == today.month:
                # Current month: count days through today (or yesterday if no run today)
                effective_day = today.day
                elapsed_days = effective_day
            elif year < today.year or (year == today.year and m < today.month):
                # Past month: full month
                elapsed_days = days_in_m
            else:
                # Future month
                elapsed_days = 0
            elapsed_weeks = elapsed_days / 7.0 if elapsed_days > 0 else 0
            avg_weekly = (dist / elapsed_weeks) if elapsed_weeks > 0 else 0

            monthly_stats.append({
                "month": m,
                "name": MONTH_NAMES[m][:3],
                "distance": dist,
                "duration": dur,
                "count": count,
                "display_distance": f"{dist:.1f}",
                "display_duration": _format_duration(dur),
                "display_pace": _format_pace(avg_pace),
                "avg_weekly": f"{avg_weekly:.1f}",
            })

            yearly_distance += dist
            yearly_duration += dur
            yearly_pace_distance += pace_dist
            yearly_pace_duration += pace_dur
            yearly_count += count
            for a in month_acts:
                d = a.get("adjusted_distance_mi") or a.get("distance_mi") or 0
                if d > longest_run:
                    longest_run = d

        for w in sorted_weeks:
            if w["distance"] > max_week_dist:
                max_week_dist = w["distance"]

        max_month_dist = max((s["distance"] for s in monthly_stats), default=1) or 1
        yearly_avg_pace = (yearly_pace_duration / yearly_pace_distance) if yearly_pace_distance > 0 else None

        # ── Weekly mileage chart data (prior 6 months) ──────────────
        chart_start = (today - timedelta(days=180)).isoformat()
        chart_end = today.isoformat()
        chart_rows = conn.execute(
            """SELECT date, COALESCE(adjusted_distance_mi, distance_mi, 0) as dist
               FROM activities
               WHERE date BETWEEN ? AND ?""",
            (chart_start, chart_end),
        ).fetchall()
        # Build weekly buckets (Sun-Sat, keyed by Saturday end date)
        chart_daily = {}
        for r in chart_rows:
            chart_daily[r["date"]] = chart_daily.get(r["date"], 0) + r["dist"]

        # Generate all Saturdays in the range
        weekly_chart_data = []
        # Find first Saturday on or after chart_start
        cs = date.fromisoformat(chart_start)
        first_sat = cs + timedelta(days=(5 - cs.weekday()) % 7)
        sat = first_sat
        while sat <= today:
            week_dist = 0.0
            for offset in range(7):
                d = sat - timedelta(days=offset)
                week_dist += chart_daily.get(d.isoformat(), 0)
            weekly_chart_data.append({
                "date": sat.isoformat(),
                "label": sat.strftime("%-m/%-d"),
                "distance": round(week_dist, 1),
            })
            sat += timedelta(days=7)

        # Year range for calendar
        year_range_row = conn.execute(
            "SELECT MIN(substr(date,1,4)) as min_y, MAX(substr(date,1,4)) as max_y FROM activities"
        ).fetchone()
        min_year = int(year_range_row["min_y"]) if year_range_row["min_y"] else year
        max_year = int(year_range_row["max_y"]) if year_range_row["max_y"] else year

        conn.close()

        # Cache bust key: max mtime of static files
        static_dir = Path(__file__).parent / "static"
        cache_bust = int(max(f.stat().st_mtime for f in static_dir.iterdir() if f.is_file()))

        return render_template(
            "index.html",
            activities=activities,
            month_calendars=month_calendars,
            sorted_weeks=sorted_weeks,
            year=year,
            months_with_data=months_with_data,
            month_names=MONTH_NAMES,
            monthly_stats=monthly_stats,
            max_month_dist=max_month_dist,
            yearly_distance=yearly_distance,
            yearly_duration=yearly_duration,
            yearly_count=yearly_count,
            yearly_avg_pace=_format_pace(yearly_avg_pace),
            yearly_display_duration=_format_duration(yearly_duration),
            longest_run=longest_run,
            max_week_dist=max_week_dist,
            min_year=min_year,
            max_year=max_year,
            shoes=shoes,
            weekly_chart_data=weekly_chart_data,
            cache_bust=cache_bust,
        )

    @app.route("/api/activities")
    def api_activities():
        start = request.args.get("start")
        end = request.args.get("end")
        if not start or not end:
            return jsonify({"error": "start and end required"}), 400
        conn = get_db()
        rows = conn.execute(
            """SELECT a.*, s.name as shoe_name
               FROM activities a
               LEFT JOIN shoes s ON a.shoe_id = s.id
               WHERE a.date BETWEEN ? AND ?
               ORDER BY a.date ASC""",
            (start, end),
        ).fetchall()
        result = [dict(r) for r in rows]
        conn.close()
        return jsonify(result)

    def _count_visible_strides(conn, activity_id):
        """Count strides from the preferred source only (FIT > Strava).

        Matches the display logic in api_intervals: when both FIT (NULL source)
        and strava_lap intervals exist, only FIT laps are shown.
        """
        rows = conn.execute(
            """SELECT source, is_stride FROM intervals
               WHERE activity_id = ? AND (source IS NULL OR source != 'pace_segment')""",
            (activity_id,),
        ).fetchall()
        fit_intervals = [r for r in rows if r["source"] is None]
        strava_laps = [r for r in rows if r["source"] == "strava_lap"]
        other = [r for r in rows if r["source"] not in (None, "strava_lap")]

        if fit_intervals and strava_laps:
            # FIT preferred: count from FIT + manual/other, exclude Strava
            visible = fit_intervals + other
        else:
            visible = rows

        count = sum(1 for r in visible if r["is_stride"])
        return count or None  # NULL if zero

    def _format_interval(r):
        iv = dict(r)
        dist = iv.get("canonical_distance_mi") or iv.get("gps_measured_distance_mi") or iv.get("prescribed_distance_mi")
        if dist and dist < 1.0:
            iv["display_distance"] = f"{dist * 1609.344:.0f}m"
        elif dist:
            iv["display_distance"] = f"{dist:.2f}mi"
        else:
            iv["display_distance"] = ""
        iv["display_duration"] = _format_duration_precise(iv.get("duration_s"))
        iv["display_pace"] = _format_pace(iv.get("avg_pace_s_per_mi"))
        iv["display_hr"] = f"{iv['avg_hr']:.1f}" if iv.get("avg_hr") else ""
        iv["display_cadence"] = f"{iv['avg_cadence']:.0f}" if iv.get("avg_cadence") else ""
        return iv

    @app.route("/api/activity/<int:activity_id>/intervals")
    def api_intervals(activity_id):
        conn = get_db()
        rows = conn.execute(
            """SELECT * FROM intervals
               WHERE activity_id = ?
                 AND (source IS NULL OR source != 'pace_segment')
               ORDER BY rep_number""",
            (activity_id,),
        ).fetchall()
        intervals = []
        laps = []
        for r in rows:
            iv = _format_interval(r)
            if iv.get("source") == "strava_lap":
                laps.append(iv)
            else:
                intervals.append(iv)

        # When both FIT laps and Strava laps exist, hide Strava laps
        # (FIT has higher GPS resolution; showing both is confusing)
        has_fit = any(iv.get("source") is None for iv in intervals)
        if has_fit and laps:
            laps = []

        # Build rep distance summary from whichever source we're showing
        summary = _build_rep_summary(intervals or laps)

        conn.close()
        return jsonify({"intervals": intervals, "laps": laps, "summary": summary})

    def _build_rep_summary(all_intervals):
        """Group work intervals by rep distance and compute averages.

        Only includes intervals that are part of a workout set (set_number is set),
        excluding warmup, cooldown, recovery, walking, and strides.
        Groups by nearest 50m bucket, then shows actual average distance.
        """
        from collections import defaultdict
        buckets = defaultdict(list)
        for iv in all_intervals:
            if iv.get("is_walking") or iv.get("is_recovery") or iv.get("is_stride"):
                continue
            if iv.get("set_number") is None:
                continue  # skip warmup/cooldown
            dist = iv.get("canonical_distance_mi") or iv.get("gps_measured_distance_mi") or iv.get("prescribed_distance_mi")
            if not dist or dist <= 0:
                continue
            # Round to nearest 50m bucket for grouping
            dist_m = dist * 1609.344
            key_m = round(dist_m / 50) * 50
            buckets[key_m].append(iv)

        summary = []
        for key_m, ivs in sorted(buckets.items()):
            if len(ivs) < 2:
                continue
            # Show actual average distance, not the rounded bucket
            dists_m = [
                (iv.get("canonical_distance_mi") or iv.get("gps_measured_distance_mi") or 0) * 1609.344
                for iv in ivs
            ]
            avg_dist_m = sum(dists_m) / len(dists_m)
            if avg_dist_m < 1609.344:
                label = f"{avg_dist_m:.0f}m"
            else:
                label = f"{avg_dist_m / 1609.344:.2f}mi"
            durations = [iv["duration_s"] for iv in ivs if iv.get("duration_s")]
            paces = [iv["avg_pace_s_per_mi"] for iv in ivs if iv.get("avg_pace_s_per_mi")]
            hrs = [iv["avg_hr"] for iv in ivs if iv.get("avg_hr")]
            summary.append({
                "distance": label,
                "count": len(ivs),
                "avg_duration": _format_duration_precise(sum(durations) / len(durations)) if durations else "",
                "avg_pace": _format_pace(sum(paces) / len(paces)) if paces else "",
                "avg_hr": f"{sum(hrs) / len(hrs):.1f}" if hrs else "",
            })
        return summary

    @app.route("/api/activity/<int:activity_id>/chart")
    def api_chart(activity_id):
        conn = get_db()
        rows = conn.execute(
            """SELECT timestamp_s, pace_s_per_mi, heart_rate FROM streams
               WHERE activity_id = ?
               ORDER BY source_id, timestamp_s""",
            (activity_id,),
        ).fetchall()
        points = [dict(r) for r in rows]
        name_row = conn.execute(
            "SELECT workout_name FROM activities WHERE id = ?", (activity_id,)
        ).fetchone()
        act_name = name_row["workout_name"] if name_row else None
        conn.close()
        if not points:
            return jsonify({"pace": [], "hr": [], "name": act_name})

        # 10-second rolling average for pace
        pace_out = []
        hr_out = []
        window = 10  # seconds
        for i, p in enumerate(points):
            t = p["timestamp_s"] or 0
            # Collect window
            pace_vals = []
            hr_vals = []
            for j in range(max(0, i - window), min(len(points), i + window + 1)):
                pj = points[j]
                tj = pj["timestamp_s"] or 0
                if abs(tj - t) <= window:
                    if pj["pace_s_per_mi"] is not None and 200 < pj["pace_s_per_mi"] < 2000:
                        pace_vals.append(pj["pace_s_per_mi"])
                    if pj["heart_rate"] is not None and 50 < pj["heart_rate"] < 220:
                        hr_vals.append(pj["heart_rate"])

            if pace_vals:
                pace_out.append({"t": t, "v": sum(pace_vals) / len(pace_vals)})
            if hr_vals:
                hr_out.append({"t": t, "v": sum(hr_vals) / len(hr_vals)})

        # Downsample to ~600 points
        def downsample(arr, target=600):
            if len(arr) <= target:
                return arr
            step = len(arr) / target
            out = []
            i = 0.0
            while int(i) < len(arr):
                out.append(arr[int(i)])
                i += step
            return out

        return jsonify({
            "pace": downsample(pace_out),
            "hr": downsample(hr_out),
            "name": act_name,
        })

    @app.route("/api/activity/<int:activity_id>/streams")
    def api_streams(activity_id):
        conn = get_db()
        rows = conn.execute(
            """SELECT lat, lon FROM streams
               WHERE activity_id = ? AND lat IS NOT NULL AND lon IS NOT NULL
               ORDER BY source_id, timestamp_s""",
            (activity_id,),
        ).fetchall()
        points = [dict(r) for r in rows]
        if len(points) > 500:
            step = len(points) / 500
            downsampled = []
            i = 0.0
            while int(i) < len(points):
                downsampled.append(points[int(i)])
                i += step
            points = downsampled
        conn.close()
        return jsonify(points)

    @app.route("/api/activity/<int:activity_id>/meta")
    def api_activity_meta(activity_id):
        conn = get_db()
        row = conn.execute(
            """SELECT a.*, s.name as shoe_name
               FROM activities a
               LEFT JOIN shoes s ON a.shoe_id = s.id
               WHERE a.id = ?""",
            (activity_id,),
        ).fetchone()
        if not row:
            conn.close()
            return jsonify({"error": "not found"}), 404
        a = dict(row)
        ovr = _get_overrides_for_activities(conn, [activity_id]).get(activity_id, {})
        overridden = list(_apply_overrides(a, ovr))

        # Compute auto workout type zone from intervals
        auto_zone = _get_workout_types(conn, [activity_id]).get(activity_id, "")
        workout_type_zone = ovr.get("workout_type_zone", auto_zone)

        conn.close()
        display_dist = a.get("adjusted_distance_mi") or a.get("distance_mi")
        return jsonify({
            "id": a["id"],
            "workout_name": a.get("workout_name") or "",
            "shoe_id": a.get("shoe_id"),
            "shoe_name": a.get("shoe_name") or "",
            "workout_category": a.get("workout_category") or "",
            "workout_type_zone": workout_type_zone,
            "distance_mi": a.get("distance_mi"),
            "adjusted_distance_mi": a.get("adjusted_distance_mi"),
            "display_distance": f"{display_dist:.2f}" if display_dist else "",
            "duration_s": a.get("duration_s"),
            "display_duration": _format_duration(a.get("duration_s")),
            "avg_pace_s_per_mi": a.get("avg_pace_s_per_mi"),
            "display_pace": _format_pace(a.get("avg_pace_s_per_mi")),
            "avg_hr": a.get("avg_hr"),
            "display_hr": f"{a['avg_hr']:.1f}" if a.get("avg_hr") else "",
            "max_hr": a.get("max_hr"),
            "avg_cadence": a.get("avg_cadence"),
            "strides": a.get("strides"),
            "notes": a.get("notes") or "",
            "vdot": a.get("vdot"),
            "gap_s_per_mi": a.get("gap_s_per_mi"),
            "display_gap": _format_pace_tenths(a.get("gap_s_per_mi")),
            "total_ascent_ft": a.get("total_ascent_ft"),
            "total_descent_ft": a.get("total_descent_ft"),
            "temperature_f": a.get("temperature_f"),
            "humidity_pct": a.get("humidity_pct"),
            "weather_conditions": a.get("weather_conditions") or "",
            "start_time": a.get("start_time"),
            "cloud_cover_pct": a.get("cloud_cover_pct"),
            "suppress_hr": bool(a.get("suppress_hr")),
            "suppress_cadence": bool(a.get("suppress_cadence")),
            "exclude_from_vdot": bool(a.get("exclude_from_vdot")),
            "overridden_fields": overridden,
        })

    @app.route("/api/activity/<int:activity_id>/flags", methods=["POST"])
    def api_activity_flags(activity_id):
        data = request.get_json(force=True)
        conn = get_db()
        allowed = {"suppress_hr", "suppress_cadence", "exclude_from_vdot"}
        updates = {}
        for k in allowed:
            if k in data:
                updates[k] = 1 if data[k] else 0
        if not updates:
            conn.close()
            return jsonify({"ok": False, "error": "No valid flags"}), 400
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        conn.execute(
            f"UPDATE activities SET {set_clause} WHERE id = ?",
            (*updates.values(), activity_id),
        )
        conn.commit()
        conn.close()
        return jsonify({"ok": True, **{k: bool(v) for k, v in updates.items()}})

    @app.route("/api/activity/<int:activity_id>/splits", methods=["POST"])
    def api_create_split(activity_id):
        from runbase.analysis.vdot import classify_pace, vdot_to_boundaries, get_current_vdot

        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON body required"}), 400

        distance_mi = data.get("distance_mi")
        duration_s = data.get("duration_s")
        pace_zone = data.get("pace_zone")

        if not distance_mi or not duration_s:
            return jsonify({"error": "distance_mi and duration_s required"}), 400

        try:
            distance_mi = float(distance_mi)
            duration_s = float(duration_s)
        except (ValueError, TypeError):
            return jsonify({"error": "invalid distance or duration"}), 400

        if distance_mi <= 0 or duration_s <= 0:
            return jsonify({"error": "distance and duration must be positive"}), 400

        avg_pace = duration_s / distance_mi

        conn = get_db()

        # Auto-classify zone if not provided
        if not pace_zone:
            row = conn.execute(
                "SELECT date FROM activities WHERE id = ?", (activity_id,)
            ).fetchone()
            if row:
                vdot = get_current_vdot(conn, row["date"])
                if vdot:
                    bounds = vdot_to_boundaries(vdot)
                    pace_zone = classify_pace(avg_pace, bounds)
                    if pace_zone == "walk":
                        pace_zone = "E"

        # Find next rep_number for this activity's manual_split intervals
        max_rep = conn.execute(
            "SELECT MAX(rep_number) as mr FROM intervals "
            "WHERE activity_id = ? AND source = 'manual_split'",
            (activity_id,),
        ).fetchone()["mr"]
        rep_number = (max_rep or 0) + 1

        cursor = conn.execute(
            """INSERT INTO intervals
               (activity_id, rep_number, canonical_distance_mi, duration_s,
                avg_pace_s_per_mi, avg_pace_display, pace_zone, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'manual_split')""",
            (activity_id, rep_number, distance_mi, duration_s,
             avg_pace, _format_pace(avg_pace), pace_zone),
        )
        interval_id = cursor.lastrowid
        conn.commit()

        # Fetch and format the new interval
        row = conn.execute(
            "SELECT * FROM intervals WHERE id = ?", (interval_id,)
        ).fetchone()
        conn.close()

        return jsonify({"ok": True, "interval": _format_interval(row)})

    @app.route("/api/activity/<int:activity_id>/splits/<int:interval_id>", methods=["DELETE"])
    def api_delete_split(activity_id, interval_id):
        conn = get_db()
        row = conn.execute(
            "SELECT id FROM intervals WHERE id = ? AND activity_id = ? AND source = 'manual_split'",
            (interval_id, activity_id),
        ).fetchone()
        if not row:
            conn.close()
            return jsonify({"error": "manual_split interval not found"}), 404

        conn.execute("DELETE FROM intervals WHERE id = ?", (interval_id,))
        conn.commit()
        conn.close()
        return jsonify({"ok": True})

    @app.route("/api/activity/<int:activity_id>/override", methods=["POST"])
    def api_override(activity_id):
        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON body required"}), 400
        field = data.get("field_name")
        value = data.get("override_value")
        if not field or value is None:
            return jsonify({"error": "field_name and override_value required"}), 400
        if field not in OVERRIDABLE_FIELDS:
            return jsonify({"error": f"field '{field}' is not overridable"}), 400

        # Empty strides = explicitly zero (user says "no strides on this activity")
        if field == "strides" and str(value).strip() == "":
            value = "0"

        # Nullable numeric fields: empty string means NULL
        NULLABLE_NUMERIC = {"avg_hr", "max_hr", "avg_cadence", "avg_pace_s_per_mi", "duration_s"}
        is_null = field in NULLABLE_NUMERIC and str(value).strip() == ""

        conn = get_db()

        if is_null:
            # Delete the override and set the activity field to NULL
            conn.execute(
                "DELETE FROM activity_overrides WHERE activity_id = ? AND field_name = ?",
                (activity_id, field),
            )
            conn.execute(
                f"UPDATE activities SET {field} = NULL, updated_at = datetime('now') WHERE id = ?",
                (activity_id,),
            )
            if field in ("duration_s", "avg_pace_s_per_mi"):
                conn.execute(
                    "UPDATE activities SET avg_pace_display = NULL WHERE id = ?",
                    (activity_id,),
                )
        else:
            conn.execute(
                """INSERT INTO activity_overrides (activity_id, field_name, override_value)
                   VALUES (?, ?, ?)
                   ON CONFLICT(activity_id, field_name)
                   DO UPDATE SET override_value = excluded.override_value,
                                 created_at = datetime('now')""",
                (activity_id, field, str(value)),
            )
            # Sync overrides to activities table so they become canonical
            # and won't be overridden by future imports
            SYNC_FIELDS = {
                "distance_mi", "duration_s", "avg_pace_s_per_mi",
                "avg_hr", "max_hr", "avg_cadence",
                "workout_name", "workout_category", "shoe_id", "strides", "notes",
            }
            if field in SYNC_FIELDS:
                if field == "distance_mi":
                    dist = float(value)
                    conn.execute(
                        "UPDATE activities SET distance_mi = ?, adjusted_distance_mi = ?, "
                        "updated_at = datetime('now') WHERE id = ?",
                        (dist, dist, activity_id),
                    )
                elif field in ("duration_s", "avg_pace_s_per_mi", "avg_hr", "max_hr", "avg_cadence"):
                    conn.execute(
                        f"UPDATE activities SET {field} = ?, updated_at = datetime('now') WHERE id = ?",
                        (float(value), activity_id),
                    )
                    # Recompute pace display if duration or pace changed
                    if field in ("duration_s", "avg_pace_s_per_mi"):
                        row = conn.execute(
                            "SELECT duration_s, distance_mi, avg_pace_s_per_mi FROM activities WHERE id = ?",
                            (activity_id,),
                        ).fetchone()
                        if row:
                            dur = row["duration_s"]
                            dist = row["distance_mi"]
                            if field == "duration_s" and dur and dist and dist > 0:
                                pace = dur / dist
                                conn.execute(
                                    "UPDATE activities SET avg_pace_s_per_mi = ?, avg_pace_display = ? WHERE id = ?",
                                    (pace, _format_pace(pace), activity_id),
                                )
                            else:
                                pace = row["avg_pace_s_per_mi"]
                                conn.execute(
                                    "UPDATE activities SET avg_pace_display = ? WHERE id = ?",
                                    (_format_pace(pace), activity_id),
                                )
                elif field in ("shoe_id", "strides"):
                    conn.execute(
                        f"UPDATE activities SET {field} = ?, updated_at = datetime('now') WHERE id = ?",
                        (int(value), activity_id),
                    )
                else:
                    conn.execute(
                        f"UPDATE activities SET {field} = ?, updated_at = datetime('now') WHERE id = ?",
                        (value, activity_id),
                    )
        conn.commit()

        # Return the activity date so JS can refresh aggregates
        row = conn.execute("SELECT date FROM activities WHERE id = ?", (activity_id,)).fetchone()
        conn.close()
        return jsonify({
            "ok": True, "activity_id": activity_id, "field": field, "value": value,
            "date": row["date"] if row else None,
        })

    @app.route("/api/interval/<int:interval_id>/walking", methods=["PUT"])
    def api_toggle_walking(interval_id):
        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON body required"}), 400
        is_walking = bool(data.get("is_walking", False))
        conn = get_db()
        # Update the interval
        conn.execute(
            "UPDATE intervals SET is_walking = ? WHERE id = ?",
            (is_walking, interval_id),
        )
        # Recalculate activity's adjusted_distance_mi
        row = conn.execute(
            "SELECT activity_id FROM intervals WHERE id = ?", (interval_id,)
        ).fetchone()
        if row:
            aid = row["activity_id"]
            # Sum non-walking interval distances, avoiding double-count
            # when both NULL-source (FIT) and strava_lap exist
            has_both = conn.execute(
                """SELECT COUNT(DISTINCT CASE WHEN source IS NULL THEN 1 END) as has_null,
                          COUNT(DISTINCT CASE WHEN source = 'strava_lap' THEN 1 END) as has_strava
                   FROM intervals WHERE activity_id = ?
                     AND (source IS NULL OR source = 'strava_lap')""",
                (aid,),
            ).fetchone()
            if has_both["has_null"] and has_both["has_strava"]:
                source_filter = "AND source = 'strava_lap'"
            else:
                source_filter = "AND (source IS NULL OR source != 'pace_segment')"
            total = conn.execute(
                f"""SELECT COALESCE(SUM(
                       COALESCE(canonical_distance_mi, gps_measured_distance_mi, prescribed_distance_mi, 0)
                   ), 0) as total
                   FROM intervals
                   WHERE activity_id = ? AND NOT is_walking
                     {source_filter}""",
                (aid,),
            ).fetchone()["total"]
            conn.execute(
                "UPDATE activities SET adjusted_distance_mi = ? WHERE id = ?",
                (total if total > 0 else None, aid),
            )
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "interval_id": interval_id, "is_walking": is_walking})

    @app.route("/api/interval/<int:interval_id>/flag", methods=["PUT"])
    def api_toggle_interval_flag(interval_id):
        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON body required"}), 400
        field = data.get("field")
        if field not in ("is_stride", "is_recovery"):
            return jsonify({"error": "field must be is_stride or is_recovery"}), 400
        value = bool(data.get("value", False))
        conn = get_db()
        conn.execute(
            f"UPDATE intervals SET {field} = ?, source = 'manual' WHERE id = ?",
            (value, interval_id),
        )

        # Recount strides for the activity (using visible-source logic)
        activity_id = conn.execute(
            "SELECT activity_id FROM intervals WHERE id = ?", (interval_id,)
        ).fetchone()[0]
        new_strides = _count_visible_strides(conn, activity_id)
        conn.execute(
            "UPDATE activities SET strides = ? WHERE id = ?",
            (new_strides, activity_id),
        )

        conn.commit()
        conn.close()
        return jsonify({
            "ok": True, "interval_id": interval_id, field: value,
            "activity_id": activity_id, "strides": new_strides,
        })

    # ── Interval editing ──────────────────────────────────────────

    INTERVAL_EDITABLE = {"distance", "duration_s", "avg_hr", "pace_zone"}

    @app.route("/api/interval/<int:interval_id>/edit", methods=["PUT"])
    def api_edit_interval(interval_id):
        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON body required"}), 400

        field = data.get("field")
        value = data.get("value")
        if field not in INTERVAL_EDITABLE:
            return jsonify({"error": f"field '{field}' is not editable"}), 400

        conn = get_db()
        row = conn.execute(
            "SELECT * FROM intervals WHERE id = ?", (interval_id,)
        ).fetchone()
        if not row:
            conn.close()
            return jsonify({"error": "interval not found"}), 404

        iv = dict(row)

        if field == "distance":
            # Value is in miles
            dist = float(value)
            conn.execute(
                "UPDATE intervals SET canonical_distance_mi = ?, source = 'manual' WHERE id = ?",
                (dist, interval_id),
            )
            # Recalculate pace if duration exists
            dur = iv.get("duration_s")
            if dur and dist > 0:
                pace = dur / dist
                conn.execute(
                    "UPDATE intervals SET avg_pace_s_per_mi = ?, avg_pace_display = ? WHERE id = ?",
                    (pace, _format_pace(pace), interval_id),
                )

        elif field == "duration_s":
            dur = float(value)
            conn.execute(
                "UPDATE intervals SET duration_s = ?, source = 'manual' WHERE id = ?",
                (dur, interval_id),
            )
            # Recalculate pace if distance exists
            dist = iv.get("canonical_distance_mi") or iv.get("gps_measured_distance_mi") or iv.get("prescribed_distance_mi")
            if dist and dist > 0:
                pace = dur / dist
                conn.execute(
                    "UPDATE intervals SET avg_pace_s_per_mi = ?, avg_pace_display = ? WHERE id = ?",
                    (pace, _format_pace(pace), interval_id),
                )

        elif field == "avg_hr":
            hr = float(value) if value else None
            conn.execute(
                "UPDATE intervals SET avg_hr = ?, source = 'manual' WHERE id = ?",
                (hr, interval_id),
            )

        elif field == "pace_zone":
            zone = value if value else None
            conn.execute(
                "UPDATE intervals SET pace_zone = ?, source = 'manual' WHERE id = ?",
                (zone, interval_id),
            )

        conn.commit()

        # Fetch updated interval for response
        updated = conn.execute(
            "SELECT * FROM intervals WHERE id = ?", (interval_id,)
        ).fetchone()
        conn.close()

        result = _format_interval(updated)
        return jsonify({"ok": True, "interval": result})

    # ── New activity creation ──────────────────────────────────────

    @app.route("/api/activity", methods=["POST"])
    def api_create_activity():
        data = request.get_json()
        if not data or not data.get("date"):
            return jsonify({"error": "date required"}), 400
        activity_date = data["date"]
        # Validate date format
        try:
            datetime.strptime(activity_date, "%Y-%m-%d")
        except ValueError:
            return jsonify({"error": "invalid date format, use YYYY-MM-DD"}), 400

        conn = get_db()
        cursor = conn.execute(
            "INSERT INTO activities (date, updated_at) VALUES (?, datetime('now'))",
            (activity_date,),
        )
        activity_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "activity_id": activity_id})

    # ── Planned activities ─────────────────────────────────────────

    @app.route("/api/planned/<date_str>", methods=["POST"])
    def api_save_planned(date_str):
        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON body required"}), 400
        distance_mi = data.get("distance_mi")
        workout_name = data.get("workout_name", "")
        workout_type_zone = data.get("workout_type_zone") or None
        strides = data.get("strides")
        if distance_mi is not None:
            try:
                distance_mi = float(distance_mi)
            except (ValueError, TypeError):
                distance_mi = None
        if strides is not None:
            try:
                strides = int(strides)
            except (ValueError, TypeError):
                strides = None
        conn = get_db()
        conn.execute(
            """INSERT INTO planned_activities (date, distance_mi, workout_name, workout_type_zone, strides)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(date) DO UPDATE SET
                   distance_mi = excluded.distance_mi,
                   workout_name = excluded.workout_name,
                   workout_type_zone = excluded.workout_type_zone,
                   strides = excluded.strides""",
            (date_str, distance_mi, workout_name, workout_type_zone, strides),
        )
        conn.commit()

        # Compute estimated I-score
        estimated_iscore = None
        if distance_mi:
            from runbase.analysis.vdot import get_current_vdot
            vdot = get_current_vdot(conn, date_str)
            e_pace = _get_avg_e_pace(conn, date_str, vdot)
            estimated_iscore = _estimate_planned_iscore(
                distance_mi, workout_name, workout_type_zone, strides, vdot, e_pace,
            )

        conn.close()
        return jsonify({
            "ok": True, "date": date_str, "distance_mi": distance_mi,
            "workout_name": workout_name, "workout_type_zone": workout_type_zone,
            "strides": strides, "estimated_iscore": estimated_iscore,
        })

    @app.route("/api/planned/<date_str>", methods=["DELETE"])
    def api_delete_planned(date_str):
        conn = get_db()
        conn.execute("DELETE FROM planned_activities WHERE date = ?", (date_str,))
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "date": date_str})

    @app.route("/api/activity/<int:activity_id>", methods=["DELETE"])
    def api_delete_activity(activity_id):
        conn = get_db()
        # Verify it exists
        row = conn.execute("SELECT id FROM activities WHERE id = ?", (activity_id,)).fetchone()
        if not row:
            conn.close()
            return jsonify({"error": "not found"}), 404

        # Collect source identifiers before deleting, then suppress them
        strava_sources = conn.execute(
            "SELECT source_id FROM activity_sources WHERE activity_id = ? AND source = 'strava' AND source_id IS NOT NULL",
            (activity_id,),
        ).fetchall()
        garmin_sources = conn.execute(
            "SELECT source_id FROM activity_sources WHERE activity_id = ? AND source = 'garmin' AND source_id IS NOT NULL",
            (activity_id,),
        ).fetchall()
        fit_sources = conn.execute(
            "SELECT raw_file_path FROM activity_sources WHERE activity_id = ? AND source = 'healthfit' AND raw_file_path IS NOT NULL",
            (activity_id,),
        ).fetchall()
        pf_rows = conn.execute(
            "SELECT file_hash FROM processed_files WHERE activity_id = ? AND file_hash IS NOT NULL",
            (activity_id,),
        ).fetchall()

        for r in strava_sources:
            conn.execute(
                "INSERT OR IGNORE INTO suppressed_sources (source, source_identifier, reason) VALUES ('strava', ?, 'user_delete')",
                (r["source_id"],),
            )
        for r in garmin_sources:
            conn.execute(
                "INSERT OR IGNORE INTO suppressed_sources (source, source_identifier, reason) VALUES ('garmin', ?, 'user_delete')",
                (r["source_id"],),
            )
        for r in fit_sources:
            conn.execute(
                "INSERT OR IGNORE INTO suppressed_sources (source, source_identifier, reason) VALUES ('healthfit', ?, 'user_delete')",
                (r["raw_file_path"],),
            )
        for r in pf_rows:
            conn.execute(
                "INSERT OR IGNORE INTO suppressed_sources (source, source_identifier, reason) VALUES ('healthfit_hash', ?, 'user_delete')",
                (r["file_hash"],),
            )

        # Clean up matching orphaned Strava/Garmin sources
        for r in strava_sources:
            conn.execute(
                "DELETE FROM activity_sources WHERE source = 'strava' AND source_id = ? AND activity_id IS NULL",
                (r["source_id"],),
            )
        for r in garmin_sources:
            conn.execute(
                "DELETE FROM activity_sources WHERE source = 'garmin' AND source_id = ? AND activity_id IS NULL",
                (r["source_id"],),
            )

        # Cascade delete — clear all FK references before deleting the activity
        conn.execute("DELETE FROM streams WHERE activity_id = ?", (activity_id,))
        conn.execute("DELETE FROM intervals WHERE activity_id = ?", (activity_id,))
        conn.execute("DELETE FROM activity_sources WHERE activity_id = ?", (activity_id,))
        conn.execute("DELETE FROM activity_overrides WHERE activity_id = ?", (activity_id,))
        conn.execute("DELETE FROM conflicts WHERE activity_id = ?", (activity_id,))
        conn.execute("DELETE FROM runalyze_metrics WHERE activity_id = ?", (activity_id,))
        conn.execute("DELETE FROM vdot_history WHERE activity_id = ?", (activity_id,))
        conn.execute("DELETE FROM detected_tracks WHERE detected_by_activity_id = ?", (activity_id,))
        conn.execute("UPDATE processed_files SET activity_id = NULL WHERE activity_id = ?", (activity_id,))
        conn.execute("DELETE FROM activities WHERE id = ?", (activity_id,))
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "activity_id": activity_id})

    @app.route("/api/seven_day_ma")
    def api_seven_day_ma():
        """Return 7d trailing mileage for a date range, including planned."""
        start = request.args.get("start")
        end = request.args.get("end")
        if not start or not end:
            return jsonify({"error": "start and end required"}), 400

        # Fetch 7 days before start for trailing window
        window_start = (date.fromisoformat(start) - timedelta(days=7)).isoformat()
        conn = get_db()

        # Real activity distances
        rows = conn.execute(
            """SELECT date, COALESCE(adjusted_distance_mi, distance_mi, 0) as dist
               FROM activities WHERE date BETWEEN ? AND ?""",
            (window_start, end),
        ).fetchall()
        daily = {}
        for r in rows:
            daily[r["date"]] = daily.get(r["date"], 0) + r["dist"]

        # Planned distances (only for dates without real activities)
        planned = conn.execute(
            "SELECT date, distance_mi FROM planned_activities WHERE date BETWEEN ? AND ?",
            (window_start, end),
        ).fetchall()
        act_dates = set(daily.keys())
        for r in planned:
            if r["date"] not in act_dates and r["distance_mi"]:
                daily[r["date"]] = daily.get(r["date"], 0) + r["distance_mi"]
        conn.close()

        # Compute 7d MA for each day in requested range
        result = {}
        d = date.fromisoformat(start)
        end_d = date.fromisoformat(end)
        while d <= end_d:
            total = 0.0
            for offset in range(7):
                dd = (d - timedelta(days=offset)).isoformat()
                total += daily.get(dd, 0)
            result[d.isoformat()] = round(total, 1)
            d += timedelta(days=1)

        return jsonify(result)

    @app.route("/api/training_load")
    def api_training_load():
        """Return 7d mileage, 7dI, ATL, CTL for dates from `from` to end of year.

        Used for live updates after planned activity changes.
        """
        from_str = request.args.get("from")
        if not from_str:
            return jsonify({"error": "from required"}), 400

        from_d = date.fromisoformat(from_str)
        year_end = date(from_d.year, 12, 31)
        # Seed 90 days before year start for ATL/CTL EMA seeding
        seed_start = date(from_d.year, 1, 1) - timedelta(days=90)
        conn = get_db()

        # ── Daily distances (activities + planned) ──
        dist_rows = conn.execute(
            """SELECT date, COALESCE(adjusted_distance_mi, distance_mi, 0) as dist
               FROM activities WHERE date BETWEEN ? AND ?""",
            (seed_start.isoformat(), year_end.isoformat()),
        ).fetchall()
        daily_dist = {}
        act_dates = set()
        for r in dist_rows:
            daily_dist[r["date"]] = daily_dist.get(r["date"], 0) + r["dist"]
            act_dates.add(r["date"])

        planned_rows = conn.execute(
            "SELECT date, distance_mi, workout_name, workout_type_zone, strides "
            "FROM planned_activities WHERE date BETWEEN ? AND ?",
            (seed_start.isoformat(), year_end.isoformat()),
        ).fetchall()
        for r in planned_rows:
            if r["date"] not in act_dates and r["distance_mi"]:
                daily_dist[r["date"]] = daily_dist.get(r["date"], 0) + r["distance_mi"]

        # ── Daily I-scores (activities + planned estimates) ──
        act_rows = conn.execute(
            "SELECT id, date FROM activities WHERE date BETWEEN ? AND ?",
            (seed_start.isoformat(), year_end.isoformat()),
        ).fetchall()
        act_ids = [r["id"] for r in act_rows]
        iscores = _compute_intensity_scores(conn, act_ids) if act_ids else {}

        daily_iscore = {}
        for r in act_rows:
            daily_iscore[r["date"]] = daily_iscore.get(r["date"], 0) + (iscores.get(r["id"], 0) or 0)

        # Add estimated I-scores from planned activities
        _vdot_cache = {}
        _epace_cache = {}
        for r in planned_rows:
            pd = r["date"]
            if pd in act_dates or not r["distance_mi"]:
                continue
            if pd not in _vdot_cache:
                from runbase.analysis.vdot import get_current_vdot
                _vdot_cache[pd] = get_current_vdot(conn, pd)
            if pd not in _epace_cache:
                _epace_cache[pd] = _get_avg_e_pace(conn, pd, _vdot_cache[pd])
            est = _estimate_planned_iscore(
                r["distance_mi"], r["workout_name"],
                r["workout_type_zone"], r["strides"],
                _vdot_cache[pd], _epace_cache[pd],
            )
            if est:
                daily_iscore[pd] = daily_iscore.get(pd, 0) + est

        conn.close()

        # ── Compute ATL/CTL EMAs ──
        lambda_a = 2.0 / (7 + 1)
        lambda_f = 2.0 / (42 + 1)
        atl = 0.0
        ctl = 0.0
        atl_ctl_map = {}
        d_cursor = seed_start
        while d_cursor <= year_end:
            trimp = daily_iscore.get(d_cursor.isoformat(), 0)
            atl = trimp * lambda_a + (1 - lambda_a) * atl
            ctl = trimp * lambda_f + (1 - lambda_f) * ctl
            if d_cursor >= from_d:
                atl_ctl_map[d_cursor.isoformat()] = (round(atl, 1), round(ctl, 1))
            d_cursor += timedelta(days=1)

        # ── Build result: 7d, 7dI, ATL, CTL for each day ──
        result = {}
        d_cursor = from_d
        while d_cursor <= year_end:
            ds = d_cursor.isoformat()
            # 7-day trailing mileage
            seven_d = 0.0
            for offset in range(7):
                dd = (d_cursor - timedelta(days=offset)).isoformat()
                seven_d += daily_dist.get(dd, 0)
            # 7-day trailing intensity
            seven_di = 0.0
            for offset in range(7):
                dd = (d_cursor - timedelta(days=offset)).isoformat()
                seven_di += daily_iscore.get(dd, 0)
            day_atl, day_ctl = atl_ctl_map.get(ds, (0, 0))
            result[ds] = {
                "seven_d": round(seven_d, 1),
                "seven_di": round(seven_di, 1),
                "atl": day_atl,
                "ctl": day_ctl,
            }
            d_cursor += timedelta(days=1)

        return jsonify(result)

    @app.route("/api/footer_stats")
    def api_footer_stats():
        """Return yearly + monthly stats for live footer updates."""
        year = request.args.get("year", type=int, default=date.today().year)
        conn = get_db()
        today_d = date.today()

        rows = conn.execute(
            """SELECT date,
                      COALESCE(adjusted_distance_mi, distance_mi, 0) as dist,
                      COALESCE(duration_s, 0) as dur,
                      duration_s as raw_dur,
                      avg_pace_s_per_mi as pace
               FROM activities
               WHERE date BETWEEN ? AND ?""",
            (f"{year}-01-01", f"{year}-12-31"),
        ).fetchall()
        conn.close()

        # Monthly buckets
        # pace_duration uses per-activity pace × distance (running time only,
        # excludes walking) so stride workouts don't inflate monthly averages.
        monthly = {m: {"distance": 0.0, "duration": 0.0, "pace_distance": 0.0, "pace_duration": 0.0, "count": 0, "longest": 0.0} for m in range(1, 13)}
        for r in rows:
            m = int(r["date"][5:7])
            monthly[m]["distance"] += r["dist"]
            monthly[m]["duration"] += r["dur"]
            if r["raw_dur"] and r["pace"] and r["dist"]:
                monthly[m]["pace_distance"] += r["dist"]
                monthly[m]["pace_duration"] += r["pace"] * r["dist"]
            monthly[m]["count"] += 1
            if r["dist"] > monthly[m]["longest"]:
                monthly[m]["longest"] = r["dist"]

        yearly_distance = sum(monthly[m]["distance"] for m in range(1, 13))
        yearly_duration = sum(monthly[m]["duration"] for m in range(1, 13))
        yearly_pace_distance = sum(monthly[m]["pace_distance"] for m in range(1, 13))
        yearly_pace_duration = sum(monthly[m]["pace_duration"] for m in range(1, 13))
        yearly_count = sum(monthly[m]["count"] for m in range(1, 13))
        longest_run = max(monthly[m]["longest"] for m in range(1, 13))
        yearly_avg_pace = (yearly_pace_duration / yearly_pace_distance) if yearly_pace_distance > 0 else None

        stats = []
        for m in range(1, 13):
            s = monthly[m]
            days_in_m = monthrange(year, m)[1]
            if year == today_d.year and m == today_d.month:
                elapsed_days = today_d.day
            elif year < today_d.year or (year == today_d.year and m < today_d.month):
                elapsed_days = days_in_m
            else:
                elapsed_days = 0
            elapsed_weeks = elapsed_days / 7.0 if elapsed_days > 0 else 0
            avg_weekly = (s["distance"] / elapsed_weeks) if elapsed_weeks > 0 else 0
            avg_pace = (s["pace_duration"] / s["pace_distance"]) if s["pace_distance"] > 0 else None

            stats.append({
                "month": m,
                "name": MONTH_NAMES[m][:3],
                "distance": round(s["distance"], 1),
                "count": s["count"],
                "display_distance": f"{s['distance']:.1f}",
                "display_duration": _format_duration(s["duration"]),
                "display_pace": _format_pace(avg_pace),
                "avg_weekly": f"{avg_weekly:.1f}",
            })

        max_month_dist = max((s["distance"] for s in stats), default=1) or 1

        return jsonify({
            "monthly": stats,
            "max_month_dist": max_month_dist,
            "yearly_distance": round(yearly_distance, 1),
            "yearly_count": yearly_count,
            "yearly_duration": _format_duration(yearly_duration),
            "yearly_avg_pace": _format_pace(yearly_avg_pace),
            "longest_run": round(longest_run, 1),
        })

    # ── Import pipeline ─────────────────────────────────────────────

    _import_lock = threading.Lock()
    _import_status = {"running": False, "output": "", "success": None}

    @app.route("/api/import", methods=["POST"])
    def api_import():
        if _import_status["running"]:
            return jsonify({"ok": False, "error": "Import already running"})
        _import_status["running"] = True
        _import_status["output"] = ""
        _import_status["success"] = None

        def run_pipeline():
            try:
                import sys
                result = subprocess.run(
                    [sys.executable, "-m", "runbase", "pipeline", "-v"],
                    capture_output=True, text=True, timeout=300,
                )
                _import_status["output"] = result.stdout + result.stderr
                _import_status["success"] = result.returncode == 0
            except Exception as e:
                _import_status["output"] = str(e)
                _import_status["success"] = False
            finally:
                _import_status["running"] = False

        threading.Thread(target=run_pipeline, daemon=True).start()
        return jsonify({"ok": True, "message": "Import started"})

    @app.route("/api/import/status")
    def api_import_status():
        return jsonify({
            "running": _import_status["running"],
            "success": _import_status["success"],
            "output": _import_status["output"],
        })

    @app.route("/api/activity/<int:activity_id>/override/<field>", methods=["DELETE"])
    def api_delete_override(activity_id, field):
        conn = get_db()
        conn.execute(
            "DELETE FROM activity_overrides WHERE activity_id = ? AND field_name = ?",
            (activity_id, field),
        )
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "activity_id": activity_id, "field": field})

    @app.route("/api/enrich", methods=["POST"])
    def api_enrich():
        from runbase.analysis.interval_enricher import enrich_activity

        data = request.get_json()
        if not data or not data.get("activity_ids"):
            return jsonify({"error": "activity_ids required"}), 400

        activity_ids = data["activity_ids"]
        cfg = app.config["RUNBASE"]
        conn = get_db()
        results = []
        for aid in activity_ids:
            try:
                enrich_activity(conn, aid, cfg, verbose=False)
                results.append({"id": aid, "ok": True})
            except Exception as e:
                results.append({"id": aid, "ok": False, "error": str(e)})
        conn.close()
        return jsonify({"ok": True, "results": results})

    @app.route("/api/activity/<int:activity_id>/vdot", methods=["POST"])
    def api_set_vdot(activity_id):
        from runbase.analysis.vdot import race_to_vdot, set_vdot
        from runbase.analysis.interval_enricher import enrich_activity

        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON body required"}), 400

        apply_timeline = data.get("apply_timeline", False)
        tag_race = data.get("tag_race", False)
        conn = get_db()

        # Get the activity date
        row = conn.execute(
            "SELECT date FROM activities WHERE id = ?", (activity_id,)
        ).fetchone()
        if not row:
            conn.close()
            return jsonify({"error": "activity not found"}), 404
        activity_date = row["date"]

        # Determine VDOT: explicit vdot (adjusted) takes priority,
        # then computed from race distance + time
        vdot = None
        race_distance_m = data.get("race_distance_m")
        race_time_s = data.get("race_time_s")

        # Validate race distance/time if provided (used for notes even if vdot is explicit)
        if race_distance_m is not None and race_time_s is not None:
            try:
                race_distance_m = float(race_distance_m)
                race_time_s = float(race_time_s)
            except (ValueError, TypeError):
                conn.close()
                return jsonify({"error": "invalid race_distance_m or race_time_s"}), 400
            if race_distance_m <= 0 or race_time_s <= 0:
                conn.close()
                return jsonify({"error": "race distance and time must be positive"}), 400

        # Explicit vdot (adjusted by user) takes priority over computed
        if "vdot" in data and data["vdot"] is not None:
            try:
                vdot = float(data["vdot"])
            except (ValueError, TypeError):
                conn.close()
                return jsonify({"error": "invalid vdot value"}), 400
        elif race_distance_m is not None and race_time_s is not None:
            vdot = race_to_vdot(race_distance_m, race_time_s)
        else:
            conn.close()
            return jsonify({"error": "vdot or race_distance_m+race_time_s required"}), 400

        # Tag as race if requested (does NOT change distance/duration)
        if tag_race:
            conn.execute(
                """UPDATE activities SET workout_category = 'race', workout_type = 'race',
                          updated_at = datetime('now')
                   WHERE id = ?""",
                (activity_id,),
            )

        if not apply_timeline:
            # Just update this single activity's vdot
            conn.execute(
                "UPDATE activities SET vdot = ?, updated_at = datetime('now') WHERE id = ?",
                (vdot, activity_id),
            )
            conn.commit()
            conn.close()
            return jsonify({"ok": True, "vdot": round(vdot, 2), "affected_count": 1})

        # Build notes for the vdot_history entry
        notes = None
        if race_distance_m and race_time_s:
            if race_distance_m >= 1609.344:
                dist_label = f"{race_distance_m / 1609.344:.2f}mi"
            else:
                dist_label = f"{race_distance_m:.0f}m"
            mins = int(race_time_s // 60)
            secs = int(race_time_s % 60)
            notes = f"{dist_label} in {mins}:{secs:02d}"

        # Insert/update vdot_history entry
        source = "race" if tag_race else "manual"
        existing = conn.execute(
            "SELECT id FROM vdot_history WHERE activity_id = ?",
            (activity_id,),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE vdot_history SET vdot = ?, effective_date = ?, source = ?, notes = ? WHERE id = ?",
                (vdot, activity_date, source, notes, existing["id"]),
            )
        else:
            conn.execute(
                """INSERT INTO vdot_history (effective_date, vdot, source, activity_id, notes)
                   VALUES (?, ?, ?, ?, ?)""",
                (activity_date, vdot, source, activity_id, notes),
            )
        conn.commit()

        # Find the date range to re-enrich: from this date until the next vdot_history entry
        next_entry = conn.execute(
            "SELECT effective_date FROM vdot_history WHERE effective_date > ? ORDER BY effective_date LIMIT 1",
            (activity_date,),
        ).fetchone()
        end_date = next_entry["effective_date"] if next_entry else "9999-12-31"

        # Re-enrich affected activities
        affected_rows = conn.execute(
            "SELECT id FROM activities WHERE date >= ? AND date < ? ORDER BY date",
            (activity_date, end_date),
        ).fetchall()

        affected_count = 0
        cfg = app.config["RUNBASE"]
        for r in affected_rows:
            try:
                enrich_activity(conn, r["id"], cfg, verbose=False)
                affected_count += 1
            except Exception:
                pass

        conn.close()
        return jsonify({"ok": True, "vdot": round(vdot, 2), "affected_count": affected_count})

    # ── Shoes API ─────────────────────────────────────────────────

    @app.route("/api/shoes")
    def api_shoes_summary():
        conn = get_db()
        rows = conn.execute(
            """SELECT s.id, s.name, s.brand, s.model, s.purchase_date, s.retired,
                      COUNT(a.id) as activity_count,
                      COALESCE(SUM(COALESCE(a.adjusted_distance_mi, a.distance_mi, 0)), 0) as total_miles,
                      MIN(a.date) as first_use,
                      MAX(a.date) as last_use,
                      CASE WHEN SUM(CASE WHEN a.duration_s > 0 THEN COALESCE(a.adjusted_distance_mi, a.distance_mi, 0) ELSE 0 END) > 0
                           THEN SUM(CASE WHEN a.duration_s > 0 THEN a.duration_s ELSE 0 END)
                                / SUM(CASE WHEN a.duration_s > 0 THEN COALESCE(a.adjusted_distance_mi, a.distance_mi, 0) ELSE 0 END)
                           ELSE NULL END as avg_pace
               FROM shoes s
               LEFT JOIN activities a ON a.shoe_id = s.id
               GROUP BY s.id
               ORDER BY s.retired ASC, last_use DESC""",
        ).fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])

    @app.route("/api/shoes/monthly")
    def api_shoes_monthly():
        conn = get_db()
        rows = conn.execute(
            """SELECT a.shoe_id,
                      s.name as shoe_name,
                      substr(a.date, 1, 7) as month,
                      SUM(COALESCE(a.adjusted_distance_mi, a.distance_mi, 0)) as miles
               FROM activities a
               JOIN shoes s ON a.shoe_id = s.id
               WHERE a.shoe_id IS NOT NULL
               GROUP BY a.shoe_id, substr(a.date, 1, 7)
               ORDER BY month, a.shoe_id""",
        ).fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])

    # ── Trends API ───────────────────────────────────────────────

    @app.route("/api/trends")
    def api_trends():
        range_str = request.args.get("range", "1y")
        today_d = date.today()

        range_map = {"6m": 180, "1y": 365, "2y": 730}
        if range_str == "all":
            range_start = date(2000, 1, 1)
        else:
            days = range_map.get(range_str, 365)
            range_start = today_d - timedelta(days=days)

        conn = get_db()

        # ── Weekly aggregates (Sun-Sat) ────────────
        rows = conn.execute(
            """SELECT date,
                      COALESCE(adjusted_distance_mi, distance_mi, 0) as dist,
                      COALESCE(duration_s, 0) as dur,
                      avg_hr
               FROM activities
               WHERE date >= ?
               ORDER BY date""",
            (range_start.isoformat(),),
        ).fetchall()

        week_buckets = {}
        for r in rows:
            dt = datetime.strptime(r["date"], "%Y-%m-%d")
            days_since_sunday = (dt.weekday() + 1) % 7
            sunday = (dt - timedelta(days=days_since_sunday)).date()
            key = sunday.isoformat()
            if key not in week_buckets:
                week_buckets[key] = {"distance": 0.0, "duration": 0.0, "hr_sum": 0.0, "hr_dist": 0.0}
            dist = r["dist"]
            dur = r["dur"]
            week_buckets[key]["distance"] += dist
            week_buckets[key]["duration"] += dur
            if r["avg_hr"] and dist > 0:
                week_buckets[key]["hr_sum"] += r["avg_hr"] * dist
                week_buckets[key]["hr_dist"] += dist

        weekly = []
        for week_key in sorted(week_buckets.keys()):
            b = week_buckets[week_key]
            avg_pace = (b["duration"] / b["distance"]) if b["distance"] > 0 else None
            avg_hr = (b["hr_sum"] / b["hr_dist"]) if b["hr_dist"] > 0 else None
            weekly.append({
                "week": week_key,
                "distance": round(b["distance"], 1),
                "avg_pace": round(avg_pace, 1) if avg_pace else None,
                "avg_hr": round(avg_hr, 1) if avg_hr else None,
            })

        # ── ATL / CTL / TSB ────────────
        # Seed 90 days before range start
        seed_start = (range_start - timedelta(days=90)).isoformat()
        iscore_rows = conn.execute(
            """SELECT id, date FROM activities WHERE date >= ? ORDER BY date""",
            (seed_start,),
        ).fetchall()
        iscore_ids = [r["id"] for r in iscore_rows]
        iscores = _compute_intensity_scores(conn, iscore_ids) if iscore_ids else {}

        daily_iscore = {}
        for r in iscore_rows:
            daily_iscore[r["date"]] = daily_iscore.get(r["date"], 0) + (iscores.get(r["id"], 0) or 0)

        lambda_a = 2.0 / (7 + 1)
        lambda_f = 2.0 / (42 + 1)
        atl = 0.0
        ctl = 0.0
        atl_ctl = []
        d_cursor = date.fromisoformat(seed_start)
        while d_cursor <= today_d:
            trimp = daily_iscore.get(d_cursor.isoformat(), 0)
            atl = trimp * lambda_a + (1 - lambda_a) * atl
            ctl = trimp * lambda_f + (1 - lambda_f) * ctl
            if d_cursor >= range_start:
                atl_ctl.append({
                    "date": d_cursor.isoformat(),
                    "atl": round(atl, 1),
                    "ctl": round(ctl, 1),
                    "tsb": round(ctl - atl, 1),
                })
            d_cursor += timedelta(days=1)

        # Downsample ATL/CTL to ~200 points for performance
        if len(atl_ctl) > 200:
            step = len(atl_ctl) / 200
            downsampled = []
            i = 0.0
            while int(i) < len(atl_ctl):
                downsampled.append(atl_ctl[int(i)])
                i += step
            atl_ctl = downsampled

        # ── Races ────────────
        race_rows = conn.execute(
            """SELECT id, date, workout_name,
                      COALESCE(adjusted_distance_mi, distance_mi, 0) as distance_mi,
                      duration_s, avg_pace_s_per_mi, vdot
               FROM activities
               WHERE date >= ?
                 AND (workout_category = 'race' OR workout_type = 'race')
               ORDER BY date""",
            (range_start.isoformat(),),
        ).fetchall()
        races = [dict(r) for r in race_rows]

        # ── VDOT timeline ────────────
        vdot_rows = conn.execute(
            """SELECT effective_date, vdot, source, notes
               FROM vdot_history
               WHERE effective_date >= ?
               ORDER BY effective_date""",
            (range_start.isoformat(),),
        ).fetchall()
        vdot_timeline = [dict(r) for r in vdot_rows]

        # ── Daily predicted race VDOT ────────────
        cvd_seed = (range_start - timedelta(days=AVD_WINDOW_DAYS)).isoformat()
        cvd_rows = conn.execute(
            "SELECT date, computed_vdot, duration_s FROM activities "
            "WHERE computed_vdot IS NOT NULL AND NOT COALESCE(exclude_from_vdot, 0) AND date >= ? ORDER BY date",
            (cvd_seed,),
        ).fetchall()
        mult = _avd_multiplier(conn)
        conn.close()

        cvd_list = [(r[0], r[1], r[2] or 0) for r in cvd_rows]
        predicted_vdot = []
        d_cursor = range_start
        while d_cursor <= today_d:
            ds = d_cursor.isoformat()
            ewa, _ = _avd_ewa(ds, cvd_list, inclusive=False)
            if ewa is not None:
                predicted_vdot.append({
                    "date": ds,
                    "value": round(ewa * mult, 1),
                })
            d_cursor += timedelta(days=1)

        # Downsample to ~300 points
        if len(predicted_vdot) > 300:
            step = len(predicted_vdot) / 300
            ds = []
            i = 0.0
            while int(i) < len(predicted_vdot):
                ds.append(predicted_vdot[int(i)])
                i += step
            predicted_vdot = ds

        return jsonify({
            "weekly": weekly,
            "atl_ctl": atl_ctl,
            "races": races,
            "vdot_timeline": vdot_timeline,
            "predicted_vdot": predicted_vdot,
        })

    @app.route("/api/race-predictions")
    def api_race_predictions():
        conn = get_db()

        # Load races from vdot_history
        races = conn.execute(
            """SELECT vh.effective_date, vh.vdot, vh.activity_id, a.workout_name
               FROM vdot_history vh
               LEFT JOIN activities a ON vh.activity_id = a.id
               WHERE vh.source = 'race'
               ORDER BY vh.effective_date""",
        ).fetchall()

        # Load all activities with computed_vdot
        cvd_rows = conn.execute(
            "SELECT date, computed_vdot, duration_s FROM activities "
            "WHERE computed_vdot IS NOT NULL AND NOT COALESCE(exclude_from_vdot, 0) ORDER BY date",
        ).fetchall()

        cvd_data = [(r[0], r[1], r[2] or 0) for r in cvd_rows]
        best_mult = _avd_multiplier(conn)
        conn.close()

        # Build results
        results = []
        for r in races:
            race_date, race_vdot, activity_id, workout_name = r[0], r[1], r[2], r[3]
            ewa, cnt = _avd_ewa(race_date, cvd_data, inclusive=False)
            if ewa is None:
                results.append({
                    "date": race_date,
                    "name": workout_name or "",
                    "race_vdot": race_vdot,
                    "ewa": None,
                    "predicted": None,
                    "error": None,
                    "prior_count": 0,
                })
                continue
            predicted = round(ewa * best_mult, 2)
            error = round(predicted - race_vdot, 2)
            results.append({
                "date": race_date,
                "name": workout_name or "",
                "race_vdot": race_vdot,
                "ewa": round(ewa, 2),
                "predicted": predicted,
                "error": error,
                "prior_count": cnt,
            })

        valid = [r for r in results if r["error"] is not None]
        rmse = math.sqrt(sum(r["error"] ** 2 for r in valid) / len(valid)) if valid else None
        mae = sum(abs(r["error"]) for r in valid) / len(valid) if valid else None

        return jsonify({
            "window_days": AVD_WINDOW_DAYS,
            "half_life": AVD_HALF_LIFE,
            "multiplier": round(best_mult, 4),
            "rmse": round(rmse, 2) if rmse else None,
            "mae": round(mae, 2) if mae else None,
            "races": results,
        })

    # ── Weather-Adjusted CVD Model ────────────────────────────

    def _wet_bulb_f(temp_f, humidity_pct):
        """Wet-bulb temperature (Stull 2011 approximation). Input/output in °F."""
        tc = (temp_f - 32) * 5 / 9
        rh = humidity_pct
        wb_c = (
            tc * math.atan(0.151977 * (rh + 8.313659) ** 0.5)
            + math.atan(tc + rh)
            - math.atan(rh - 1.676331)
            + 0.00391838 * rh ** 1.5 * math.atan(0.023101 * rh)
            - 4.686035
        )
        return wb_c * 9 / 5 + 32

    def _fit_weather_model(conn):
        """Build training data, fit RandomForest, return results dict."""
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

        rows = conn.execute(
            """SELECT date, computed_vdot, duration_s,
                      COALESCE(adjusted_distance_mi, distance_mi) as dist_mi,
                      temperature_f, humidity_pct, weather_conditions
               FROM activities
               WHERE computed_vdot IS NOT NULL
                 AND NOT COALESCE(exclude_from_vdot, 0)
                 AND temperature_f IS NOT NULL
                 AND humidity_pct IS NOT NULL
                 AND COALESCE(adjusted_distance_mi, distance_mi) >= 3.0
               ORDER BY date""",
        ).fetchall()

        if len(rows) < 20:
            return None

        # Build feature rows with 7d avg CVD lookback
        features = []
        for i, r in enumerate(rows):
            target_date = date.fromisoformat(r["date"])
            # Compute 7d duration-weighted avg CVD from prior activities
            tw = 0.0
            twv = 0.0
            count = 0
            for j in range(i - 1, -1, -1):
                d = date.fromisoformat(rows[j]["date"])
                delta = (target_date - d).days
                if delta > 7:
                    break
                if delta < 1:
                    continue
                dur = rows[j]["duration_s"] or 60
                tw += dur
                twv += dur * rows[j]["computed_vdot"]
                count += 1
            if count < 2:
                continue
            avg_7d_cvd = twv / tw

            temp_f = r["temperature_f"]
            hum = r["humidity_pct"]
            wb = _wet_bulb_f(temp_f, hum)
            cond = (r["weather_conditions"] or "").lower()
            is_precip = 1 if any(w in cond for w in ("rain", "drizzle", "snow", "thunderstorm")) else 0

            features.append({
                "date": r["date"],
                "actual_cvd": r["computed_vdot"],
                "temperature_f": temp_f,
                "humidity_pct": hum,
                "is_precip": is_precip,
                "avg_7d_cvd": round(avg_7d_cvd, 2),
                "wet_bulb_f": round(wb, 1),
                "distance_mi": round(r["dist_mi"], 2) if r["dist_mi"] else 0,
            })

        if len(features) < 20:
            return None

        import numpy as np
        feature_names = ["temperature_f", "humidity_pct", "is_precip",
                         "avg_7d_cvd", "wet_bulb_f", "distance_mi"]
        display_names = ["Temperature (°F)", "Humidity (%)", "Precipitation",
                         "7d Avg CVD", "Wet Bulb (°F)", "Distance (mi)"]
        X = np.array([[f[k] for k in feature_names] for f in features])
        y = np.array([f["actual_cvd"] for f in features])

        # Train/test split (80/20)
        n = len(features)
        np.random.seed(42)
        indices = np.random.permutation(n)
        split = int(n * 0.8)
        train_idx, test_idx = indices[:split], indices[split:]

        X_train, X_test = X[train_idx], X[test_idx]
        y_train, y_test = y[train_idx], y[test_idx]

        model = RandomForestRegressor(
            n_estimators=100, max_depth=4, random_state=42,
        )
        model.fit(X_train, y_train)

        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)

        train_metrics = {
            "r2": round(r2_score(y_train, y_train_pred), 4),
            "rmse": round(mean_squared_error(y_train, y_train_pred) ** 0.5, 2),
            "mae": round(mean_absolute_error(y_train, y_train_pred), 2),
        }
        test_metrics = {
            "r2": round(r2_score(y_test, y_test_pred), 4),
            "rmse": round(mean_squared_error(y_test, y_test_pred) ** 0.5, 2),
            "mae": round(mean_absolute_error(y_test, y_test_pred), 2),
        }

        importances = model.feature_importances_
        fi = [{"name": display_names[i], "importance": round(float(importances[i]), 4)}
              for i in range(len(feature_names))]
        fi.sort(key=lambda x: x["importance"], reverse=True)

        # Build per-activity results
        all_pred = model.predict(X)
        train_set = set(train_idx.tolist())
        activities = []
        for i, f in enumerate(features):
            activities.append({
                "date": f["date"],
                "actual_cvd": round(f["actual_cvd"], 2),
                "predicted_cvd": round(float(all_pred[i]), 2),
                "residual": round(float(all_pred[i]) - f["actual_cvd"], 2),
                "split": "train" if i in train_set else "test",
                "temperature_f": f["temperature_f"],
                "humidity_pct": f["humidity_pct"],
                "wet_bulb_f": f["wet_bulb_f"],
                "avg_7d_cvd": f["avg_7d_cvd"],
                "distance_mi": f["distance_mi"],
                "is_precip": f["is_precip"],
            })

        return {
            "n_train": int(len(train_idx)),
            "n_test": int(len(test_idx)),
            "train": train_metrics,
            "test": test_metrics,
            "feature_importances": fi,
            "activities": activities,
        }

    @app.route("/api/weather-regression")
    def api_weather_regression():
        conn = get_db()
        result = _fit_weather_model(conn)
        conn.close()
        if result is None:
            return jsonify({"error": "Not enough data"}), 400
        return jsonify(result)

    # ── Planning Tab ────────────────────────────────────────────

    @app.route("/api/planning")
    def api_planning():
        year = request.args.get("year", type=int, default=date.today().year)
        conn = get_db()
        today = date.today()

        # Compute first Sunday on or before Jan 1 and last Saturday on or after Dec 31
        jan1 = date(year, 1, 1)
        days_since_sun = (jan1.weekday() + 1) % 7
        first_sunday = jan1 - timedelta(days=days_since_sun)

        dec31 = date(year, 12, 31)
        days_until_sat = (5 - dec31.weekday()) % 7
        last_saturday = dec31 + timedelta(days=days_until_sat)

        range_start = first_sunday.isoformat()
        range_end = last_saturday.isoformat()

        # Fetch real activities in range
        act_rows = conn.execute(
            """SELECT id, date,
                      COALESCE(adjusted_distance_mi, distance_mi, 0) as dist,
                      duration_s
               FROM activities
               WHERE date BETWEEN ? AND ?""",
            (range_start, range_end),
        ).fetchall()

        # Build daily distance + collect IDs per date
        daily_dist = {}
        daily_ids = {}
        for r in act_rows:
            daily_dist[r["date"]] = daily_dist.get(r["date"], 0) + r["dist"]
            daily_ids.setdefault(r["date"], []).append(r["id"])

        # Fetch planned activities
        planned_rows = conn.execute(
            "SELECT date, distance_mi, workout_name, workout_type_zone, strides "
            "FROM planned_activities WHERE date BETWEEN ? AND ?",
            (range_start, range_end),
        ).fetchall()
        planned_map = {r["date"]: dict(r) for r in planned_rows}

        # Compute intensity scores for real activities
        all_ids = [r["id"] for r in act_rows]
        intensity_scores = _compute_intensity_scores(conn, all_ids) if all_ids else {}

        # Daily intensity sums
        daily_intensity = {}
        for r in act_rows:
            iscore = intensity_scores.get(r["id"], 0) or 0
            daily_intensity[r["date"]] = daily_intensity.get(r["date"], 0) + iscore

        # Add planned activity distances and estimated I-scores for dates without real activities
        from runbase.analysis.vdot import get_current_vdot
        _vdot_cache = {}
        _epace_cache = {}
        for p_date, p_data in planned_map.items():
            if p_date not in daily_ids and p_data.get("distance_mi"):
                daily_dist[p_date] = daily_dist.get(p_date, 0) + p_data["distance_mi"]
                if p_date not in _vdot_cache:
                    _vdot_cache[p_date] = get_current_vdot(conn, p_date)
                if p_date not in _epace_cache:
                    _epace_cache[p_date] = _get_avg_e_pace(conn, p_date, _vdot_cache[p_date])
                est = _estimate_planned_iscore(
                    p_data["distance_mi"],
                    p_data.get("workout_name"),
                    p_data.get("workout_type_zone"),
                    p_data.get("strides"),
                    _vdot_cache[p_date],
                    _epace_cache[p_date],
                )
                if est:
                    daily_intensity[p_date] = daily_intensity.get(p_date, 0) + est

        # Bucket into Sun-Sat weeks
        weeks = []
        sunday = first_sunday
        while sunday <= last_saturday:
            saturday = sunday + timedelta(days=6)
            total_mi = 0.0
            total_i = 0.0
            run_count = 0
            for offset in range(7):
                d = sunday + timedelta(days=offset)
                ds = d.isoformat()
                total_mi += daily_dist.get(ds, 0)
                total_i += daily_intensity.get(ds, 0)
                if ds in daily_ids:
                    run_count += len(daily_ids[ds])
                elif ds in planned_map and planned_map[ds].get("distance_mi"):
                    run_count += 1

            weeks.append({
                "week_start": sunday.isoformat(),
                "week_end": saturday.isoformat(),
                "label": f"{sunday.strftime('%-m/%-d')} \u2013 {saturday.strftime('%-m/%-d')}",
                "month": sunday.month,
                "total_mileage": round(total_mi, 1),
                "total_intensity": round(total_i, 1),
                "run_count": run_count,
                "phase": None,
                "note": None,
                "target_mileage": None,
                "target_intensity": None,
                "is_future": saturday >= today,
            })
            sunday += timedelta(days=7)

        # Fetch weekly_plan rows and merge
        plan_rows = conn.execute(
            "SELECT week_start, phase, note, target_mileage, target_intensity FROM weekly_plan "
            "WHERE week_start BETWEEN ? AND ?",
            (range_start, range_end),
        ).fetchall()
        plan_map = {r["week_start"]: dict(r) for r in plan_rows}
        for w in weeks:
            plan = plan_map.get(w["week_start"])
            if plan:
                w["phase"] = plan.get("phase")
                w["note"] = plan.get("note")
                w["target_mileage"] = plan.get("target_mileage")
                w["target_intensity"] = plan.get("target_intensity")

        conn.close()
        return jsonify({"weeks": weeks, "year": year})

    @app.route("/api/planning/<week_start>", methods=["POST"])
    def api_planning_save(week_start):
        # Validate week_start is a Sunday
        try:
            ws = date.fromisoformat(week_start)
        except ValueError:
            return jsonify({"ok": False, "error": "Invalid date"}), 400
        if (ws.weekday() + 1) % 7 != 0:
            return jsonify({"ok": False, "error": "week_start must be a Sunday"}), 400

        data = request.get_json(force=True)
        phase = data.get("phase") or None
        note = data.get("note") or None
        target_mileage = None
        if "target_mileage" in data and data["target_mileage"] is not None:
            try:
                target_mileage = float(data["target_mileage"])
            except (ValueError, TypeError):
                target_mileage = None
        target_intensity = None
        if "target_intensity" in data and data["target_intensity"] is not None:
            try:
                target_intensity = float(data["target_intensity"])
            except (ValueError, TypeError):
                target_intensity = None

        conn = get_db()
        conn.execute(
            """INSERT INTO weekly_plan (week_start, phase, note, target_mileage, target_intensity)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(week_start) DO UPDATE SET
                   phase=excluded.phase, note=excluded.note,
                   target_mileage=excluded.target_mileage,
                   target_intensity=excluded.target_intensity""",
            (week_start, phase, note, target_mileage, target_intensity),
        )
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "week_start": week_start,
                        "phase": phase, "note": note,
                        "target_mileage": target_mileage,
                        "target_intensity": target_intensity})

    return app
