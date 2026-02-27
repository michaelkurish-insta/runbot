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

    def _format_pace(seconds_per_mile):
        if seconds_per_mile is None:
            return ""
        m, s = divmod(int(seconds_per_mile), 60)
        return f"{m}:{s:02d}"

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
        total = int(seconds)
        tenths = round((seconds - total) * 10) % 10
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
        """Get predominant pace zone per activity from qualifying intervals."""
        if not activity_ids:
            return {}
        placeholders = ",".join("?" * len(activity_ids))
        rows = conn.execute(
            f"""SELECT activity_id, pace_zone
                FROM intervals
                WHERE activity_id IN ({placeholders})
                  AND pace_zone IS NOT NULL
                  AND NOT is_recovery
                  AND NOT is_walking
                  AND (source IS NULL OR source != 'pace_segment')""",
            activity_ids,
        ).fetchall()
        # Pick highest-priority zone per activity
        result = {}
        for r in rows:
            aid = r["activity_id"]
            zone = r["pace_zone"]
            prio = ZONE_PRIORITY.get(zone, 0)
            if prio > ZONE_PRIORITY.get(result.get(aid, ""), 0):
                result[aid] = zone
        return result

    def _build_activity(r, overrides_map, shoes, has_streams_set, workout_types):
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
        # Keep primary's pace/HR/cadence/shoe/name/vdot
        merged["has_streams"] = any(a["has_streams"] for a in day_acts)
        merged["overridden_fields"] = list(set().union(*(a["overridden_fields"] for a in day_acts)))

        # Pick highest-priority workout type zone across all activities
        best_zone = ""
        for a in day_acts:
            z = a.get("workout_type_zone", "")
            if ZONE_PRIORITY.get(z, 0) > ZONE_PRIORITY.get(best_zone, 0):
                best_zone = z
        merged["workout_type_zone"] = best_zone

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

        # Fetch prior year's last 6 days for 7d MA at start of year
        prior_start = f"{year - 1}-12-25"
        prior_rows = conn.execute(
            """SELECT date, COALESCE(adjusted_distance_mi, distance_mi, 0) as dist
               FROM activities
               WHERE date BETWEEN ? AND ?""",
            (prior_start, f"{year - 1}-12-31"),
        ).fetchall()
        daily_dist = {}
        for r in prior_rows:
            daily_dist[r["date"]] = daily_dist.get(r["date"], 0) + r["dist"]

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

        # Fetch planned activities for the year (used in blank calendar rows + 7d MA)
        planned_rows = conn.execute(
            "SELECT date, distance_mi, workout_name FROM planned_activities "
            "WHERE date BETWEEN ? AND ?",
            (start, end),
        ).fetchall()
        planned_map = {r["date"]: dict(r) for r in planned_rows}

        # Build activities grouped by date
        activities = []
        months_with_data = set()
        by_date = {}
        for r in rows:
            a = _build_activity(r, overrides_map, shoes, has_streams, workout_types)
            activities.append(a)
            months_with_data.add(a["month_num"])
            by_date.setdefault(a["date"], []).append(a)
            # Accumulate daily distances for 7d MA
            dist = a.get("adjusted_distance_mi") or a.get("distance_mi") or 0
            daily_dist[a["date"]] = daily_dist.get(a["date"], 0) + dist

        # Add planned distances to daily_dist for 7d MA (only for dates without real activities)
        for p_date, p_data in planned_map.items():
            if p_date not in by_date and p_data.get("distance_mi"):
                daily_dist[p_date] = daily_dist.get(p_date, 0) + p_data["distance_mi"]

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

                if day_acts:
                    merged = _merge_day(day_acts)
                    merged["week_of_month"] = wom
                    merged["seven_day_ma"] = ma_display
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
        yearly_count = 0
        longest_run = 0.0
        max_week_dist = 0.0

        for m in range(1, 13):
            month_acts = [day["activity"] for day in month_calendars.get(m, []) if not day["blank"]]
            dist = sum((a.get("adjusted_distance_mi") or a.get("distance_mi") or 0) for a in month_acts)
            dur = sum((a.get("duration_s") or 0) for a in month_acts)
            count = len(month_acts)
            avg_pace = (dur / dist) if dist > 0 else None

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
            yearly_count += count
            for a in month_acts:
                d = a.get("adjusted_distance_mi") or a.get("distance_mi") or 0
                if d > longest_run:
                    longest_run = d

        for w in sorted_weeks:
            if w["distance"] > max_week_dist:
                max_week_dist = w["distance"]

        max_month_dist = max((s["distance"] for s in monthly_stats), default=1) or 1
        yearly_avg_pace = (yearly_duration / yearly_distance) if yearly_distance > 0 else None

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
        iv["display_hr"] = f"{iv['avg_hr']:.0f}" if iv.get("avg_hr") else ""
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

        # Build rep distance summary stats (group by canonical distance)
        summary = _build_rep_summary(intervals + laps)

        conn.close()
        return jsonify({"intervals": intervals, "laps": laps, "summary": summary})

    def _build_rep_summary(all_intervals):
        """Group intervals by rep distance and compute averages."""
        from collections import defaultdict
        buckets = defaultdict(list)
        for iv in all_intervals:
            if iv.get("is_walking") or iv.get("is_recovery"):
                continue
            dist = iv.get("canonical_distance_mi") or iv.get("gps_measured_distance_mi") or iv.get("prescribed_distance_mi")
            if not dist or dist <= 0:
                continue
            # Round to nearest bucket for grouping
            if dist < 1.0:
                key_m = round(dist * 1609.344 / 50) * 50  # nearest 50m
                label = f"{key_m:.0f}m"
            else:
                key_m = round(dist, 2)
                label = f"{key_m:.2f}mi"
            buckets[label].append(iv)

        summary = []
        for label, ivs in buckets.items():
            if len(ivs) < 2:
                continue
            durations = [iv["duration_s"] for iv in ivs if iv.get("duration_s")]
            paces = [iv["avg_pace_s_per_mi"] for iv in ivs if iv.get("avg_pace_s_per_mi")]
            hrs = [iv["avg_hr"] for iv in ivs if iv.get("avg_hr")]
            summary.append({
                "distance": label,
                "count": len(ivs),
                "avg_duration": _format_duration_precise(sum(durations) / len(durations)) if durations else "",
                "avg_pace": _format_pace(sum(paces) / len(paces)) if paces else "",
                "avg_hr": f"{sum(hrs) / len(hrs):.0f}" if hrs else "",
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
        conn.close()
        if not points:
            return jsonify({"pace": [], "hr": []})

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
        conn.close()

        display_dist = a.get("adjusted_distance_mi") or a.get("distance_mi")
        return jsonify({
            "id": a["id"],
            "workout_name": a.get("workout_name") or "",
            "shoe_id": a.get("shoe_id"),
            "shoe_name": a.get("shoe_name") or "",
            "workout_category": a.get("workout_category") or "",
            "distance_mi": a.get("distance_mi"),
            "adjusted_distance_mi": a.get("adjusted_distance_mi"),
            "display_distance": f"{display_dist:.2f}" if display_dist else "",
            "duration_s": a.get("duration_s"),
            "display_duration": _format_duration(a.get("duration_s")),
            "avg_pace_s_per_mi": a.get("avg_pace_s_per_mi"),
            "display_pace": _format_pace(a.get("avg_pace_s_per_mi")),
            "avg_hr": a.get("avg_hr"),
            "display_hr": f"{a['avg_hr']:.0f}" if a.get("avg_hr") else "",
            "max_hr": a.get("max_hr"),
            "avg_cadence": a.get("avg_cadence"),
            "strides": a.get("strides"),
            "notes": a.get("notes") or "",
            "overridden_fields": overridden,
        })

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

    # ── Planned activities ─────────────────────────────────────────

    @app.route("/api/planned/<date_str>", methods=["POST"])
    def api_save_planned(date_str):
        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON body required"}), 400
        distance_mi = data.get("distance_mi")
        workout_name = data.get("workout_name", "")
        if distance_mi is not None:
            try:
                distance_mi = float(distance_mi)
            except (ValueError, TypeError):
                distance_mi = None
        conn = get_db()
        conn.execute(
            """INSERT INTO planned_activities (date, distance_mi, workout_name)
               VALUES (?, ?, ?)
               ON CONFLICT(date) DO UPDATE SET
                   distance_mi = excluded.distance_mi,
                   workout_name = excluded.workout_name""",
            (date_str, distance_mi, workout_name),
        )
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "date": date_str, "distance_mi": distance_mi, "workout_name": workout_name})

    @app.route("/api/planned/<date_str>", methods=["DELETE"])
    def api_delete_planned(date_str):
        conn = get_db()
        conn.execute("DELETE FROM planned_activities WHERE date = ?", (date_str,))
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "date": date_str})

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

    @app.route("/api/footer_stats")
    def api_footer_stats():
        """Return yearly + monthly stats for live footer updates."""
        year = request.args.get("year", type=int, default=date.today().year)
        conn = get_db()
        today_d = date.today()

        rows = conn.execute(
            """SELECT date,
                      COALESCE(adjusted_distance_mi, distance_mi, 0) as dist,
                      COALESCE(duration_s, 0) as dur
               FROM activities
               WHERE date BETWEEN ? AND ?""",
            (f"{year}-01-01", f"{year}-12-31"),
        ).fetchall()
        conn.close()

        # Monthly buckets
        monthly = {m: {"distance": 0.0, "duration": 0.0, "count": 0, "longest": 0.0} for m in range(1, 13)}
        for r in rows:
            m = int(r["date"][5:7])
            monthly[m]["distance"] += r["dist"]
            monthly[m]["duration"] += r["dur"]
            monthly[m]["count"] += 1
            if r["dist"] > monthly[m]["longest"]:
                monthly[m]["longest"] = r["dist"]

        yearly_distance = sum(monthly[m]["distance"] for m in range(1, 13))
        yearly_duration = sum(monthly[m]["duration"] for m in range(1, 13))
        yearly_count = sum(monthly[m]["count"] for m in range(1, 13))
        longest_run = max(monthly[m]["longest"] for m in range(1, 13))
        yearly_avg_pace = (yearly_duration / yearly_distance) if yearly_distance > 0 else None

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
            avg_pace = (s["duration"] / s["distance"]) if s["distance"] > 0 else None

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

    return app
