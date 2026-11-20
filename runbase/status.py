"""Local readiness and data-completeness checks; never exposes credentials."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path

from runbase.analysis.vdot import effective_vdot_from_gap_and_hr
from runbase.ingest.icloud_sync import _pending_enrichment


def system_status(config: dict) -> dict:
    checks = []

    def add(name, status, message):
        checks.append({"name": name, "status": status, "message": message})

    paths = config.get("paths", {})
    db_path = Path(paths.get("db", "")).expanduser()
    last_sync = {}
    if not paths.get("db") or not db_path.is_file():
        add("Database", "error", f"Database is missing: {db_path}. Restore it before importing.")
    else:
        try:
            with closing(sqlite3.connect(db_path.resolve().as_uri() + "?mode=ro", uri=True)) as conn:
                conn.row_factory = sqlite3.Row
                count, latest = conn.execute("SELECT count(*), max(date) FROM activities").fetchone()
                add("Database", "ok", f"{count:,} activities through {latest or 'none'}; {db_path}")
                last_sync = dict(conn.execute("SELECT source, last_sync_at FROM sync_state"))
                pending = _pending_enrichment(conn)
                pending = sorted(set(pending) | {row[0] for row in conn.execute(
                    "SELECT DISTINCT activity_id FROM activity_sources WHERE activity_id IS NOT NULL "
                    "AND json_extract(metadata_json, '$.enrichment_pending') = 1")})
                add("Enrichment", "warning" if pending else "ok",
                    f"{len(pending)} imported activities need enrichment; click Import to retry."
                    if pending else "No interrupted or updated-source enrichment is pending.")
                recent = conn.execute(
                    "SELECT id, date, vdot, computed_vdot, gap_s_per_mi, avg_hr, suppress_hr, "
                    "temperature_f, humidity_pct, cloud_cover_pct FROM activities "
                    "WHERE date >= date(?, '-30 days')", (latest,)).fetchall()
                eligible_missing, exclusions, unenriched, weather_missing = [], [], [], []
                for activity in recent:
                    label = f"{activity['date']} (#{activity['id']})"
                    if activity['vdot'] is None:
                        unenriched.append(label)
                    if any(activity[k] is None for k in ('temperature_f', 'humidity_pct', 'cloud_cover_pct')):
                        weather_missing.append(label)
                    if activity['computed_vdot'] is not None:
                        continue
                    expected = effective_vdot_from_gap_and_hr(
                        activity['gap_s_per_mi'], activity['avg_hr'], config.get('athlete', {}).get('hr_max'))
                    if activity['suppress_hr']:
                        exclusions.append(f"{label}: heart rate suppressed")
                    elif expected is None:
                        exclusions.append(f"{label}: missing pace/heart rate or heart rate outside the CVD range")
                    else:
                        eligible_missing.append(label)
                add("Recent activity ratings", "warning" if unenriched else "ok",
                    f"{len(unenriched)} of {len(recent)} recent activities lack a VDOT baseline: " + ', '.join(unenriched)
                    if unenriched else f"All {len(recent)} activities in the latest 30 days have an enrichment baseline.")
                add("Recent CVD", "warning" if eligible_missing else "ok",
                    (f"{len(eligible_missing)} eligible activities need CVD: " + ', '.join(eligible_missing)
                     if eligible_missing else "CVD is present for every eligible recent activity.")
                    + (" Expected blanks: " + '; '.join(exclusions) if exclusions else ""))
                add("Recent weather", "warning" if weather_missing else "ok",
                    f"{len(weather_missing)} recent activities have incomplete weather."
                    if weather_missing else "Recent activities have temperature, humidity and cloud cover.")
                unmatched = conn.execute(
                    "SELECT json_extract(metadata_json,'$.start_date') AS date, distance_mi "
                    "FROM activity_sources WHERE source='strava' AND activity_id IS NULL "
                    "AND json_extract(metadata_json,'$.start_date') >= date(?, '-30 days') ORDER BY date", (latest,)).fetchall()
                add("Unmatched Strava runs", "warning" if unmatched else "ok",
                    "These runs need review before adding to the calendar: "
                    + '; '.join(f"{row['date']} ({row['distance_mi'] or 0:.2f} mi)" for row in unmatched)
                    if unmatched else "No unmatched Strava runs in the latest 30 days.")
        except sqlite3.Error as exc:
            add("Database checks", "error", f"Could not inspect database: {exc}")

    cloud = Path(paths.get("icloud_healthfit", "")).expanduser()
    if not paths.get("icloud_healthfit") or not cloud.is_dir():
        add("iCloud HealthFit", "error", f"Folder is missing or inaccessible: {cloud}")
    else:
        try:
            count = sum(1 for p in cloud.rglob('*') if p.suffix.lower() == '.fit')
            add("iCloud HealthFit", "ok" if count else "warning",
                f"{count} FIT files visible; {cloud}. Last successful import: {last_sync.get('icloud', 'never')}.")
        except OSError as exc:
            add("iCloud HealthFit", "error", f"Could not scan folder: {exc}")

    strava = config.get('strava', {})
    token_path = Path(strava.get('token_file', '')).expanduser()
    try:
        tokens = json.loads(token_path.read_text())
        valid = all(tokens.get(k) for k in ('access_token', 'refresh_token', 'expires_at'))
        credentials = all(strava.get(k) and not str(strava[k]).startswith('$') for k in ('client_id', 'client_secret'))
        if not valid or not credentials:
            raise ValueError('Incomplete credentials')
        add("Strava", "ok", "Login tokens and app credentials are available. "
            f"Last successful sync: {last_sync.get('strava', 'never')}. Import checks API access.")
    except (OSError, ValueError, TypeError):
        add("Strava", "warning", "Login is missing or incomplete. Run scripts/setup_strava_auth.py.")

    garmin = config.get('garmin', {}).get('token_store')
    add("Garmin (optional)", "info", "Login tokens are available; API access is checked during import."
        if garmin and Path(garmin).expanduser().exists() else "Not connected; Garmin sync is skipped.")
    for key, label in (('xlsx_import', 'Historical spreadsheet'), ('garmin_export_zip', 'Garmin export')):
        path = paths.get(key)
        if path and not Path(path).expanduser().exists():
            add(label + " (optional)", "info", "File is absent on this Mac. Existing database history remains available.")

    return {"ok": not any(c['status'] == 'error' for c in checks), "checks": checks}
