"""Import running activities from training_log.xlsx into RunBase DB."""

import json
import re
from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path

import openpyxl

from runbase.db import get_connection
from runbase.ingest.fit_parser import format_pace, _compute_file_hash

METERS_PER_MILE = 1609.344


# Column index mapping (0-based)
COL_DATE = 0
COL_INTENSITY = 3
COL_CARDIO = 7
COL_SHOE_ID = 8
COL_CARDIO_NOTE = 9
COL_WORKOUT_TITLE = 10
COL_RUN_TIME = 23
COL_NOTE = 24

# Non-running keywords for text cardio filtering
NON_RUNNING_KEYWORDS = re.compile(
    r'\b(hike|hiking|walk|walking|swim|swimming|bike|biking|cycling|'
    r'off|rest|yoga|stretch|strength|weights|elliptical|rowing|cross[\s-]?train)\b',
    re.IGNORECASE,
)


@dataclass
class NoteParseResult:
    avg_pace_s_per_mi: float | None
    avg_hr: float | None
    avg_cadence: float | None
    free_text: str | None
    splits_s: list[float] | None = None


@dataclass
class ParsedRow:
    row_number: int
    date: str
    distance_mi: float | None
    duration_s: float | None
    avg_pace_s_per_mi: float | None
    avg_pace_display: str | None
    avg_hr: float | None
    avg_cadence: float | None
    intensity_score: float | None
    shoe_id: int | None
    workout_name: str | None
    notes: str | None
    raw_note: str | None
    raw_cardio: str | None
    parse_method: str
    pace_source: str | None
    strides: int | None = None
    workout_category: str | None = None
    splits_s: list[float] | None = None


def import_xlsx(config: dict, dry_run: bool = False, verbose: bool = False) -> dict:
    """Import activities from training_log.xlsx.

    Returns dict with keys: new, skipped, errors, skipped_non_running, parse_stats.
    """
    xlsx_path = config["paths"]["xlsx_import"]
    xlsx_path = str(Path(xlsx_path).expanduser())

    if not Path(xlsx_path).exists():
        raise FileNotFoundError(f"XLSX file not found: {xlsx_path}")

    conn = get_connection(config)

    # Check if already processed
    file_hash = _compute_file_hash(xlsx_path)
    already = conn.execute(
        "SELECT id FROM processed_files WHERE file_hash = ? AND source = 'master_xlsx'",
        (file_hash,),
    ).fetchone()
    if already:
        if verbose:
            print(f"XLSX already imported (hash match). Skipping.")
        conn.close()
        return {"new": 0, "skipped": 0, "errors": 0, "skipped_non_running": 0,
                "parse_stats": {}, "already_imported": True}

    raw_rows = _read_xlsx(xlsx_path)
    if verbose:
        print(f"Read {len(raw_rows)} data rows from {xlsx_path}")

    parsed_rows, skipped_non_running, parse_stats = _parse_rows(raw_rows, verbose)
    if verbose:
        print(f"Parsed {len(parsed_rows)} running rows, skipped {skipped_non_running} non-running")
        print(f"Parse methods: {parse_stats}")

    result = {"new": 0, "skipped": 0, "errors": 0,
              "skipped_non_running": skipped_non_running, "parse_stats": parse_stats}

    for row in parsed_rows:
        try:
            activity_id = _insert_row(conn, row, xlsx_path, dry_run, verbose)
            if dry_run or activity_id is not None:
                result["new"] += 1
            else:
                result["skipped"] += 1
        except Exception as e:
            result["errors"] += 1
            if verbose:
                print(f"  ERROR row {row.row_number}: {e}")

    # Record processed file after all rows
    if not dry_run and result["new"] > 0:
        conn.execute(
            "INSERT INTO processed_files (file_path, file_hash, source) VALUES (?, ?, ?)",
            (xlsx_path, file_hash, "master_xlsx"),
        )
        conn.commit()

    conn.close()
    return result


def _read_xlsx(path: str) -> list[dict]:
    """Read XLSX, return list of raw row dicts with values by column index."""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows = []

    for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        # Must have a date and non-empty cardio
        if not row or len(row) <= COL_CARDIO:
            continue
        date_val = row[COL_DATE] if len(row) > COL_DATE else None
        cardio_val = row[COL_CARDIO] if len(row) > COL_CARDIO else None

        if date_val is None or cardio_val is None:
            continue
        if isinstance(cardio_val, str) and not cardio_val.strip():
            continue

        rows.append({
            "row_number": row_idx,
            "date": date_val,
            "intensity": row[COL_INTENSITY] if len(row) > COL_INTENSITY else None,
            "cardio": cardio_val,
            "shoe_id": row[COL_SHOE_ID] if len(row) > COL_SHOE_ID else None,
            "cardio_note": row[COL_CARDIO_NOTE] if len(row) > COL_CARDIO_NOTE else None,
            "workout_title": row[COL_WORKOUT_TITLE] if len(row) > COL_WORKOUT_TITLE else None,
            "run_time": row[COL_RUN_TIME] if len(row) > COL_RUN_TIME else None,
            "note": row[COL_NOTE] if len(row) > COL_NOTE else None,
        })

    wb.close()
    return rows


def _classify_row(raw: dict) -> str | None:
    """Classify a row as 'numeric', 'text', or None (skip).

    Numeric cardio = running distance. Text cardio checked against
    non-running keywords.
    """
    cardio = raw["cardio"]

    if isinstance(cardio, (int, float)):
        return "numeric"

    if isinstance(cardio, str):
        cardio_stripped = cardio.strip()
        # Try to parse as number
        try:
            float(cardio_stripped)
            return "numeric"
        except ValueError:
            pass

        # Check for non-running
        if NON_RUNNING_KEYWORDS.search(cardio_stripped):
            return None

        # Has text — could be a text running entry
        return "text"

    return None


def _parse_rows(raw_rows: list[dict], verbose: bool) -> tuple[list[ParsedRow], int, dict]:
    """Classify and parse all rows.

    Returns (parsed_rows, skipped_non_running_count, parse_stats).
    """
    parsed = []
    skipped_non_running = 0
    parse_stats = {"numeric": 0, "text_with_time": 0, "text_distance_only": 0, "text_skipped": 0}

    for raw in raw_rows:
        row_type = _classify_row(raw)

        if row_type is None:
            skipped_non_running += 1
            continue

        if row_type == "numeric":
            row = _parse_numeric_row(raw)
            parse_stats["numeric"] += 1
            parsed.append(row)

        elif row_type == "text":
            row = _parse_text_row(raw)
            if row is not None:
                if row.parse_method == "text_with_time":
                    parse_stats["text_with_time"] += 1
                else:
                    parse_stats["text_distance_only"] += 1
                parsed.append(row)
            else:
                parse_stats["text_skipped"] += 1
                skipped_non_running += 1

    return parsed, skipped_non_running, parse_stats


def _parse_strides(cardio_note: str | None, workout_title: str | None) -> int | None:
    """Extract strides count from cardio_note (col 9) or workout_title (col 10).

    Matches patterns like "4 strides", "strides(6)", "6strides", "6.5 strides".
    Returns rounded integer count, or None if no count found.
    """
    for text in (cardio_note, workout_title):
        if not text:
            continue
        m = re.search(r'(\d+\.?\d*)\s*strides', str(text), re.IGNORECASE)
        if m:
            return round(float(m.group(1)))
        # Also match "strides(N)" and "strides (N)"
        m = re.search(r'strides\s*\(\s*(\d+\.?\d*)\s*\)', str(text), re.IGNORECASE)
        if m:
            return round(float(m.group(1)))
    return None


def _parse_workout_category(cardio_note: str | None, workout_title: str | None) -> str | None:
    """Classify workout category from cardio_note (col 9) and workout_title (col 10).

    Returns one of: tempo, interval, repetition, fartlek, hills, race, long, easy, strides.
    Returns None only if no classification can be made (caller should default to 'easy').
    """
    cn = str(cardio_note).strip() if cardio_note else ""
    wt = str(workout_title).strip() if workout_title else ""
    cn_lower = cn.lower()
    wt_lower = wt.lower()

    # Skip lift-only entries
    if cn_lower == "lift":
        return None

    # Race detection — check both fields
    race_patterns = (
        r'\b\d+k\s+race\b', r'\b\d+\s*mile\s+race\b', r'\bmile\s+TT\b',
        r'\b\d+\s*TT\b', r'\bhalf\s+race\b', r'\bfull\s+race\b',
        r'\brace\b', r'\bgoal\s+mile\b', r'\beaster\s+mile\b',
    )
    for pat in race_patterns:
        if re.search(pat, cn, re.IGNORECASE) or re.search(pat, wt, re.IGNORECASE):
            return "race"

    # Speed workout types from cardio_note
    if re.search(r'\bspeed\s+T\b|^ST$|\bspeed\s+T/R\b', cn, re.IGNORECASE):
        return "tempo"
    if re.search(r'\bspeed\s+I\b', cn, re.IGNORECASE):
        return "interval"
    if re.search(r'\bspeed\s+R\b|\bspeed\s+R/I\b', cn, re.IGNORECASE):
        return "repetition"
    if re.search(r'\bspeed\s+F\b', cn, re.IGNORECASE):
        return "fartlek"

    # Hills
    if re.search(r'\bhills?\b', cn_lower):
        return "hills"

    # Long run — check workout_title
    if re.search(r'\blong\b', wt_lower):
        return "long"

    # Strides-only (no speed/race keywords above matched)
    if re.search(r'\bstrides?\b', cn_lower) or re.search(r'\bstrides?\b', wt_lower):
        return "easy"

    # Pre-race / shake out
    if re.search(r'\bshake\s*out\b|\bpre[\s-]?race\b', cn_lower):
        return "easy"

    # If cardio_note is empty, default easy
    if not cn:
        return "easy"

    return None


def _parse_numeric_row(raw: dict) -> ParsedRow:
    """Parse a row with numeric cardio (distance in miles)."""
    date_str = _normalize_date(raw["date"])

    cardio = raw["cardio"]
    if isinstance(cardio, str):
        distance_mi = float(cardio.strip())
    else:
        distance_mi = float(cardio)
    distance_mi = round(distance_mi, 2)

    # Duration from col 23
    duration_s = _time_to_seconds(raw.get("run_time"))

    # Note parsing
    raw_note = raw.get("note")
    note_str = str(raw_note).strip() if raw_note is not None else None
    if note_str == "" or note_str == "None":
        note_str = None

    note_result = _parse_note(note_str) if note_str else NoteParseResult(None, None, None, None)

    # Pace resolution
    # When splits are present, they're interval paces (working reps only),
    # NOT the overall run pace. Overall pace/HR are unknown for these rows.
    avg_pace = None
    avg_pace_display = None
    pace_source = None

    if note_result.splits_s:
        pace_source = "splits_only"
        # avg_pace stays None — splits are interval data, not whole-run pace
    elif note_result.avg_pace_s_per_mi is not None:
        avg_pace = note_result.avg_pace_s_per_mi
        pace_source = "note_parsed"
    elif distance_mi and distance_mi > 0 and duration_s and duration_s > 0:
        avg_pace = round(duration_s / distance_mi, 1)
        pace_source = "computed"

    if avg_pace is not None:
        avg_pace_display = format_pace(avg_pace)

    # Workout name: col 10 only (col 9 is category/strides metadata)
    workout_name = None
    wt = raw.get("workout_title")
    if wt and str(wt).strip():
        workout_name = str(wt).strip()

    # Strides and workout category
    cardio_note = raw.get("cardio_note")
    cn_str = str(cardio_note).strip() if cardio_note else None
    if cn_str == "" or cn_str == "None":
        cn_str = None

    strides = _parse_strides(cn_str, workout_name)
    workout_category = _parse_workout_category(cn_str, workout_name)
    if workout_category is None:
        workout_category = "easy"

    # Intensity score
    intensity = raw.get("intensity")
    intensity_score = None
    if intensity is not None:
        try:
            intensity_score = round(float(intensity), 2)
        except (ValueError, TypeError):
            pass

    # Shoe ID
    shoe_id = None
    shoe_raw = raw.get("shoe_id")
    if shoe_raw is not None:
        try:
            shoe_id = int(shoe_raw)
        except (ValueError, TypeError):
            pass

    return ParsedRow(
        row_number=raw["row_number"],
        date=date_str,
        distance_mi=distance_mi,
        duration_s=duration_s,
        avg_pace_s_per_mi=avg_pace,
        avg_pace_display=avg_pace_display,
        avg_hr=note_result.avg_hr,
        avg_cadence=note_result.avg_cadence,
        intensity_score=intensity_score,
        shoe_id=shoe_id,
        workout_name=workout_name,
        notes=note_result.free_text,
        raw_note=note_str,
        raw_cardio=str(raw["cardio"]) if raw["cardio"] is not None else None,
        parse_method="numeric",
        pace_source=pace_source,
        strides=strides,
        workout_category=workout_category,
        splits_s=note_result.splits_s,
    )


def _parse_text_row(raw: dict) -> ParsedRow | None:
    """Parse a row with text cardio (e.g. '3 miles in 26:30')."""
    cardio_text = str(raw["cardio"]).strip()
    date_str = _normalize_date(raw["date"])

    distance_mi = None
    duration_s = None
    avg_pace = None
    avg_pace_display = None
    pace_source = None
    parse_method = "text_distance_only"

    # Try "X miles in HH:MM:SS" pattern
    m = re.search(r'(\d+\.?\d*)\s*miles?\s+in\s+(\d+:\d+(?::\d+)?)', cardio_text, re.IGNORECASE)
    if m:
        distance_mi = round(float(m.group(1)), 2)
        duration_s = _time_str_to_seconds(m.group(2))
        parse_method = "text_with_time"

        # Check for parenthetical pace
        pace_m = re.search(r'\((\d+:\d+(?:\.\d)?)/mi', cardio_text)
        if pace_m:
            avg_pace = _pace_str_to_seconds(pace_m.group(1))
            pace_source = "text_parsed"
        elif distance_mi and distance_mi > 0 and duration_s and duration_s > 0:
            avg_pace = round(duration_s / distance_mi, 1)
            pace_source = "computed"
    else:
        # Distance-only: "X miles"
        m2 = re.search(r'(\d+\.?\d*)\s*miles?', cardio_text, re.IGNORECASE)
        if m2:
            distance_mi = round(float(m2.group(1)), 2)
        else:
            # Can't parse distance — skip
            return None

    if avg_pace is not None:
        avg_pace_display = format_pace(avg_pace)

    # Intensity score
    intensity = raw.get("intensity")
    intensity_score = None
    if intensity is not None:
        try:
            intensity_score = round(float(intensity), 2)
        except (ValueError, TypeError):
            pass

    # Shoe ID
    shoe_id = None
    shoe_raw = raw.get("shoe_id")
    if shoe_raw is not None:
        try:
            shoe_id = int(shoe_raw)
        except (ValueError, TypeError):
            pass

    # Workout name: col 10 only
    workout_name = None
    wt = raw.get("workout_title")
    if wt and str(wt).strip():
        workout_name = str(wt).strip()

    # Strides and workout category
    cardio_note = raw.get("cardio_note")
    cn_str = str(cardio_note).strip() if cardio_note else None
    if cn_str == "" or cn_str == "None":
        cn_str = None

    strides = _parse_strides(cn_str, workout_name)
    workout_category = _parse_workout_category(cn_str, workout_name)
    if workout_category is None:
        workout_category = "easy"

    return ParsedRow(
        row_number=raw["row_number"],
        date=date_str,
        distance_mi=distance_mi,
        duration_s=duration_s,
        avg_pace_s_per_mi=avg_pace,
        avg_pace_display=avg_pace_display,
        avg_hr=None,
        avg_cadence=None,
        intensity_score=intensity_score,
        shoe_id=shoe_id,
        workout_name=workout_name,
        notes=None,
        raw_note=None,
        raw_cardio=cardio_text,
        parse_method=parse_method,
        pace_source=pace_source,
        strides=strides,
        workout_category=workout_category,
    )


def _parse_note(note: str) -> NoteParseResult:
    """Parse structured data from note field using regex cascade.

    Tried in order, first match wins:
    0. Splits: X:XX-X:XX[-X:XX...][; text] — avg pace from lap splits
    1. Full: pace, HR, cadence[; text]
    2. Pace + HR: pace, HR[; text]
    3. Pace only: pace[; text]
    4. @pattern: ...@pace HR cadence...
    5. Fallback: entire note as free text
    """
    if not note:
        return NoteParseResult(None, None, None, None)

    note = note.strip()

    # 0. Splits at start: X:XX-X:XX[-X:XX...][; text]
    #    Two or more dash-separated times at the beginning of the note
    m = re.match(
        r'^(\d{1,2}:\d{2}(?:\.\d)?(?:\s*-\s*\d{1,2}:\d{2}(?:\.\d)?)+)\s*[;.,]?\s*(.*)$',
        note,
    )
    if m:
        splits_str = m.group(1)
        splits = [_pace_str_to_seconds(s) for s in re.split(r'\s*-\s*', splits_str)]
        avg_pace = round(sum(splits) / len(splits), 1)
        free = m.group(2).strip() or None
        if all(60 <= s <= 900 for s in splits):
            return NoteParseResult(avg_pace, None, None, free, splits_s=splits)

    # 1. Full: pace, HR, cadence[; text]
    m = re.match(
        r'^(\d{1,2}:\d{2}(?:\.\d)?)\s*,\s*(\d{2,3})\s*,\s*(\d{2,3})\b\s*[;.,]?\s*(.*)$',
        note,
    )
    if m:
        pace = _pace_str_to_seconds(m.group(1))
        hr = float(m.group(2))
        cadence = float(m.group(3))
        free = m.group(4).strip() or None
        if 240 <= pace <= 900 and 80 <= hr <= 220:
            return NoteParseResult(pace, hr, cadence, free)

    # 2. Pace + HR: pace, HR[; text]
    m = re.match(
        r'^(\d{1,2}:\d{2}(?:\.\d)?)\s*,\s*(\d{2,3})\b\s*[;.,]?\s*(.*)$',
        note,
    )
    if m:
        pace = _pace_str_to_seconds(m.group(1))
        hr = float(m.group(2))
        remaining = m.group(3).strip()
        # Reject if remaining starts with 2-3 digit number (would be cadence → should match pattern 1)
        if not re.match(r'^\d{2,3}\b', remaining):
            if 240 <= pace <= 900 and 80 <= hr <= 220:
                free = remaining or None
                return NoteParseResult(pace, hr, None, free)

    # 3. Pace only: pace[; text]
    m = re.match(r'^(\d{1,2}:\d{2}(?:\.\d)?)\s*[;,]?\s*(.*)$', note)
    if m:
        pace = _pace_str_to_seconds(m.group(1))
        free = m.group(2).strip() or None
        if 240 <= pace <= 900:
            return NoteParseResult(pace, None, None, free)

    # Also try space-delimited pace
    m = re.match(r'^(\d{1,2}:\d{2}(?:\.\d)?)\s+(.+)$', note)
    if m:
        pace = _pace_str_to_seconds(m.group(1))
        free = m.group(2).strip() or None
        if 240 <= pace <= 900:
            return NoteParseResult(pace, None, None, free)

    # 4. @pattern: ...@pace HR cadence...
    m = re.search(r'@(\d{1,2}:\d{2}(?:\.\d)?)', note)
    if m:
        pace = _pace_str_to_seconds(m.group(1))
        if 240 <= pace <= 900:
            # Look for HR and cadence after the pace
            after = note[m.end():].strip()
            hr = None
            cadence = None

            nums = re.findall(r'\b(\d{2,3})\b', after)
            for n in nums:
                val = int(n)
                if hr is None and 80 <= val <= 220:
                    hr = float(val)
                elif cadence is None and 140 <= val <= 200:
                    cadence = float(val)

            # Build free text: remove the @pace and any extracted numbers
            free_text = note[:m.start()].strip()
            remaining_after = after
            if hr is not None:
                remaining_after = re.sub(r'\b' + str(int(hr)) + r'\b', '', remaining_after, count=1)
            if cadence is not None:
                remaining_after = re.sub(r'\b' + str(int(cadence)) + r'\b', '', remaining_after, count=1)
            remaining_after = remaining_after.strip(' ,;.')
            if remaining_after:
                free_text = (free_text + " " + remaining_after).strip() if free_text else remaining_after
            free_text = free_text.strip() or None

            return NoteParseResult(pace, hr, cadence, free_text)

    # 5. Fallback: entire note as free text
    return NoteParseResult(None, None, None, note)


def _parse_interval_distance(workout_name: str | None) -> float | None:
    """Parse per-interval distance in miles from workout name.

    Handles patterns like:
      '4x800'          → 800m  = 0.497 mi
      '3x1 mile at T'  → 1 mi
      '4x1200 w 3 min' → 1200m = 0.745 mi
      '5x1k w 400 jg'  → 1000m = 0.621 mi
      '3x1 T w 2 mins' → 1 mi  (bare small number = miles)
      '5k @ t'         → 1 mi  (mile splits of a continuous tempo/race)
    """
    if not workout_name:
        return None

    # NxDIST pattern: e.g. "4x800", "3x1 mile", "5x1k"
    m = re.search(r'(\d+)\s*x\s*(\d+\.?\d*)\s*(mile|mi|k|km|m)?\b', workout_name, re.IGNORECASE)
    if m:
        dist_val = float(m.group(2))
        unit = (m.group(3) or "").lower()

        if unit in ("mile", "mi"):
            return dist_val
        elif unit in ("k", "km"):
            return round(dist_val * 1000 / METERS_PER_MILE, 4)
        elif unit == "m":
            return round(dist_val / METERS_PER_MILE, 4)
        else:
            # No unit: small numbers (≤ 10) are miles, large numbers are meters
            if dist_val <= 10:
                return dist_val
            else:
                return round(dist_val / METERS_PER_MILE, 4)

    # Race/tempo pattern: "5k @ t" — mile splits of a continuous effort
    if re.search(r'\d+k\s*@', workout_name, re.IGNORECASE):
        return 1.0

    return None


def _insert_row(conn, parsed: ParsedRow, xlsx_path: str,
                dry_run: bool, verbose: bool) -> int | None:
    """Insert a parsed row into activities + activity_sources.

    Returns activity_id or None (dry run).
    """
    if dry_run:
        if verbose:
            pace_str = parsed.avg_pace_display or "N/A"
            hr_str = f" HR:{parsed.avg_hr:.0f}" if parsed.avg_hr else ""
            print(f"  DRY   row {parsed.row_number}: {parsed.date} "
                  f"{parsed.distance_mi:.2f}mi @ {pace_str}/mi{hr_str} "
                  f"[{parsed.parse_method}]")
        return None

    cursor = conn.cursor()
    try:
        # 1. Insert activity (shoe_id stored in metadata only — shoes table not populated yet)
        cursor.execute(
            """INSERT INTO activities
               (date, distance_mi, duration_s, avg_pace_s_per_mi, avg_pace_display,
                avg_hr, avg_cadence, workout_type, workout_name,
                strides, workout_category, intensity_score, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (parsed.date, parsed.distance_mi, parsed.duration_s,
             parsed.avg_pace_s_per_mi, parsed.avg_pace_display,
             parsed.avg_hr, parsed.avg_cadence, "running", parsed.workout_name,
             parsed.strides, parsed.workout_category,
             parsed.intensity_score, parsed.notes),
        )
        activity_id = cursor.lastrowid

        # 2. Insert activity source with metadata
        metadata = {
            "row_number": parsed.row_number,
            "parse_method": parsed.parse_method,
            "pace_source": parsed.pace_source,
            "raw_note": parsed.raw_note,
            "raw_cardio": parsed.raw_cardio,
            "xlsx_shoe_id": parsed.shoe_id,
        }
        if parsed.splits_s:
            metadata["splits_s"] = parsed.splits_s
            metadata["splits_display"] = [format_pace(s) for s in parsed.splits_s]

        # Build metrics dict for notes field
        metrics = {}
        if parsed.distance_mi is not None:
            metrics["distance_mi"] = parsed.distance_mi
        if parsed.duration_s is not None:
            metrics["duration_s"] = parsed.duration_s
        if parsed.avg_pace_s_per_mi is not None:
            metrics["avg_pace_s_per_mi"] = parsed.avg_pace_s_per_mi
        if parsed.avg_hr is not None:
            metrics["avg_hr"] = parsed.avg_hr
        if parsed.avg_cadence is not None:
            metrics["avg_cadence"] = parsed.avg_cadence

        cursor.execute(
            """INSERT INTO activity_sources
               (activity_id, source, source_id, raw_file_path,
                distance_mi, duration_s, avg_pace_s_per_mi, avg_hr, avg_cadence,
                workout_name, notes, metadata_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (activity_id, "master_xlsx", f"xlsx_row_{parsed.row_number}",
             xlsx_path,
             parsed.distance_mi, parsed.duration_s, parsed.avg_pace_s_per_mi,
             parsed.avg_hr, parsed.avg_cadence,
             parsed.workout_name, json.dumps(metrics) if metrics else None,
             json.dumps(metadata)),
        )

        # 3. Insert intervals from splits
        if parsed.splits_s:
            interval_dist = _parse_interval_distance(parsed.workout_name)
            for i, split_duration_s in enumerate(parsed.splits_s, start=1):
                # splits_s are durations (time for the rep), not pace/mi
                split_pace = None
                split_pace_display = None
                if interval_dist and interval_dist > 0:
                    split_pace = round(split_duration_s / interval_dist, 1)
                    split_pace_display = format_pace(split_pace)
                cursor.execute(
                    """INSERT INTO intervals
                       (activity_id, rep_number, prescribed_distance_mi,
                        actual_distance_mi, duration_s,
                        avg_pace_s_per_mi, avg_pace_display, is_recovery)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (activity_id, i, interval_dist,
                     interval_dist, split_duration_s,
                     split_pace, split_pace_display, False),
                )

        conn.commit()

        if verbose:
            pace_str = parsed.avg_pace_display or "N/A"
            hr_str = f" HR:{parsed.avg_hr:.0f}" if parsed.avg_hr else ""
            print(f"  NEW   row {parsed.row_number} → activity {activity_id}: "
                  f"{parsed.date} {parsed.distance_mi:.2f}mi @ {pace_str}/mi{hr_str}")

        return activity_id

    except Exception:
        conn.rollback()
        raise


def _normalize_date(val) -> str:
    """Convert date value to YYYY-MM-DD string."""
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    if isinstance(val, str):
        # Try common formats
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(val.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return val.strip()
    return str(val)


def _time_to_seconds(val) -> float | None:
    """Convert a time object or string to total seconds."""
    if val is None:
        return None

    if isinstance(val, time):
        return val.hour * 3600 + val.minute * 60 + val.second

    if isinstance(val, datetime):
        t = val.time()
        return t.hour * 3600 + t.minute * 60 + t.second

    if isinstance(val, str):
        try:
            return _time_str_to_seconds(val)
        except (ValueError, IndexError):
            return None

    return None


def _time_str_to_seconds(s: str) -> float:
    """Convert 'H:MM:SS' or 'MM:SS' string to seconds."""
    parts = s.strip().split(":")
    if len(parts) == 3:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
    elif len(parts) == 2:
        return int(parts[0]) * 60 + float(parts[1])
    else:
        return float(s)


def _pace_str_to_seconds(s: str) -> float:
    """Convert pace string like '7:30' or '7:30.5' to seconds per mile."""
    parts = s.strip().split(":")
    if len(parts) == 2:
        minutes = int(parts[0])
        secs = float(parts[1])
        return minutes * 60 + secs
    return float(s)
