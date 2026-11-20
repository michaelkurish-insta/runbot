#!/usr/bin/env python3
"""Audit/recalculate GAP and CVD only, preserving mileage, laps and user edits."""

import argparse
import json
import sqlite3
import statistics
import sys
from contextlib import closing
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from runbase.config import load_config
from runbase.analysis.grade_adjustment import compute_gap
from runbase.analysis.interval_enricher import _load_streams
from runbase.analysis.vdot import effective_vdot_from_gap_and_hr


def recalculate(config, apply=False):
    db = Path(config['paths']['db']).expanduser().resolve()
    with closing(sqlite3.connect(db.as_uri() + '?mode=ro', uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        activities = conn.execute(
            'SELECT * FROM activities WHERE EXISTS (SELECT 1 FROM streams WHERE activity_id=activities.id) ORDER BY date'
        ).fetchall()
        changes = []
        for activity in activities:
            pace = activity['avg_pace_s_per_mi']
            gap = compute_gap(_load_streams(conn, activity['id']))
            cvd = None if activity['suppress_hr'] else effective_vdot_from_gap_and_hr(
                gap, activity['avg_hr'], config.get('athlete', {}).get('hr_max'))
            changes.append(dict(id=activity['id'],date=activity['date'],pace=pace,
                                hr=activity['avg_hr'],suppress_hr=activity['suppress_hr'],
                                old_gap=activity['gap_s_per_mi'],new_gap=gap,
                                old_cvd=activity['computed_vdot'],new_cvd=cvd))
    paired = [r for r in changes if r['old_gap'] is not None and r['new_gap'] is not None]
    print(f"{len(changes)} activities with streams; {sum(r['new_gap'] is not None for r in changes)} have sufficient grade data.")
    if paired:
        print(f"Median GAP change: {statistics.median(r['new_gap']-r['old_gap'] for r in paired):+.1f} seconds/mile.")
        print('Largest GAP changes (seconds/mile):')
        for r in sorted(paired, key=lambda r: abs(r['new_gap']-r['old_gap']), reverse=True)[:10]:
            print(f"  #{r['id']} {r['date']}: {r['old_gap']:.1f} -> {r['new_gap']:.1f}")
    if not apply:
        print('Audit only; no database changes made. Use --apply to back up and save GAP/CVD.')
        return changes
    stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    directory = Path(__file__).resolve().parents[1] / 'data' / 'backups'
    directory.mkdir(parents=True, exist_ok=True)
    backup = directory / f'data-before-gap-recalculation-{stamp}.db'
    audit = directory / f'gap-recalculation-{stamp}.json'
    with closing(sqlite3.connect(db)) as conn:
        with closing(sqlite3.connect(backup)) as destination:
            conn.backup(destination)
        with conn:
            for r in changes:
                current = conn.execute('SELECT avg_pace_s_per_mi,avg_hr,suppress_hr,gap_s_per_mi,computed_vdot FROM activities WHERE id=?',(r['id'],)).fetchone()
                expected = (r['pace'],r['hr'],r['suppress_hr'],r['old_gap'],r['old_cvd'])
                if current != expected:
                    raise RuntimeError(f"Activity #{r['id']} changed during calculation; no updates applied. Retry.")
                conn.execute('UPDATE activities SET gap_s_per_mi=?,computed_vdot=? WHERE id=?',
                             (r['new_gap'],r['new_cvd'],r['id']))
    audit.write_text(json.dumps(changes,indent=2))
    print(f'Saved GAP/CVD. Backup: {backup}\nBefore/after values: {audit}')
    return changes


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Back up database and write calculated GAP/CVD')
    args = parser.parse_args()
    recalculate(load_config(),apply=args.apply)
