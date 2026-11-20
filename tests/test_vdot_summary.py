import sqlite3
import tempfile
import unittest
from contextlib import closing, contextmanager
from pathlib import Path
from unittest.mock import patch

from flask import template_rendered

from runbase.db import init_db
from runbase.review.app import create_app


class VdotSummaryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / "test.db"
        config = {"paths": {"db": str(self.db)}}
        init_db(config)
        self.app = create_app(config)
        self.app.testing = True
        self.client = self.app.test_client()

    @contextmanager
    def connect(self):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            yield conn

    def add_run(self, aid, cvd, duration=3600, distance=6, day="2026-11-07", **flags):
        values = {"id": aid, "date": day, "computed_vdot": cvd,
                  "distance_mi": distance, "duration_s": duration, **flags}
        with self.connect() as conn:
            conn.execute(
                f"INSERT INTO activities ({','.join(values)}) VALUES ({','.join('?' for _ in values)})",
                list(values.values()),
            )
            conn.execute(
                "INSERT INTO intervals (activity_id, rep_number, source, pace_zone, duration_s, "
                "gps_measured_distance_mi) VALUES (?, 1, 'fit_lap', 'E', ?, ?)",
                (aid, duration, distance),
            )

    def daily_row(self, day="2026-11-07"):
        contexts = []

        def capture(sender, template, context, **extra):
            contexts.append(context)

        with template_rendered.connected_to(capture, self.app):
            response = self.client.get("/?year=2026")
        self.assertEqual(response.status_code, 200)
        return next(item["activity"] for month in contexts[0]["month_calendars"].values()
                    for item in month if not item["blank"] and item["activity"]["date"] == day)

    def flags(self, aid, **flags):
        response = self.client.post(f"/api/activity/{aid}/flags", json=flags)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["ok"])

    def test_excluded_strides_do_not_affect_daily_cvd_or_mileage(self):
        self.add_run(1, 60)
        self.add_run(2, 30, duration=600, distance=1, exclude_from_vdot=1)
        row = self.daily_row()
        self.assertEqual(row["computed_vdot"], 60)
        self.assertEqual(row["adjusted_vdot"], 60)
        self.assertEqual(row["distance_mi"], 7)
        self.assertEqual(row["intensity_score"], 14)

    def test_cvd_weights_only_eligible_activities_by_duration(self):
        self.add_run(1, 60)
        self.add_run(2, 30, duration=600, distance=1)
        self.add_run(3, None, duration=900, distance=1.5)
        self.assertAlmostEqual(self.daily_row()["computed_vdot"], (60 * 3600 + 30 * 600) / 4200)
        # The largest run being excluded must not conceal the remaining CVD.
        self.flags(1, exclude_from_vdot=True)
        self.assertEqual(self.daily_row()["computed_vdot"], 30)
        self.flags(2, exclude_from_vdot=True)
        self.assertIsNone(self.daily_row()["computed_vdot"])

    def test_single_excluded_run_is_blank_and_can_be_reincluded(self):
        self.add_run(1, 60, exclude_from_vdot=1)
        row = self.daily_row()
        self.assertIsNone(row["computed_vdot"])
        self.assertIsNone(row["adjusted_vdot"])
        self.flags(1, exclude_from_vdot=False)
        row = self.daily_row()
        self.assertEqual(row["computed_vdot"], 60)
        self.assertEqual(row["adjusted_vdot"], 60)
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT computed_vdot FROM activities").fetchone()[0], 60)

    def test_next_page_load_recomputes_avd_after_exclusion_without_enrichment(self):
        self.add_run(1, 60, day="2026-11-06")
        self.add_run(2, 40)
        before = self.daily_row()
        self.assertLess(before["adjusted_vdot"], 60)
        with patch("runbase.analysis.interval_enricher.enrich_activity") as enrich:
            self.flags(2, exclude_from_vdot=True)
            after = self.daily_row()
            enrich.assert_not_called()
        self.assertEqual(after["adjusted_vdot"], 60)
        self.assertIsNone(after["computed_vdot"])
        self.flags(2, exclude_from_vdot=False)
        self.assertEqual(self.daily_row()["adjusted_vdot"], before["adjusted_vdot"])

    def test_suppressed_hr_cannot_contribute_a_stale_cvd(self):
        self.add_run(1, 60)
        self.add_run(2, 40, suppress_hr=1)
        row = self.daily_row()
        self.assertEqual(row["computed_vdot"], 60)
        self.assertEqual(row["adjusted_vdot"], 60)

    def test_enrichment_failure_is_not_reported_as_success(self):
        self.add_run(1, 60)
        with patch("runbase.analysis.interval_enricher.enrich_activity", side_effect=RuntimeError("Could not calculate")):
            response = self.client.post("/api/enrich", json={"activity_ids": [1]})
        self.assertEqual(response.status_code, 500)
        self.assertFalse(response.get_json()["ok"])
        self.assertEqual(response.get_json()["results"][0]["error"], "Could not calculate")


if __name__ == "__main__":
    unittest.main()
