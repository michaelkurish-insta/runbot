import sqlite3
import tempfile
import unittest
from contextlib import closing, contextmanager
from pathlib import Path

from runbase.analysis.vdot import vdot_to_paces
from runbase.db import init_db
from runbase.review.app import create_app


class PlannedIntensityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / "test.db"
        config = {"paths": {"db": str(self.db)}}
        init_db(config)
        with self.connect() as conn:
            conn.execute("INSERT INTO vdot_history (effective_date,vdot,source) VALUES ('2026-11-01',57.5,'manual')")
            conn.execute("INSERT INTO activities (id,date,distance_mi,duration_s,avg_pace_s_per_mi) VALUES (1,'2026-11-14',10,4800,480)")
            conn.execute("INSERT INTO intervals (activity_id,source,rep_number,pace_zone,duration_s,gps_measured_distance_mi) VALUES (1,'fit_lap',1,'E',4800,10)")
        app = create_app(config)
        app.testing = True
        self.client = app.test_client()

    @contextmanager
    def connect(self):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            yield conn

    def plan(self, name="1x8k", zone="CV", miles=7.5):
        response = self.client.post("/api/planned/2026-11-21", json={
            "distance_mi": miles, "workout_name": name, "workout_type_zone": zone,
        })
        self.assertEqual(response.status_code, 200)
        return response.get_json()["estimated_iscore"]

    def test_cv_pace_is_between_threshold_and_interval_paces(self):
        paces = vdot_to_paces(57.5)
        self.assertGreater(paces["T"], paces["CV"])
        self.assertGreater(paces["CV"], paces["I"])
        self.assertEqual(paces["CV"], 350.7)

    def test_eight_km_cv_plan_uses_cv_time_and_total_planned_mileage(self):
        self.assertEqual(self.plan(), 27.3)
        self.assertEqual(self.plan(miles=7), 26.5)
        # A continuous 8K has no extra recovery miles beyond its own distance.
        self.assertEqual(self.plan(miles=8000 / 1609.344), 23.2)

    def test_explicit_cv_names_work_without_relying_on_row_zone(self):
        for name in ("1x8k @CV", "1x8km @CV", "8k @CV", "8km @CV"):
            with self.subTest(name=name):
                self.assertEqual(self.plan(name, zone="E"), 27.3)
                self.assertEqual(self.plan(name, zone=None), 27.3)

    def test_time_based_cv_and_explicit_recovery_are_recognized(self):
        self.assertEqual(self.plan("1x(30min CV)"), 27.8)
        self.assertEqual(self.plan("3x(5min CV, 2min E)", miles=6), 17.5)

    def test_interval_recovery_does_not_exceed_total_planned_distance(self):
        self.assertEqual(self.plan("1x5mi", zone="I", miles=5), 28.0)
        self.assertEqual(self.plan("10x400m", zone="R", miles=3), 20.2)

    def test_mixed_explicit_zones_use_their_own_paces(self):
        paces = vdot_to_paces(57.5)
        cv_miles, r_miles = 2000 / 1609.344, 800 / 1609.344
        expected = (cv_miles * paces["CV"] * .8 + r_miles * paces["R"] * 1.5
                    + (7.5 - cv_miles - r_miles) * 480 * .2) / 60
        self.assertEqual(self.plan("2x1km @CV + 2x400m @R", zone="E"), round(expected, 1))

    def test_planned_and_completed_equivalent_workouts_score_the_same(self):
        planned = self.plan()
        cv_miles = 8000 / 1609.344
        cv_time = cv_miles * vdot_to_paces(57.5)["CV"]
        easy_time = (7.5 - cv_miles) * 480
        with self.connect() as conn:
            conn.execute("INSERT INTO activities (id,date,distance_mi,duration_s) VALUES (2,'2026-11-21',7.5,?)", (cv_time + easy_time,))
            conn.executemany("INSERT INTO intervals (activity_id,source,rep_number,pace_zone,duration_s,gps_measured_distance_mi) VALUES (2,'fit_lap',?,?,?,?)", [
                (1, "CV", cv_time, cv_miles), (2, "E", easy_time, 7.5 - cv_miles),
            ])
        result = self.client.get("/api/training_load?from=2026-11-21").get_json()
        self.assertEqual(result["2026-11-21"]["seven_di"], planned)


if __name__ == "__main__":
    unittest.main()
