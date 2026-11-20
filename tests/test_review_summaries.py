import sqlite3
import tempfile
import unittest
from contextlib import closing, contextmanager
from datetime import date, timedelta
from pathlib import Path

from runbase.db import init_db
from runbase.review.app import create_app


class ReviewSummaryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / "test.db"
        config = {"paths": {"db": str(self.db)}}
        init_db(config)
        app = create_app(config)
        app.testing = True
        self.client = app.test_client()
        with self.connect() as conn:
            conn.executemany("INSERT INTO shoes (id, name) VALUES (?, ?)",
                             [(1, "Daily"), (2, "Race"), (3, "Unused")])

    @contextmanager
    def connect(self):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            yield conn

    def add_activity(self, **values):
        fields = {"date": "2026-06-24", "shoe_id": 1, **values}
        with self.connect() as conn:
            return conn.execute(
                f"INSERT INTO activities ({','.join(fields)}) VALUES ({','.join('?' for _ in fields)})",
                list(fields.values()),
            ).lastrowid

    def shoes(self):
        response = self.client.get("/api/shoes")
        self.assertEqual(response.status_code, 200)
        return {row["id"]: row for row in response.get_json()}

    def week(self):
        response = self.client.get("/api/trends?range=all")
        self.assertEqual(response.status_code, 200)
        return next(w for w in response.get_json()["weekly"] if w["week"] == "2026-06-21")

    def test_duration_only_workouts_do_not_distort_june_pace(self):
        self.add_activity(distance_mi=2.02, duration_s=1005.902, avg_pace_s_per_mi=497.9713)
        for duration in (1505.995, 1304.901, 1517.489, 3032.812):
            self.add_activity(duration_s=duration, workout_name="Other")
        self.assertEqual(self.week()["avg_pace"], 498.0)
        self.assertEqual(self.week()["distance"], 2.0)
        shoe = self.shoes()[1]
        self.assertAlmostEqual(shoe["avg_pace"], 497.9713)
        self.assertEqual(shoe["total_miles"], 2.02)
        self.assertEqual(shoe["activity_count"], 5)

    def test_all_trends_start_at_actual_history_and_keep_each_day(self):
        today = date.today()
        first = today - timedelta(days=400)
        self.add_activity(date=first.isoformat(), distance_mi=5)
        history_start = first - timedelta(days=7)
        with self.connect() as conn:
            conn.execute("INSERT INTO vdot_history (effective_date, vdot, source) VALUES (?, 55, 'manual')",
                         (history_start.isoformat(),))
        data = self.client.get("/api/trends?range=all").get_json()
        self.assertEqual(data["date_range"], {"start": history_start.isoformat(), "end": today.isoformat()})
        dates = [row["date"] for row in data["atl_ctl"]]
        self.assertEqual(dates, [(history_start + timedelta(days=n)).isoformat() for n in range(408)])

    def test_trends_share_requested_range_and_do_not_skip_predicted_days(self):
        today = date.today()
        start = today - timedelta(days=365)
        for offset in range(-7, 366, 7):
            self.add_activity(date=(start + timedelta(days=offset)).isoformat(),
                              distance_mi=5, duration_s=2400, computed_vdot=55)
        # Future entries must not stretch historical chart axes past today.
        self.add_activity(date=(today + timedelta(days=30)).isoformat(), distance_mi=100,
                          workout_category="race")
        with self.connect() as conn:
            conn.execute("INSERT INTO vdot_history (effective_date, vdot, source) VALUES (?, 60, 'race')",
                         ((today + timedelta(days=30)).isoformat(),))
        data = self.client.get("/api/trends?range=1y").get_json()
        self.assertEqual(data["date_range"], {"start": start.isoformat(), "end": today.isoformat()})
        dates = [(start + timedelta(days=n)).isoformat() for n in range(366)]
        self.assertEqual([d["date"] for d in data["atl_ctl"]], dates)
        self.assertEqual([d["date"] for d in data["predicted_vdot"]], dates)
        self.assertTrue(all(w["week"] <= today.isoformat() for w in data["weekly"]))
        self.assertEqual(data["races"], [])
        self.assertEqual(data["vdot_timeline"], [])

    def test_empty_all_trends_have_a_bounded_date_range(self):
        data = self.client.get("/api/trends?range=all").get_json()
        self.assertEqual(data["date_range"], {
            "start": (date.today() - timedelta(days=365)).isoformat(), "end": date.today().isoformat(),
        })
        self.assertEqual(data["weekly"], [])

    def test_pace_is_distance_weighted_and_uses_recorded_running_pace(self):
        # Full timer duration includes walking already removed from running pace.
        self.add_activity(distance_mi=2, duration_s=1200, avg_pace_s_per_mi=480)
        self.add_activity(distance_mi=6, duration_s=3240, avg_pace_s_per_mi=540)
        self.assertEqual(self.week()["avg_pace"], 525)
        self.assertEqual(self.shoes()[1]["avg_pace"], 525)

    def test_adjusted_zero_distance_is_not_replaced_with_raw_distance(self):
        self.add_activity(distance_mi=2, duration_s=960, avg_pace_s_per_mi=480)
        self.add_activity(distance_mi=.18, adjusted_distance_mi=0,
                          duration_s=3005.395, avg_pace_s_per_mi=16000)
        self.assertEqual(self.week()["avg_pace"], 480)
        self.assertEqual(self.week()["distance"], 2)
        self.assertEqual(self.shoes()[1]["avg_pace"], 480)
        self.assertEqual(self.shoes()[1]["total_miles"], 2)

    def test_missing_or_invalid_pace_preserves_mileage_without_diluting_average(self):
        self.add_activity(distance_mi=2, avg_pace_s_per_mi=480)
        for pace in (None, 0, -1):
            self.add_activity(distance_mi=1, duration_s=600, avg_pace_s_per_mi=pace)
        self.assertEqual(self.week()["avg_pace"], 480)
        self.assertEqual(self.week()["distance"], 5)
        self.assertEqual(self.shoes()[1]["avg_pace"], 480)

    def test_no_valid_pace_or_cvd_is_blank_instead_of_zero(self):
        self.add_activity(distance_mi=1, duration_s=600)
        self.assertIsNone(self.week()["avg_pace"])
        for shoe in self.shoes().values():
            self.assertIsNone(shoe["avg_pace"])
            self.assertIsNone(shoe["avg_cvd"])
            self.assertEqual(shoe["cvd_count"], 0)

    def test_shoe_cvd_is_duration_weighted_and_respects_exclusions(self):
        self.add_activity(computed_vdot=60, duration_s=3600, distance_mi=6)
        self.add_activity(computed_vdot=40, duration_s=600, distance_mi=1)
        self.add_activity(computed_vdot=20, duration_s=3600, exclude_from_vdot=1)
        self.add_activity(computed_vdot=30, duration_s=3600, suppress_hr=1)
        self.add_activity(duration_s=3600)
        self.add_activity(shoe_id=2, computed_vdot=50, duration_s=1800)
        shoes = self.shoes()
        self.assertAlmostEqual(shoes[1]["avg_cvd"], (60 * 3600 + 40 * 600) / 4200)
        self.assertEqual(shoes[1]["cvd_count"], 2)
        self.assertEqual(shoes[1]["total_miles"], 7)
        self.assertEqual(shoes[1]["activity_count"], 5)
        self.assertEqual(shoes[2]["avg_cvd"], 50)
        self.assertEqual(shoes[2]["cvd_count"], 1)

    def test_shoe_cvd_refreshes_after_exclusion_and_reinclusion(self):
        aid = self.add_activity(computed_vdot=60, duration_s=3600)
        self.assertEqual(self.shoes()[1]["avg_cvd"], 60)
        self.client.post(f"/api/activity/{aid}/flags", json={"exclude_from_vdot": True})
        self.assertIsNone(self.shoes()[1]["avg_cvd"])
        self.client.post(f"/api/activity/{aid}/flags", json={"exclude_from_vdot": False})
        self.assertEqual(self.shoes()[1]["avg_cvd"], 60)

    def test_shoe_cvd_uses_same_minimum_weight_as_daily_cvd(self):
        self.add_activity(computed_vdot=60, duration_s=120)
        self.add_activity(computed_vdot=30)
        self.assertEqual(self.shoes()[1]["avg_cvd"], 50)


if __name__ == "__main__":
    unittest.main()
