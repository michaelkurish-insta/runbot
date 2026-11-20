import sqlite3
import tempfile
import unittest
from contextlib import closing, contextmanager
from pathlib import Path

from runbase.analysis.interval_enricher import enrich_activity
from runbase.analysis.vdot_eligibility import (
    refresh_stride_vdot_exclusion, stride_distance_fraction, stride_exclusion_decision,
)
from runbase.db import init_db
from runbase.review.app import create_app


def lap(distance, *, stride=True, source="fit_lap", **values):
    return {"source": source, "gps_measured_distance_mi": distance,
            "is_stride": stride, **values}


class StrideFractionTests(unittest.TestCase):
    def test_forty_percent_is_inclusive_but_just_below_is_not(self):
        for distance, expected in ((.39999, (None, None)), (.4, (1, "strides")), (.6, (1, "strides"))):
            with self.subTest(distance=distance):
                self.assertEqual(stride_exclusion_decision({"distance_mi": 1}, [lap(distance)]), expected)

    def test_duplicate_sources_and_manual_overlays_do_not_add_distance(self):
        rows = [lap(.3), lap(.3, source="strava_lap"), lap(.3, source="mile_split"),
                lap(.3, source="manual_split"), lap(.7, stride=False)]
        self.assertEqual(stride_distance_fraction(1, rows), .3)
        self.assertEqual(stride_exclusion_decision({"distance_mi": 1}, rows), (None, None))

    def test_generated_splits_and_pace_segments_can_be_fallbacks(self):
        for source in ("mile_split", "pace_segment", "manual_split"):
            with self.subTest(source=source):
                self.assertEqual(stride_distance_fraction(1, [lap(.4, source=source)]), .4)

    def test_full_activity_distance_includes_recoveries(self):
        activity = {"distance_mi": 1, "adjusted_distance_mi": .5}
        self.assertEqual(stride_exclusion_decision(activity, [lap(.3)]), (None, None))

    def test_missing_distance_and_stride_count_alone_cannot_exclude(self):
        for distance in (None, 0, -1, float("nan")):
            self.assertIsNone(stride_distance_fraction(distance, [lap(.5)]))
        self.assertEqual(stride_exclusion_decision({"distance_mi": 1, "strides": 10}, []), (None, None))

    def test_walking_does_not_count_as_stride_distance(self):
        self.assertEqual(stride_distance_fraction(1, [lap(.6, is_walking=True)]), 0)


class StrideVdotIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / "test.db"
        self.config = {"paths": {"db": str(self.db)}}
        init_db(self.config)
        with self.connect() as conn:
            conn.execute("INSERT INTO shoes (id,name) VALUES (1,'Daily')")
            conn.execute("INSERT INTO activities (id,date,distance_mi,adjusted_distance_mi,duration_s,avg_pace_s_per_mi,computed_vdot,shoe_id) VALUES (1,'2026-11-18',1,.8,600,600,60,1)")
            conn.executemany("INSERT INTO intervals (id,activity_id,source,rep_number,gps_measured_distance_mi,duration_s,pace_zone,is_stride) VALUES (?,1,'manual',?,?,?,?,?)", [
                (1, 1, .4, 20, "R", 1), (2, 2, .6, 580, "E", 0),
            ])
        app = create_app(self.config)
        app.testing = True
        self.client = app.test_client()

    @contextmanager
    def connect(self):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            yield conn

    def refresh(self):
        with self.connect() as conn:
            return refresh_stride_vdot_exclusion(conn, 1)

    def state(self):
        with self.connect() as conn:
            return conn.execute("SELECT exclude_from_vdot,vdot_exclusion_source FROM activities WHERE id=1").fetchone()

    def test_default_exclusion_preserves_cvd_distance_and_intensity(self):
        before_load = self.client.get("/api/training_load?from=2026-11-18").get_json()
        self.refresh()
        self.assertEqual(self.state(), (1, "strides"))
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT distance_mi,adjusted_distance_mi,computed_vdot FROM activities WHERE id=1").fetchone(), (1, .8, 60))
        self.assertEqual(self.client.get("/api/training_load?from=2026-11-18").get_json(), before_load)
        self.assertIsNone(self.client.get("/api/shoes").get_json()[0]["avg_cvd"])
        meta = self.client.get("/api/activity/1/meta").get_json()
        self.assertTrue(meta["exclude_from_vdot"])
        self.assertEqual(meta["vdot_exclusion_source"], "strides")

    def test_manual_inclusion_survives_refresh_and_enrichment(self):
        self.refresh()
        response = self.client.post("/api/activity/1/flags", json={"exclude_from_vdot": False})
        self.assertTrue(response.get_json()["ok"])
        self.assertEqual(self.state(), (0, None))
        self.refresh()
        self.assertEqual(self.client.get("/api/shoes").get_json()[0]["avg_cvd"], 60)
        with self.connect() as conn:
            enrich_activity(conn, 1, self.config)
        self.assertEqual(self.state(), (0, None))

    def test_existing_manual_exclusion_is_not_replaced_or_reversed(self):
        self.client.post("/api/activity/1/flags", json={"exclude_from_vdot": True})
        self.refresh()
        self.client.put("/api/interval/1/flag", json={"field": "is_stride", "value": False})
        self.assertEqual(self.state(), (1, None))

    def test_stride_checkbox_recalculates_and_can_reverse_the_default(self):
        off = self.client.put("/api/interval/1/flag", json={"field": "is_stride", "value": False}).get_json()
        self.assertFalse(off["exclude_from_vdot"])
        on = self.client.put("/api/interval/1/flag", json={"field": "is_stride", "value": True}).get_json()
        self.assertTrue(on["exclude_from_vdot"])
        self.assertEqual(on["vdot_exclusion_source"], "strides")
        self.client.put("/api/interval/1/flag", json={"field": "is_stride", "value": False})
        self.assertEqual(self.state(), (None, None))

    def test_distance_and_walking_edits_recalculate_the_default(self):
        self.refresh()
        self.client.put("/api/interval/1/edit", json={"field": "distance", "value": .3})
        self.assertEqual(self.state(), (None, None))
        self.client.put("/api/interval/1/edit", json={"field": "distance", "value": .4})
        self.assertEqual(self.state(), (1, "strides"))
        self.client.put("/api/interval/1/walking", json={"is_walking": True})
        self.assertEqual(self.state(), (None, None))
        self.client.put("/api/interval/1/walking", json={"is_walking": False})
        self.assertEqual(self.state(), (1, "strides"))
        self.client.post("/api/activity/1/override", json={"field_name": "distance_mi", "override_value": 2})
        self.assertEqual(self.state(), (None, None))

    def test_enrichment_applies_the_rule_and_keeps_it_idempotent(self):
        with self.connect() as conn:
            enrich_activity(conn, 1, self.config)
            enrich_activity(conn, 1, self.config)
        self.assertEqual(self.state(), (1, "strides"))


if __name__ == "__main__":
    unittest.main()
