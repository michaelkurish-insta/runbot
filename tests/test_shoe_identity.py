import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from runbase.db import init_db
from runbase.ingest.strava_sync import _ensure_shoe, _legacy_shoe_for_gear


class ShoeIdentityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / "test.db"
        init_db({"paths": {"db": str(self.db)}})
        self.conn = sqlite3.connect(self.db)
        self.addCleanup(self.conn.close)
        self.client = Mock()
        self.client.get_gear.return_value = SimpleNamespace(name="Adidas Takumi Sen 9", brand_name="Adidas", model_name="Takumi Sen 9")
        self.limiter = Mock()
        self.limiter.check.return_value = True

    def shoe(self, gear=None):
        return self.conn.execute("INSERT INTO shoes (name,strava_gear_id) VALUES ('Adidas Takumi Sen 9',?)", (gear,)).lastrowid

    def linked_source(self, shoe, gear):
        aid = self.conn.execute("INSERT INTO activities (date,shoe_id) VALUES ('2025-08-20',?)", (shoe,)).lastrowid
        self.conn.execute("INSERT INTO activity_sources (activity_id,source,metadata_json) VALUES (?,'strava',?)", (aid, json.dumps({"gear_id": gear})))

    def ensure(self, gear="g123"):
        return _ensure_shoe(self.conn, self.client, gear, {}, self.limiter, False)

    def test_reuses_legacy_shoe_with_matching_source_identity(self):
        sid = self.shoe()
        self.linked_source(sid, "g123")
        self.assertEqual(self.ensure(), sid)
        self.assertEqual(self.conn.execute("SELECT strava_gear_id FROM shoes WHERE id=?", (sid,)).fetchone()[0], "g123")
        self.assertEqual(self.conn.execute("SELECT count(*) FROM shoes").fetchone()[0], 1)
        self.client.get_gear.assert_not_called()

    def test_same_model_name_does_not_prove_two_pairs_are_identical(self):
        sid = self.shoe()
        self.assertNotEqual(self.ensure(), sid)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM shoes").fetchone()[0], 2)

    def test_conflicting_source_gear_ids_prevent_automatic_reuse(self):
        sid = self.shoe()
        self.linked_source(sid, "g123")
        self.linked_source(sid, "g456")
        self.assertIsNone(_legacy_shoe_for_gear(self.conn, "g123"))

    def test_ambiguous_legacy_candidates_are_not_silently_combined(self):
        for _ in range(2):
            self.linked_source(self.shoe(), "g123")
        self.assertIsNone(_legacy_shoe_for_gear(self.conn, "g123"))

    def test_different_registered_pairs_remain_separate(self):
        sid = self.shoe("g456")
        self.assertNotEqual(self.ensure(), sid)
        self.assertEqual(self.conn.execute("SELECT strava_gear_id FROM shoes WHERE id=?", (sid,)).fetchone()[0], "g456")

    def test_existing_gear_mapping_is_reused_without_a_network_request(self):
        sid = self.shoe("g123")
        self.assertEqual(self.ensure(), sid)
        self.client.get_gear.assert_not_called()


if __name__ == "__main__":
    unittest.main()
