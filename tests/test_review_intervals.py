import sqlite3
import tempfile
import unittest
from contextlib import contextmanager, closing
from pathlib import Path

from runbase.db import init_db
from runbase.review.app import create_app


class ReviewIntervalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / "test.db"
        self.config = {"paths": {"db": str(self.db)}}
        init_db(self.config)
        with self.connect() as conn:
            conn.execute("INSERT INTO activities (id, date, distance_mi, adjusted_distance_mi, duration_s, avg_pace_s_per_mi) VALUES (1, '2026-10-02', 2, 2, 1200, 600)")
        app = create_app(self.config)
        app.testing = True
        self.client = app.test_client()

    @contextmanager
    def connect(self):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            yield conn

    def add_interval(self, source, rep=1, duration=600, zone="E", distance=1, **flags):
        fields = {"activity_id": 1, "rep_number": rep, "source": source,
                  "duration_s": duration, "pace_zone": zone,
                  "gps_measured_distance_mi": distance,
                  "canonical_distance_mi": distance, **flags}
        with self.connect() as conn:
            return conn.execute(
                f"INSERT INTO intervals ({','.join(fields)}) VALUES ({','.join('?' for _ in fields)})",
                list(fields.values()),
            ).lastrowid

    def displayed(self):
        response = self.client.get('/api/activity/1/intervals')
        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        return body['intervals'] + body['laps'] + body.get('splits', [])

    def load(self):
        response = self.client.get('/api/training_load?from=2026-10-02')
        self.assertEqual(response.status_code, 200)
        return response.get_json()['2026-10-02']

    def test_recorded_laps_are_displayed_and_scored_once(self):
        for source in (None, 'fit_lap', 'strava_lap'):
            with self.subTest(source=source):
                with self.connect() as conn:
                    conn.execute('DELETE FROM intervals')
                for rep in (1, 2):
                    self.add_interval(source, rep)
                    self.add_interval('mile_split', rep)
                self.add_interval('pace_segment', duration=1200, distance=2)
                if source != 'strava_lap':
                    for rep in (1, 2):
                        self.add_interval('strava_lap', rep)
                self.assertEqual([iv['source'] for iv in self.displayed()], [source, source])
                self.assertEqual(self.load()['seven_di'], 4.0)
                self.assertEqual(self.load()['seven_d'], 2.0)
                with self.connect() as conn:
                    self.assertEqual(conn.execute('SELECT distance_mi, adjusted_distance_mi FROM activities').fetchone(), (2, 2))
                    self.assertEqual(conn.execute("SELECT count(*) FROM intervals WHERE source='mile_split'").fetchone()[0], 2)

    def test_manual_lap_edits_are_preserved(self):
        self.add_interval('fit_lap')
        manual_id = self.add_interval('manual', 2, zone='T')
        for rep in (1, 2):
            self.add_interval('mile_split', rep)
        self.assertIn(manual_id, [iv['id'] for iv in self.displayed()])
        self.assertEqual(len(self.displayed()), 2)
        self.assertEqual(self.load()['seven_di'], 8.0)

    def test_generated_splits_are_a_fallback(self):
        for rep in (1, 2):
            self.add_interval('mile_split', rep)
        self.add_interval('pace_segment', duration=1200, distance=2)
        self.assertEqual(len(self.displayed()), 2)
        self.assertEqual(self.load()['seven_di'], 4.0)

    def test_auto_laps_use_generated_miles_with_saved_zone_edits(self):
        for rep in (1, 2):
            self.add_interval('fit_lap', rep, lap_kind='split')
            self.add_interval('mile_split', rep, lap_kind='split')
        with self.connect() as conn:
            conn.execute("INSERT INTO interval_zone_overrides (activity_id,source,rep_number,pace_zone) VALUES (1,'mile_split',2,'M')")
        self.assertEqual([iv['source'] for iv in self.displayed()], ['mile_split', 'mile_split'])
        self.assertEqual(self.displayed()[1]['pace_zone'], 'M')
        self.assertEqual(self.load()['seven_di'], 6.0)

    def test_zone_override_follows_manually_edited_recorded_lap(self):
        self.add_interval('fit_lap')
        self.add_interval('manual', 2, zone='T')
        self.add_interval('mile_split', 1, duration=1200, distance=2)
        with self.connect() as conn:
            conn.execute("INSERT INTO interval_zone_overrides (activity_id,source,rep_number,pace_zone) VALUES (1,'fit_lap',2,'I')")
        self.assertEqual(self.displayed()[1]['pace_zone'], 'I')
        self.assertEqual(self.load()['seven_di'], 12.0)

    def test_editing_a_generated_mile_does_not_reintroduce_recorded_laps(self):
        self.add_interval('fit_lap', 1, lap_kind='split')
        self.add_interval('fit_lap', 2, lap_kind='split')
        self.add_interval('mile_split', 1, lap_kind='split')
        edited_id = self.add_interval('mile_split', 2, lap_kind='split')
        response = self.client.put(f'/api/interval/{edited_id}/edit', json={'field': 'pace_zone', 'value': 'M'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual([iv['source'] for iv in self.displayed()], ['mile_split', 'mile_split'])
        self.assertEqual(self.load()['seven_di'], 6.0)

    def test_critical_velocity_is_scored_between_threshold_and_intervals(self):
        self.add_interval('fit_lap', 1, zone='CV')
        self.add_interval('fit_lap', 2, zone='E')
        self.assertEqual(self.load()['seven_di'], 10.0)

    def test_walking_edit_counts_the_visible_miles_once(self):
        for rep in (1, 2):
            self.add_interval('fit_lap', rep, lap_kind='split')
            mile_id = self.add_interval('mile_split', rep, lap_kind='split')
        response = self.client.put(f'/api/interval/{mile_id}/walking', json={'is_walking': True})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.load()['seven_d'], 1.0)

    def test_enrichment_does_not_sum_or_rewrite_generated_mile_splits(self):
        from runbase.analysis.interval_enricher import enrich_activity

        for rep in (1, 2):
            self.add_interval('fit_lap', rep)
            self.add_interval('strava_lap', rep)
            self.add_interval('mile_split', rep, zone='M')
        with self.connect() as conn:
            before = conn.execute("SELECT * FROM intervals WHERE source='mile_split'").fetchall()
            enrich_activity(conn, 1, self.config)
            self.assertEqual(conn.execute('SELECT adjusted_distance_mi FROM activities').fetchone()[0], 2)
            self.assertEqual(conn.execute("SELECT * FROM intervals WHERE source='mile_split'").fetchall(), before)

    def test_partial_laps_do_not_add_to_complete_generated_splits(self):
        self.add_interval('fit_lap', duration=30, distance=.05)
        self.add_interval('mile_split', duration=1200, distance=2)
        self.assertEqual(self.load()['seven_di'], 4.0)

    def test_manual_split_scores_work_and_remainder_once(self):
        for rep in (1, 2):
            self.add_interval('fit_lap', rep)
            self.add_interval('mile_split', rep)
        self.add_interval('manual_split', duration=600, distance=1, zone='T')
        self.assertEqual(self.load()['seven_di'], 8.0)

    def test_fit_selection_precedes_workout_type_filtering(self):
        self.add_interval('fit_lap', is_stride=True, zone='R')
        self.add_interval('strava_lap', zone='FR')
        self.add_interval('mile_split', zone='I')
        response = self.client.get('/api/activity/1/meta')
        self.assertEqual(response.get_json()['workout_type_zone'], '')

    def test_source_enrichment_queue_survives_failure_and_clears_on_success(self):
        from unittest.mock import patch
        from runbase.analysis.interval_enricher import enrich_activity
        self.add_interval('fit_lap', duration=1200, distance=2)
        with self.connect() as conn:
            conn.execute("INSERT INTO vdot_history (effective_date,vdot) VALUES ('2026-01-01',57.5)")
            conn.execute("INSERT INTO activity_sources (activity_id,source,metadata_json) VALUES (1,'strava','{\"enrichment_pending\":true,\"workout_type\":3}')")
        with self.connect() as conn:
            with patch('runbase.analysis.interval_enricher._load_streams', side_effect=RuntimeError('interrupted')):
                with self.assertRaises(RuntimeError):
                    enrich_activity(conn, 1, self.config)
            conn.rollback()
            self.assertEqual(conn.execute("SELECT json_extract(metadata_json,'$.enrichment_pending') FROM activity_sources").fetchone()[0], 1)
            enrich_activity(conn, 1, self.config)
            self.assertIsNone(conn.execute("SELECT json_extract(metadata_json,'$.enrichment_pending') FROM activity_sources").fetchone()[0])
            self.assertEqual(conn.execute("SELECT json_extract(metadata_json,'$.workout_type') FROM activity_sources").fetchone()[0], 3)

    def test_auto_partial_mile_inherits_easy_and_cannot_classify_run_as_marathon(self):
        for source in ('mile_split','fit_lap'):
            with self.subTest(source=source):
                with self.connect() as conn:
                    conn.execute('DELETE FROM intervals')
                self.add_interval(source,1,lap_kind='split')
                self.add_interval(source,2,lap_kind='split')
                self.add_interval(source,3,distance=.18,duration=72,zone='M',lap_kind='split')
                tail = self.client.get('/api/activity/1/intervals').get_json()['splits'][-1]
                self.assertEqual(tail['pace_zone'], 'E')
                self.assertTrue(tail['zone_inherited'])
                self.assertEqual(self.client.get('/api/activity/1/meta').get_json()['workout_type_zone'], 'E')
                self.assertEqual(self.load()['seven_di'], 4.2)

    def test_button_press_fragment_is_not_treated_as_automatic_tail(self):
        self.add_interval('fit_lap',1,lap_kind='interval')
        self.add_interval('fit_lap',2,lap_kind='interval')
        self.add_interval('fit_lap',3,distance=.18,duration=72,zone='M',lap_kind='interval')
        tail = self.displayed()[-1]
        self.assertEqual(tail['pace_zone'],'M')
        self.assertFalse(tail.get('is_partial_split'))
        self.assertEqual(self.client.get('/api/activity/1/meta').get_json()['workout_type_zone'],'M')

    def test_marathon_finish_edits_survive_enrichment_and_leave_source_intact(self):
        from runbase.analysis.interval_enricher import enrich_activity
        for rep in (1,2):
            iid = self.add_interval('fit_lap',rep,lap_kind='split')
        self.add_interval('fit_lap',3,lap_kind='split',distance=.18,duration=72,zone='M')
        response = self.client.put(f'/api/interval/{iid}/edit',json={'field':'pace_zone','value':'M'})
        self.assertEqual(response.status_code,200)
        with self.connect() as conn:
            self.assertEqual(conn.execute('SELECT source,lap_kind FROM intervals WHERE id=?',(iid,)).fetchone(),('fit_lap','split'))
            enrich_activity(conn,1,self.config)
        splits = self.client.get('/api/activity/1/intervals').get_json()['splits']
        self.assertEqual(splits[1]['pace_zone'],'M')
        self.assertTrue(splits[1]['zone_overridden'])
        self.assertEqual(self.client.get('/api/activity/1/meta').get_json()['workout_type_zone'],'M')
        self.assertEqual(self.load()['seven_di'],6.2)

    def test_explicit_partial_zone_scores_but_does_not_classify_whole_workout(self):
        self.add_interval('mile_split',1)
        tail_id = self.add_interval('mile_split',2,distance=.18,duration=72,zone='M')
        self.client.put(f'/api/interval/{tail_id}/edit',json={'field':'pace_zone','value':'M'})
        tail = self.displayed()[-1]
        self.assertEqual(tail['pace_zone'],'M')
        self.assertTrue(tail['zone_overridden'])
        self.assertEqual(self.client.get('/api/activity/1/meta').get_json()['workout_type_zone'],'E')
        self.assertEqual(self.load()['seven_di'],2.5)
        self.client.put(f'/api/interval/{tail_id}/edit',json={'field':'pace_zone','value':''})
        self.assertEqual(self.displayed()[-1]['pace_zone'],'E')
        self.assertEqual(self.load()['seven_di'],2.2)

    def test_unknown_intensity_zone_is_rejected(self):
        iid = self.add_interval('mile_split')
        response = self.client.put(f'/api/interval/{iid}/edit',json={'field':'pace_zone','value':'BOGUS'})
        self.assertEqual(response.status_code,400)
        self.assertEqual(self.displayed()[0]['pace_zone'],'E')
