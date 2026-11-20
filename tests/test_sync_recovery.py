import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from runbase.db import get_connection, init_db
from runbase.ingest.icloud_sync import sync_icloud, _pending_enrichment
from runbase.ingest.strava_sync import sync_strava, _get_client, _save_tokens
from runbase.status import system_status


class SyncRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.config = {'paths': {'db': str(self.root/'test.db'), 'icloud_healthfit': str(self.root), 'raw_store': str(self.root/'raw')},
                       'athlete': {'hr_max': 189},
                       'strava': {'token_file': str(self.root/'tokens.json'), 'client_id': 123, 'client_secret': 'PRIVATE-TEST-SECRET'}}
        init_db(self.config)

    def test_failed_enrichment_is_retried_even_on_first_sync(self):
        def insert(conn, *args):
            aid = conn.execute("INSERT INTO activities (date) VALUES ('2026-11-12')").lastrowid
            conn.execute("INSERT INTO processed_files (file_path,source,activity_id) VALUES ('new.fit','healthfit',?)", (aid,))
            conn.commit()
            return aid
        with closing(get_connection(self.config)) as conn:
            conn.execute("INSERT INTO vdot_history (effective_date,vdot) VALUES ('2026-01-01',57.5)")
            conn.commit()
        with patch('runbase.ingest.icloud_sync._trigger_icloud_download'), \
             patch('runbase.ingest.icloud_sync._scan_fit_files', return_value=[Path('new.fit')]), \
             patch('runbase.ingest.icloud_sync._import_single_file', side_effect=insert), \
             patch('runbase.ingest.icloud_sync._enrich_new_activities', return_value=0), \
             patch('runbase.analysis.interval_enricher.enrich_activity', side_effect=RuntimeError('enrichment failed')):
            result = sync_icloud(self.config)
        self.assertEqual(result['errors'], 1)
        with closing(get_connection(self.config)) as conn:
            self.assertEqual(_pending_enrichment(conn), [1])
        def enrich(conn, aid, *args, **kwargs):
            conn.execute('UPDATE activities SET vdot=57.5 WHERE id=?', (aid,))
            conn.commit()
        with patch('runbase.ingest.icloud_sync._trigger_icloud_download'), \
             patch('runbase.ingest.icloud_sync._scan_fit_files', return_value=[]), \
             patch('runbase.ingest.icloud_sync._enrich_new_activities', return_value=0), \
             patch('runbase.analysis.interval_enricher.enrich_activity', side_effect=enrich):
            result = sync_icloud(self.config)
        self.assertEqual((result['new'], result['resumed'], result['errors']), (0, 1, 0))
        with closing(get_connection(self.config)) as conn:
            self.assertEqual(_pending_enrichment(conn), [])

    def test_partial_strava_failure_does_not_skip_failed_activity_on_retry(self):
        checkpoint = '2026-10-01T00:00:00+00:00'
        with closing(get_connection(self.config)) as conn:
            conn.execute("INSERT INTO sync_state (source,last_sync_at,metadata_json) VALUES ('strava',?,?)",
                         (checkpoint, json.dumps({'last_activity_timestamp': checkpoint})))
            conn.commit()
        activities = [SimpleNamespace(id=i, type='Run', start_date=datetime(2026,11,i,tzinfo=timezone.utc)) for i in (1,2)]
        client = MagicMock()
        client.get_activities.return_value = activities
        data = {'date':'2026-11-01','distance_mi':1.0,'name':'Run'}
        with patch('runbase.ingest.strava_sync._get_client', return_value=client), \
             patch('runbase.ingest.strava_sync._update_rate_limiter'), \
             patch('runbase.ingest.strava_sync._match_strava_activity', return_value=None), \
             patch('runbase.ingest.strava_sync._insert_activity_source'), \
             patch('runbase.ingest.strava_sync._extract_strava_data', side_effect=[RuntimeError('temporary failure'), data]):
            result = sync_strava(self.config)
        self.assertEqual(result['errors'], 1)
        with closing(get_connection(self.config)) as conn:
            self.assertEqual(conn.execute("SELECT last_sync_at FROM sync_state WHERE source='strava'").fetchone()[0], checkpoint)
        with patch('runbase.ingest.strava_sync._get_client', return_value=client), \
             patch('runbase.ingest.strava_sync._update_rate_limiter'), \
             patch('runbase.ingest.strava_sync._match_strava_activity', return_value=None), \
             patch('runbase.ingest.strava_sync._insert_activity_source'), \
             patch('runbase.ingest.strava_sync._extract_strava_data', return_value=data):
            result = sync_strava(self.config)
        self.assertEqual((result['errors'],result['unmatched'],result['skipped']), (0,1,1))

    def test_token_refresh_preserves_permissions_and_private_file_mode(self):
        _save_tokens(self.config, {'access_token':'old','refresh_token':'refresh','expires_at':1,'scope':['read','activity:read_all']})
        client = MagicMock()
        client.refresh_access_token.return_value = {'access_token':'new','refresh_token':'new-refresh','expires_at':9999999999}
        with patch.dict(os.environ), patch('runbase.ingest.strava_sync.Client', return_value=client):
            _get_client(self.config)
        path = Path(self.config['strava']['token_file'])
        self.assertEqual(json.loads(path.read_text())['scope'], ['read','activity:read_all'])
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        client.protocol.refresh_access_token = MagicMock(return_value={
            'access_token': 'auto-new', 'refresh_token': 'auto-refresh', 'expires_at': 9999999999})
        with patch.dict(os.environ), patch('runbase.ingest.strava_sync.Client', return_value=client):
            configured = _get_client(self.config)
            configured.protocol.refresh_access_token()
        saved = json.loads(path.read_text())
        self.assertEqual(saved['refresh_token'], 'auto-refresh')
        self.assertEqual(saved['scope'], ['read', 'activity:read_all'])

    def test_strava_promotion_keeps_local_clock_and_links_import_record(self):
        from runbase.reconcile.enricher import promote_orphans
        orphan = {'id':1, 'source_id':'1234', 'start_date':'2026-10-26', 'distance_mi':8, 'duration_s':3750,
                  'metadata': {'start_time':'2026-10-26T08:47:30+00:00'}}
        with closing(get_connection(self.config)) as conn:
            conn.execute("INSERT INTO activity_sources (id,source,source_id) VALUES (1,'strava','1234')")
            conn.execute("INSERT INTO processed_files (file_path,source) VALUES ('strava:1234','strava')")
            promoted = promote_orphans(conn, [[orphan]])
            aid = promoted[0]['activity_id']
            self.assertEqual(conn.execute('SELECT start_time FROM activities WHERE id=?',(aid,)).fetchone()[0], '2026-10-26T08:47:30')
            self.assertEqual(conn.execute('SELECT activity_id FROM processed_files').fetchone()[0],aid)

    def test_status_distinguishes_missing_cvd_from_expected_exclusion(self):
        with closing(get_connection(self.config)) as conn:
            conn.execute("INSERT INTO activities (date,vdot,gap_s_per_mi,avg_hr) VALUES ('2026-11-12',57.5,600,140), ('2026-11-11',57.5,600,80)")
            conn.commit()
        report = system_status(self.config)
        cvd = next(c for c in report['checks'] if c['name']=='Recent CVD')
        self.assertEqual(cvd['status'], 'warning')
        self.assertIn('1 eligible activities', cvd['message'])
        self.assertIn('Expected blanks', cvd['message'])
        self.assertNotIn('PRIVATE-TEST-SECRET', json.dumps(report))
        missing = self.root/'missing.db'
        self.config['paths']['db'] = str(missing)
        self.assertFalse(system_status(self.config)['ok'])
        self.assertFalse(missing.exists())

    def test_garmin_supports_current_and_legacy_token_backends(self):
        from runbase.ingest.garmin_sync import _save_session, _get_client as garmin_client
        for attribute in ('client', 'garth'):
            backend = MagicMock()
            _save_session(SimpleNamespace(**{attribute: backend}), self.root/'garmin')
            backend.dump.assert_called_once_with(str(self.root/'garmin'))
        self.config['garmin'] = {'token_store': str(self.root/'absent'), 'email': '${GARMIN_EMAIL}', 'password': '${GARMIN_PASSWORD}'}
        with self.assertRaises(FileNotFoundError), patch('garminconnect.Garmin') as client:
            garmin_client(self.config)
        client.assert_not_called()
