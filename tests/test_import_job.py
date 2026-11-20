import sys
import tempfile
import time
import unittest
from pathlib import Path

from runbase.review.import_job import ImportJob


class ImportJobTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def job(self, code, timeout=5):
        return ImportJob([sys.executable, '-u', '-c', code], self.tmp.name, timeout)

    def wait_for(self, predicate, timeout=3):
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            if predicate():
                return
            time.sleep(.01)
        self.fail('Import did not reach expected state')

    def test_live_progress_and_duplicate_start_guard(self):
        job = self.job("from pathlib import Path; import time; print('=== iCloud sync ==='); print('Checking HealthFit file 1/10');\nwhile not Path('continue').exists(): time.sleep(.01)\nprint('2 new, 8 skipped')")
        self.assertTrue(job.start())
        try:
            self.wait_for(lambda: 'Checking HealthFit' in job.snapshot()['output'])
            state = job.snapshot()
            self.assertTrue(state['running'])
            self.assertEqual(state['stage'], 'iCloud sync')
            self.assertFalse(job.start())
        finally:
            Path(self.tmp.name, 'continue').touch()
        self.wait_for(lambda: not job.snapshot()['running'])
        self.assertTrue(job.snapshot()['success'])
        self.assertEqual(job.snapshot()['new_count'], 2)

    def test_failure_preserves_the_reason_and_output(self):
        job = self.job("import sys; print('Reading file 3/10'); print('File could not be read', file=sys.stderr); sys.exit(2)")
        job.start()
        self.wait_for(lambda: not job.snapshot()['running'])
        state = job.snapshot()
        self.assertFalse(state['success'])
        self.assertIn('File could not be read', state['message'])
        self.assertIn('Reading file 3/10', state['output'])

    def test_missing_source_warning_is_retained_on_success(self):
        job = self.job("print('WARNING: Strava skipped: login tokens are missing.'); print('Pipeline complete')")
        job.start()
        self.wait_for(lambda: not job.snapshot()['running'])
        self.assertTrue(job.snapshot()['success'])
        self.assertEqual(job.snapshot()['warnings'], ['Strava skipped: login tokens are missing.'])

    def test_timeout_keeps_partial_progress(self):
        job = self.job("import time; print('Waiting for iCloud download'); time.sleep(60)", timeout=.25)
        job.start()
        self.wait_for(lambda: not job.snapshot()['running'])
        state = job.snapshot()
        self.assertFalse(state['success'])
        self.assertIn('timed out', state['message'])
        self.assertIn('Waiting for iCloud download', state['output'])

    def test_success_reports_existing_rows_enriched_and_strava_matches(self):
        job = self.job("print('0 new, 274 skipped'); print('Enrichment complete: 13 enriched, 0 failed, 0 skipped.'); print('Pipeline complete: 0 new, 20 Strava matched, 0 reconciled')")
        job.start()
        self.wait_for(lambda: not job.snapshot()['running'])
        state = job.snapshot()
        self.assertTrue(state['success'])
        self.assertEqual(state['enriched_count'], 13)
        self.assertEqual(state['strava_matched'], 20)
        self.assertIn('13 enriched, 20 Strava matched', state['message'])
