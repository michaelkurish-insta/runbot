import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch

from runbase.ingest.icloud_sync import _is_already_processed, _import_single_file, _pending_enrichment


class ICloudMigrationTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.addCleanup(self.conn.close)
        self.conn.executescript('''
            CREATE TABLE processed_files (id INTEGER PRIMARY KEY, source TEXT, file_path TEXT, file_hash TEXT);
            CREATE TABLE suppressed_sources (id INTEGER PRIMARY KEY, source TEXT, source_identifier TEXT);
        ''')
        suffix = '/Library/Mobile Documents/iCloud~com~altifondo~HealthFit/Documents/2026-03-31-run.fit'
        self.old_path = '/Users/olduser' + suffix
        self.new_path = '/Users/newuser' + suffix

    def test_changed_hash_after_machine_move_is_still_processed(self):
        self.conn.execute("INSERT INTO processed_files VALUES (1, 'healthfit', ?, 'old-hash')", (self.old_path,))
        self.assertTrue(_is_already_processed(self.conn, self.new_path, 'new-hash'))
        with patch('runbase.ingest.fit_parser._compute_file_hash', side_effect=AssertionError('Should not download known file')):
            self.assertIsNone(_import_single_file(self.conn, Path(self.new_path), '/unused', False, False))

    def test_deleted_activity_stays_suppressed_after_move(self):
        self.conn.execute("INSERT INTO suppressed_sources VALUES (1, 'healthfit', ?)", (self.old_path,))
        self.assertTrue(_is_already_processed(self.conn, self.new_path))

    def test_matching_basename_in_another_folder_is_not_enough(self):
        self.conn.execute("INSERT INTO processed_files VALUES (1, 'healthfit', ?, 'old-hash')", (self.old_path,))
        self.assertFalse(_is_already_processed(self.conn, '/tmp/2026-03-31-run.fit', 'new-hash'))
        self.assertFalse(_is_already_processed(self.conn, self.new_path.replace('/Documents/', '/Documents/other/'), 'new-hash'))

    def test_hash_deduplication_still_works(self):
        self.conn.execute("INSERT INTO processed_files VALUES (1, 'healthfit', ?, 'same-hash')", (self.old_path,))
        self.assertTrue(_is_already_processed(self.conn, '/tmp/renamed.fit', 'same-hash'))

    def test_retry_resumes_recent_imports_without_reenriching_history(self):
        self.conn.executescript('''
            ALTER TABLE processed_files ADD COLUMN activity_id INTEGER;
            ALTER TABLE processed_files ADD COLUMN processed_at TEXT;
            CREATE TABLE activities (id INTEGER PRIMARY KEY, date TEXT, vdot REAL);
            CREATE TABLE sync_state (source TEXT, last_sync_at TEXT);
            CREATE TABLE vdot_history (effective_date TEXT);
            INSERT INTO sync_state VALUES ('icloud', '2026-10-09T16:17:08+00:00');
            INSERT INTO vdot_history VALUES ('2026-01-01');
            INSERT INTO activities VALUES (1, '2026-09-01', NULL), (2, '2026-11-02', NULL), (3, '2026-11-03', 50);
            INSERT INTO processed_files (source,activity_id,processed_at) VALUES
                ('healthfit',1,'2026-09-01 12:00:00'),
                ('healthfit',2,'2026-11-13 22:00:00'),
                ('healthfit',3,'2026-11-13 22:00:01');
        ''')
        self.assertEqual(_pending_enrichment(self.conn), [2])
