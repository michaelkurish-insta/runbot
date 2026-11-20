import os
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from runbase.ingest.fit_parser import _extract_session, _extract_records, _extract_laps
from runbase.analysis.weather import fetch_weather


class Message:
    def __init__(self, name, **values):
        self.name, self.values = name, values

    def get_value(self, name):
        return self.values.get(name)


class FitTimeTests(unittest.TestCase):
    def test_fit_keeps_button_presses_distinct_from_auto_miles_and_tail(self):
        def lap(trigger, distance):
            return Message('lap', lap_trigger=trigger,total_distance=distance,total_timer_time=400)
        auto = _extract_laps([lap('distance',1609.34),lap('distance',1609.34),lap('session_end',200)],[])
        manual = _extract_laps([lap('manual',1609.34),lap('session_end',200)],[])
        self.assertEqual([x.lap_kind for x in auto],['split','split','split'])
        self.assertEqual([x.lap_kind for x in manual],['interval','interval'])

    def test_local_day_and_dst_offsets_come_from_file(self):
        for month, shift in ((10, 4), (11, 5)):
            with self.subTest(month=month):
                session = Message('session', start_time=datetime(2026, month, 12, 2), sport='running')
                clock = Message('activity', timestamp=datetime(2026, month, 12, 3),
                                local_timestamp=datetime(2026, month, 11, 24+3-shift))
                activity = _extract_session([session, clock])
                self.assertEqual(activity.date, f'2026-{month:02}-11')
                self.assertEqual(activity.start_time, f'2026-{month:02}-11T{24+2-shift:02}:00:00')

    def test_record_epoch_does_not_depend_on_machine_timezone(self):
        record = Message('record', timestamp=datetime(2026, 11, 12, 20, 30))
        expected = datetime(2026, 11, 12, 20, 30, tzinfo=timezone.utc).timestamp()
        try:
            with patch.dict(os.environ, {'TZ': 'America/Los_Angeles'}):
                time.tzset()
                self.assertEqual(_extract_records([record], None)[0].timestamp_s, expected)
        finally:
            time.tzset()

    def test_absent_local_clock_stays_explicitly_utc(self):
        session = Message('session', start_time=datetime(2026, 11, 12, 20, 30))
        self.assertEqual(_extract_session([session]).start_time, '2026-11-12T20:30:00+00:00')

    def test_weather_uses_utc_for_aware_times_and_keeps_zero_cloud_cover(self):
        payload = {'hourly': {'time': ['2026-11-12T15:00', '2026-11-12T20:00'],
                             'temperature_2m': [50, 40], 'relative_humidity_2m': [60, 70],
                             'cloud_cover': [70, 0], 'weather_code': [3, 0]}}
        with patch('runbase.analysis.weather._fetch_json', return_value=payload) as fetch:
            result = fetch_weather(40, -73, datetime.fromisoformat('2026-11-12T15:00:00-05:00'))
        self.assertIn('timezone=UTC', fetch.call_args.args[0])
        self.assertEqual(result['temperature_f'], 40)
        self.assertEqual(result['cloud_cover_pct'], 0)
