import math
import unittest

from runbase.analysis.grade_adjustment import compute_gap, _minetti_cost_ratio, METERS_PER_MILE, FT_PER_M


def recording(length=1000, step=5, grade=0, source=1, start=0, elevation=100):
    return [dict(timestamp_s=start + x/3, distance_mi=x/METERS_PER_MILE,
                 altitude_ft=(elevation + grade*x)*FT_PER_M, source_id=source)
            for x in range(0, length+1, step)]


class GradeAdjustmentTests(unittest.TestCase):
    def test_flat_run_preserves_actual_pace(self):
        self.assertAlmostEqual(compute_gap(recording(), 480), 480)

    def test_constant_real_hill_retains_expected_adjustment(self):
        for grade in (-.04, .02, .08):
            with self.subTest(grade=grade):
                self.assertAlmostEqual(compute_gap(recording(grade=grade), 480), 480/_minetti_cost_ratio(grade))

    def test_short_altitude_noise_does_not_turn_flat_run_into_hills(self):
        points = recording(step=2)
        for p in points:
            x = p['distance_mi']*METERS_PER_MILE
            p['altitude_ft'] += 1.5*math.sin(x*2*math.pi/20)*FT_PER_M
        self.assertLess(abs(compute_gap(points, 480)-480), 1)

    def test_source_boundaries_and_reset_distances_do_not_create_hills(self):
        points = recording(source=None) + recording(source=2, start=400, elevation=300)
        self.assertAlmostEqual(compute_gap(points, 480), 480)
        points = recording() + recording(start=400, elevation=300)
        self.assertAlmostEqual(compute_gap(points, 480), 480)

    def test_pause_and_missing_samples_break_grade_windows(self):
        for missing in (False, True):
            points = recording()
            second = recording(start=500, elevation=300)
            for p in second:
                p['distance_mi'] += 1005/METERS_PER_MILE
            if missing:
                second[0]['timestamp_s'] = points[-1]['timestamp_s']+2
                points.append(dict(timestamp_s=points[-1]['timestamp_s']+1, distance_mi=None, altitude_ft=None, source_id=1))
            self.assertAlmostEqual(compute_gap(points+second, 480),480)

    def test_sampling_frequency_does_not_change_a_real_grade(self):
        self.assertAlmostEqual(compute_gap(recording(grade=.03,step=1),480),
                               compute_gap(recording(grade=.03,step=10),480))

    def test_missing_or_nonfinite_data_does_not_produce_a_score(self):
        self.assertIsNone(compute_gap([], 480))
        self.assertIsNone(compute_gap([dict(timestamp_s=1, distance_mi=float('nan'), altitude_ft=100)],480))
