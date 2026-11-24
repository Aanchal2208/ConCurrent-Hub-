from __future__ import annotations

import unittest

from benchmark import Metrics, operation_is_contended, percentile


class BenchmarkMetricTests(unittest.TestCase):
    def test_percentiles_interpolate_between_samples(self) -> None:
        values = [10.0, 20.0, 30.0, 40.0]
        self.assertEqual(percentile(values, 0.50), 25.0)
        self.assertAlmostEqual(percentile(values, 0.95) or 0.0, 38.5)
        self.assertIsNone(percentile([], 0.50))

    def test_metrics_count_conflicts_separately_from_failures(self) -> None:
        metrics = Metrics()
        metrics.record("ok", 4.0)
        metrics.record("conf", 8.0)
        metrics.record("fail", 12.0)

        snapshot = metrics.snapshot(2.0, 64.0)

        self.assertEqual((snapshot["done"], snapshot["ok"], snapshot["conf"], snapshot["fail"]), (3, 1, 1, 1))
        self.assertEqual(snapshot["req_per_sec"], 1.5)
        self.assertEqual(snapshot["rss_mb"], 64.0)

    def test_workloads_select_contention_consistently(self) -> None:
        self.assertFalse(operation_is_contended("low_contention", 0))
        self.assertTrue(operation_is_contended("high_contention", 7))
        self.assertTrue(operation_is_contended("mixed_workload", 0))
        self.assertFalse(operation_is_contended("mixed_workload", 1))


if __name__ == "__main__":
    unittest.main()
