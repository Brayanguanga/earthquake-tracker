"""Tests for data transformation logic."""

from earthquake_tracker.transform import compute_daily_aggregates, magnitude_bucket


class TestMagnitudeBucket:
    def test_below_two(self):
        assert magnitude_bucket(0.0) == "0-2"
        assert magnitude_bucket(1.9) == "0-2"

    def test_two_to_four(self):
        assert magnitude_bucket(2.0) == "2-4"
        assert magnitude_bucket(3.9) == "2-4"

    def test_four_to_six(self):
        assert magnitude_bucket(4.0) == "4-6"
        assert magnitude_bucket(5.9) == "4-6"

    def test_six_plus(self):
        assert magnitude_bucket(6.0) == "6+"
        assert magnitude_bucket(9.5) == "6+"

    def test_none_is_unknown(self):
        assert magnitude_bucket(None) == "unknown"


class TestComputeDailyAggregates:
    def _make_events(self, rows: list[tuple]) -> list[dict]:
        return [{"date": d, "magnitude": m} for d, m in rows]

    def test_single_day_single_bucket(self):
        events = self._make_events([("2024-04-20", 1.0), ("2024-04-20", 0.5)])
        result = compute_daily_aggregates(events)

        assert len(result) == 1
        assert result[0] == {"date": "2024-04-20", "mag_bucket": "0-2", "count": 2}

    def test_multiple_buckets_same_day(self):
        events = self._make_events([
            ("2024-04-20", 1.0),
            ("2024-04-20", 3.0),
            ("2024-04-20", 5.0),
            ("2024-04-20", 7.0),
        ])
        result = compute_daily_aggregates(events)
        buckets = {r["mag_bucket"]: r["count"] for r in result}

        assert buckets == {"0-2": 1, "2-4": 1, "4-6": 1, "6+": 1}

    def test_multiple_days(self):
        events = self._make_events([
            ("2024-04-19", 1.0),
            ("2024-04-20", 3.0),
            ("2024-04-20", 3.5),
        ])
        result = compute_daily_aggregates(events)

        assert len(result) == 2
        day20 = next(r for r in result if r["date"] == "2024-04-20")
        assert day20["count"] == 2

    def test_null_magnitude_counted_as_unknown(self):
        events = self._make_events([("2024-04-20", None)])
        result = compute_daily_aggregates(events)

        assert result[0]["mag_bucket"] == "unknown"
        assert result[0]["count"] == 1

    def test_empty_input(self):
        assert compute_daily_aggregates([]) == []

    def test_event_without_date_is_skipped(self):
        events = [{"magnitude": 3.0}]  # no "date" key
        result = compute_daily_aggregates(events)
        assert result == []

    def test_results_sorted_by_date_then_bucket(self):
        events = self._make_events([
            ("2024-04-21", 1.0),
            ("2024-04-20", 5.0),
            ("2024-04-20", 1.0),
        ])
        result = compute_daily_aggregates(events)
        dates = [r["date"] for r in result]

        assert dates == sorted(dates)
