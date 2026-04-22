"""Integration test for the pipeline — API fully mocked."""

from datetime import date, datetime, timezone
from unittest.mock import patch

import pytest

from earthquake_tracker.pipeline import run, status, report
from .fixtures import SAMPLE_FEATURE, SAMPLE_FEATURE_NO_MAG


MOCK_FEATURES = [SAMPLE_FEATURE, SAMPLE_FEATURE_NO_MAG]


@pytest.fixture
def db(tmp_path):
    return tmp_path / "pipeline_test.db"


class TestPipeline:
    def test_run_returns_summary(self, db):
        with patch("earthquake_tracker.pipeline.fetch_events", return_value=iter(MOCK_FEATURES)):
            summary = run(start=date(2024, 3, 21), end=date(2024, 4, 20), db_path=db)

        assert summary["events_fetched"] == 2
        assert summary["events_skipped"] == 0
        assert summary["aggregate_rows"] >= 1
        assert summary["start"] == "2024-03-21"
        assert summary["end"] == "2024-04-20"

    def test_run_stores_events_in_db(self, db):
        with patch("earthquake_tracker.pipeline.fetch_events", return_value=iter(MOCK_FEATURES)):
            run(start=date(2024, 3, 21), end=date(2024, 4, 20), db_path=db)

        from earthquake_tracker.db import get_events_for_date
        rows = get_events_for_date("2024-04-20", db)
        assert len(rows) == 2

    def test_run_stores_aggregates_in_db(self, db):
        with patch("earthquake_tracker.pipeline.fetch_events", return_value=iter(MOCK_FEATURES)):
            run(start=date(2024, 3, 21), end=date(2024, 4, 20), db_path=db)

        from earthquake_tracker.db import get_aggregates
        rows = get_aggregates(db)
        assert len(rows) >= 1
        assert all("mag_bucket" in r for r in rows)

    def test_run_handles_empty_result(self, db):
        with patch("earthquake_tracker.pipeline.fetch_events", return_value=iter([])):
            summary = run(start=date(2024, 3, 21), end=date(2024, 4, 20), db_path=db)

        assert summary["events_fetched"] == 0
        assert summary["aggregate_rows"] == 0

    def test_run_skips_malformed_events(self, db):
        bad_feature = {"id": "bad", "properties": None, "geometry": None}
        features = [SAMPLE_FEATURE, bad_feature]

        with patch("earthquake_tracker.pipeline.fetch_events", return_value=iter(features)):
            summary = run(start=date(2024, 3, 21), end=date(2024, 4, 20), db_path=db)

        assert summary["events_fetched"] == 1
        assert summary["events_skipped"] == 1

    def test_run_saves_checkpoint(self, db):
        with patch("earthquake_tracker.pipeline.fetch_events", return_value=iter(MOCK_FEATURES)):
            run(start=date(2024, 3, 21), end=date(2024, 4, 20), db_path=db)

        from earthquake_tracker.db import get_checkpoint
        assert get_checkpoint(db) == "2024-04-20T00:00:00+00:00"

    def test_run_resumes_from_checkpoint(self, db):
        from earthquake_tracker.db import init_db, set_checkpoint
        init_db(db)
        set_checkpoint("2024-04-19T12:00:00+00:00", db)

        with patch("earthquake_tracker.pipeline.fetch_events", return_value=iter(MOCK_FEATURES)) as mock_fetch:
            run(db_path=db)

        call_start = mock_fetch.call_args[0][0]
        from datetime import datetime, timezone
        assert call_start == datetime(2024, 4, 19, 12, 0, tzinfo=timezone.utc)

    def test_run_passes_min_magnitude_to_fetch(self, db):
        with patch("earthquake_tracker.pipeline.fetch_events", return_value=iter([])) as mock_fetch:
            run(start=date(2024, 3, 21), end=date(2024, 4, 20), db_path=db, min_magnitude=2.5)

        assert mock_fetch.call_args[1]["min_magnitude"] == 2.5

    def test_run_propagates_fetch_error(self, db):
        def _boom(*_args, **_kwargs):
            raise RuntimeError("network failure")
            yield  # make it a generator

        with patch("earthquake_tracker.pipeline.fetch_events", side_effect=_boom):
            with pytest.raises(RuntimeError, match="network failure"):
                run(start=date(2024, 3, 21), end=date(2024, 4, 20), db_path=db)


class TestStatus:
    def test_no_runs_yet(self, db):
        result = status(db_path=db)
        assert result["checkpoint"] is None
        assert result["stale"] is None

    def test_fresh_run_not_stale(self, db):
        from earthquake_tracker.db import init_db, set_checkpoint
        init_db(db)
        now = datetime.now(tz=timezone.utc)
        set_checkpoint(now.isoformat(), db)

        result = status(db_path=db, stale_hours=25)
        assert result["stale"] is False

    def test_old_run_is_stale(self, db):
        from earthquake_tracker.db import init_db, set_checkpoint
        from datetime import timedelta
        init_db(db)
        old = datetime.now(tz=timezone.utc) - timedelta(hours=30)
        set_checkpoint(old.isoformat(), db)

        result = status(db_path=db, stale_hours=25)
        assert result["stale"] is True


class TestReport:
    def test_returns_empty_when_no_data(self, db):
        rows = report(db_path=db)
        assert rows == []

    def test_returns_aggregates_after_run(self, db):
        with patch("earthquake_tracker.pipeline.fetch_events", return_value=iter(MOCK_FEATURES)):
            run(start=date(2024, 3, 21), end=date(2024, 4, 20), db_path=db)

        rows = report(db_path=db)
        assert len(rows) >= 1
        assert all({"date", "mag_bucket", "count"} <= set(r) for r in rows)
