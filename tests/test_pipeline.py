"""Integration test for the pipeline — API fully mocked."""

from datetime import date
from unittest.mock import patch

import pytest

from earthquake_tracker.pipeline import run
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

    def test_run_propagates_fetch_error(self, db):
        def _boom(*_args, **_kwargs):
            raise RuntimeError("network failure")
            yield  # make it a generator

        with patch("earthquake_tracker.pipeline.fetch_events", side_effect=_boom):
            with pytest.raises(RuntimeError, match="network failure"):
                run(start=date(2024, 3, 21), end=date(2024, 4, 20), db_path=db)
