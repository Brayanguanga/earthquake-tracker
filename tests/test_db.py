"""Tests for database layer — uses in-memory SQLite."""

import pytest

from earthquake_tracker.db import (
    get_aggregates,
    get_events_for_date,
    init_db,
    upsert_aggregates,
    upsert_events,
)

DB = ":memory:"  # never touches disk

EVENTS = [
    {
        "id": "ev1",
        "time": "2024-04-20T00:00:00+00:00",
        "date": "2024-04-20",
        "magnitude": 3.5,
        "place": "Somewhere, CA",
        "longitude": -118.25,
        "latitude": 34.05,
        "depth_km": 10.0,
        "event_type": "earthquake",
        "status": "reviewed",
        "url": "https://example.com/ev1",
    },
    {
        "id": "ev2",
        "time": "2024-04-20T12:00:00+00:00",
        "date": "2024-04-20",
        "magnitude": 1.2,
        "place": "Elsewhere, NV",
        "longitude": -115.0,
        "latitude": 36.0,
        "depth_km": 5.0,
        "event_type": "earthquake",
        "status": "automatic",
        "url": None,
    },
]

AGGREGATES = [
    {"date": "2024-04-20", "mag_bucket": "0-2", "count": 1},
    {"date": "2024-04-20", "mag_bucket": "2-4", "count": 1},
]


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "test.db"
    init_db(path)
    return path


class TestInitDb:
    def test_creates_tables(self, db):
        # If init didn't raise, tables exist; verify by querying
        from earthquake_tracker.db import get_connection
        with get_connection(db) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "events" in tables
        assert "daily_aggregates" in tables

    def test_idempotent(self, db):
        init_db(db)  # second call must not raise


class TestUpsertEvents:
    def test_inserts_events(self, db):
        upsert_events(EVENTS, db)
        rows = get_events_for_date("2024-04-20", db)

        assert len(rows) == 2
        ids = {r["id"] for r in rows}
        assert ids == {"ev1", "ev2"}

    def test_replaces_on_conflict(self, db):
        upsert_events(EVENTS, db)
        updated = [{**EVENTS[0], "magnitude": 4.0}]
        upsert_events(updated, db)

        rows = get_events_for_date("2024-04-20", db)
        ev1 = next(r for r in rows if r["id"] == "ev1")
        assert ev1["magnitude"] == 4.0

    def test_empty_list_is_noop(self, db):
        assert upsert_events([], db) == 0

    def test_returns_count(self, db):
        count = upsert_events(EVENTS, db)
        assert count == 2


class TestUpsertAggregates:
    def test_inserts_aggregates(self, db):
        upsert_aggregates(AGGREGATES, db)
        rows = get_aggregates(db)

        assert len(rows) == 2

    def test_replaces_on_conflict(self, db):
        upsert_aggregates(AGGREGATES, db)
        updated = [{"date": "2024-04-20", "mag_bucket": "0-2", "count": 99}]
        upsert_aggregates(updated, db)

        rows = get_aggregates(db)
        row = next(r for r in rows if r["mag_bucket"] == "0-2")
        assert row["count"] == 99

    def test_empty_list_is_noop(self, db):
        assert upsert_aggregates([], db) == 0


class TestCheckpoint:
    def test_returns_none_when_no_checkpoint(self, db):
        from earthquake_tracker.db import get_checkpoint
        assert get_checkpoint(db) is None

    def test_saves_and_retrieves_checkpoint(self, db):
        from earthquake_tracker.db import get_checkpoint, set_checkpoint
        set_checkpoint("2024-04-20T12:00:00+00:00", db)
        assert get_checkpoint(db) == "2024-04-20T12:00:00+00:00"

    def test_overwrites_previous_checkpoint(self, db):
        from earthquake_tracker.db import get_checkpoint, set_checkpoint
        set_checkpoint("2024-04-19T00:00:00+00:00", db)
        set_checkpoint("2024-04-20T12:00:00+00:00", db)
        assert get_checkpoint(db) == "2024-04-20T12:00:00+00:00"


class TestGetEventsForDate:
    def test_returns_only_matching_date(self, db):
        extra = [{**EVENTS[0], "id": "ev3", "date": "2024-04-19",
                  "time": "2024-04-19T00:00:00+00:00"}]
        upsert_events(EVENTS + extra, db)

        rows = get_events_for_date("2024-04-19", db)
        assert len(rows) == 1
        assert rows[0]["id"] == "ev3"

    def test_empty_when_no_match(self, db):
        rows = get_events_for_date("2000-01-01", db)
        assert rows == []
