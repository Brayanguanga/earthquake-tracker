"""SQLite persistence layer."""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id          TEXT PRIMARY KEY,
    time        TEXT NOT NULL,
    date        TEXT NOT NULL,
    magnitude   REAL,
    place       TEXT,
    longitude   REAL,
    latitude    REAL,
    depth_km    REAL,
    event_type  TEXT,
    status      TEXT,
    url         TEXT
);

CREATE TABLE IF NOT EXISTS daily_aggregates (
    date        TEXT NOT NULL,
    mag_bucket  TEXT NOT NULL,
    count       INTEGER NOT NULL,
    PRIMARY KEY (date, mag_bucket)
);

CREATE INDEX IF NOT EXISTS idx_events_date ON events(date);
CREATE INDEX IF NOT EXISTS idx_events_mag  ON events(magnitude);
"""


@contextmanager
def get_connection(db_path: str | Path):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str | Path) -> None:
    logger.info("Initialising database at %s", db_path)
    with get_connection(db_path) as conn:
        conn.executescript(SCHEMA)


def upsert_events(events: list[dict], db_path: str | Path) -> int:
    """Insert or replace raw events. Returns count written."""
    if not events:
        return 0

    sql = """
        INSERT OR REPLACE INTO events
            (id, time, date, magnitude, place, longitude, latitude,
             depth_km, event_type, status, url)
        VALUES
            (:id, :time, :date, :magnitude, :place, :longitude, :latitude,
             :depth_km, :event_type, :status, :url)
    """
    with get_connection(db_path) as conn:
        conn.executemany(sql, events)

    logger.info("Upserted %d events", len(events))
    return len(events)


def upsert_aggregates(aggregates: list[dict], db_path: str | Path) -> int:
    """Insert or replace daily aggregate rows. Returns count written."""
    if not aggregates:
        return 0

    sql = """
        INSERT OR REPLACE INTO daily_aggregates (date, mag_bucket, count)
        VALUES (:date, :mag_bucket, :count)
    """
    with get_connection(db_path) as conn:
        conn.executemany(sql, aggregates)

    logger.info("Upserted %d aggregate rows", len(aggregates))
    return len(aggregates)


def get_events_for_date(date_str: str, db_path: str | Path) -> list[dict]:
    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM events WHERE date = ? ORDER BY time", (date_str,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_aggregates(db_path: str | Path) -> list[dict]:
    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM daily_aggregates ORDER BY date, mag_bucket"
        ).fetchall()
    return [dict(r) for r in rows]
