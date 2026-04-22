"""
SQLite persistence layer.

Schema (4 tables):
    events           — raw earthquake events, one row per USGS event ID
    daily_aggregates — pre-computed daily counts per magnitude bucket
    run_state        — key/value store; holds the incremental fetch checkpoint
    run_history      — append-only log of every pipeline run

All writes use INSERT OR REPLACE so reruns over the same window are idempotent.
Schema migrations are applied automatically on init_db() for existing databases.
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id          TEXT PRIMARY KEY,
    time_ms     INTEGER NOT NULL,
    date        TEXT NOT NULL,
    magnitude   REAL,
    mag_bucket  TEXT NOT NULL,
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

CREATE TABLE IF NOT EXISTS run_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL,
    ended_at        TEXT NOT NULL,
    events_fetched  INTEGER NOT NULL,
    events_skipped  INTEGER NOT NULL,
    aggregate_rows  INTEGER NOT NULL,
    status          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_date        ON events(date);
CREATE INDEX IF NOT EXISTS idx_events_mag         ON events(magnitude);
CREATE INDEX IF NOT EXISTS idx_events_date_bucket ON events(date, mag_bucket);
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


def _migrate(conn) -> None:
    """Apply incremental schema changes to existing databases. No-op on fresh DBs."""
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "events" not in tables:
        return  # fresh database — schema creation handles everything

    def columns():
        return {row[1] for row in conn.execute("PRAGMA table_info(events)").fetchall()}

    # v2: rename time (ISO text) → time_ms (epoch integer)
    existing = columns()
    if "time" in existing and "time_ms" not in existing:
        logger.info("Migration: renaming time → time_ms in events")
        conn.execute("ALTER TABLE events RENAME COLUMN time TO time_ms")
        conn.execute("""
            UPDATE events
            SET time_ms = CAST(
                (julianday(time_ms) - julianday('1970-01-01')) * 86400000 AS INTEGER
            )
        """)
        logger.info("Migration: converted time ISO strings to epoch ms")

    # v2: add mag_bucket column (re-read columns in case rename just ran)
    if "mag_bucket" not in columns():
        logger.info("Migration: adding mag_bucket column to events")
        conn.execute("ALTER TABLE events ADD COLUMN mag_bucket TEXT NOT NULL DEFAULT 'unknown'")


def init_db(db_path: str | Path) -> None:
    logger.info("Initialising database at %s", db_path)
    with get_connection(db_path) as conn:
        _migrate(conn)
        conn.executescript(SCHEMA)


def upsert_events(events: list[dict], db_path: str | Path) -> int:
    """Insert or replace raw events. Returns count written."""
    if not events:
        return 0

    sql = """
        INSERT OR REPLACE INTO events
            (id, time_ms, date, magnitude, mag_bucket, place, longitude, latitude,
             depth_km, event_type, status, url)
        VALUES
            (:id, :time_ms, :date, :magnitude, :mag_bucket, :place, :longitude, :latitude,
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


def add_run_history(record: dict, db_path: str | Path) -> None:
    """Append a run record to the history table."""
    sql = """
        INSERT INTO run_history
            (started_at, ended_at, events_fetched, events_skipped, aggregate_rows, status)
        VALUES
            (:started_at, :ended_at, :events_fetched, :events_skipped, :aggregate_rows, :status)
    """
    with get_connection(db_path) as conn:
        conn.execute(sql, record)
    logger.info("Run history recorded: %s", record["status"])


def get_run_history(db_path: str | Path, limit: int = 20) -> list[dict]:
    """Return the most recent run records, newest first."""
    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM run_history ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_events_for_date(date_str: str, db_path: str | Path) -> list[dict]:
    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM events WHERE date = ? ORDER BY time_ms", (date_str,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_aggregates(db_path: str | Path) -> list[dict]:
    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM daily_aggregates ORDER BY date, mag_bucket"
        ).fetchall()
    return [dict(r) for r in rows]


def get_checkpoint(db_path: str | Path) -> str | None:
    """Return the ISO timestamp of the last successfully fetched event, or None."""
    with get_connection(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM run_state WHERE key = 'last_fetched_time'"
        ).fetchone()
    return row["value"] if row else None


def set_checkpoint(timestamp: str, db_path: str | Path) -> None:
    """Persist the latest event timestamp after a successful run."""
    with get_connection(db_path) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO run_state (key, value) VALUES ('last_fetched_time', ?)",
            (timestamp,),
        )
    logger.info("Checkpoint saved: %s", timestamp)
