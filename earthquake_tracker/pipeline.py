"""Orchestrates fetch → parse → store → aggregate pipeline."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Union
from pathlib import Path

import requests

from .api import fetch_events, parse_event
from .db import (
    init_db, upsert_events, upsert_aggregates,
    get_checkpoint, set_checkpoint,
    get_aggregates, add_run_history, get_run_history,
)
from .transform import compute_daily_aggregates, magnitude_bucket

logger = logging.getLogger(__name__)

DEFAULT_DB = Path("earthquakes.db")
BATCH_SIZE = 500


def status(db_path: Path = DEFAULT_DB, stale_hours: int = 25) -> dict:
    """
    Report the last run checkpoint and whether it is stale.

    stale_hours: warn if the checkpoint is older than this many hours (default 25,
    giving a daily job a 1-hour grace window before it is flagged).
    """
    init_db(db_path)
    checkpoint = get_checkpoint(db_path)

    if checkpoint is None:
        return {"checkpoint": None, "stale": None, "message": "No runs recorded yet."}

    checkpoint_dt = datetime.fromisoformat(checkpoint)
    age = datetime.now(tz=checkpoint_dt.tzinfo) - checkpoint_dt
    hours_old = age.total_seconds() / 3600
    is_stale = hours_old > stale_hours

    return {
        "checkpoint": checkpoint,
        "hours_since_last_run": round(hours_old, 1),
        "stale": is_stale,
        "message": f"{'STALE — last run was' if is_stale else 'OK — last run was'} {hours_old:.1f}h ago",
    }


def report(db_path: Path = DEFAULT_DB) -> list[dict]:
    """Return all daily aggregate rows for display."""
    init_db(db_path)
    return get_aggregates(db_path)


def history(db_path: Path = DEFAULT_DB, limit: int = 20) -> list[dict]:
    """Return recent run history records."""
    init_db(db_path)
    return get_run_history(db_path, limit=limit)


def run(
    start: Union[date, datetime, None] = None,
    end: Union[date, datetime, None] = None,
    db_path: Path = DEFAULT_DB,
    min_magnitude: float | None = None,
) -> dict:
    """
    Full pipeline: fetch window, store raw events, compute and store aggregates.

    Returns a summary dict for logging/monitoring.
    """
    run_started_at = datetime.now(tz=timezone.utc)
    end = end or date.today()

    if start is None:
        init_db(db_path)
        checkpoint = get_checkpoint(db_path)
        if checkpoint:
            start = datetime.fromisoformat(checkpoint)
            logger.info("Resuming from checkpoint: %s", checkpoint)
        else:
            start = end - timedelta(days=30)
            logger.info("No checkpoint found — fetching full 30-day window")

    logger.info("Pipeline starting: %s → %s, db=%s", start, end, db_path)

    init_db(db_path)  # no-op if already initialized

    session = requests.Session()
    session.headers.update({"User-Agent": "earthquake-tracker/1.0 (brayanguanga@gmail.com)"})

    total_fetched = 0
    total_skipped = 0
    all_events: list[dict] = []
    batch: list[dict] = []
    latest_time_ms: int | None = None

    try:
        for feature in fetch_events(start, end, session, min_magnitude=min_magnitude):
            event = parse_event(feature)
            if event is None:
                total_skipped += 1
                continue

            event["mag_bucket"] = magnitude_bucket(event.get("magnitude"))

            batch.append(event)
            all_events.append(event)
            total_fetched += 1

            if latest_time_ms is None or event["time_ms"] > latest_time_ms:
                latest_time_ms = event["time_ms"]

            if len(batch) >= BATCH_SIZE:
                logger.debug("Flushing batch of %d events to DB", len(batch))
                upsert_events(batch, db_path)
                batch.clear()

        if batch:
            upsert_events(batch, db_path)

    except Exception as exc:
        logger.error("Pipeline failed during fetch: %s", exc, exc_info=True)
        add_run_history({
            "started_at": run_started_at.isoformat(),
            "ended_at": datetime.now(tz=timezone.utc).isoformat(),
            "events_fetched": total_fetched,
            "events_skipped": total_skipped,
            "aggregate_rows": 0,
            "status": "failed",
        }, db_path)
        raise

    logger.info("Computing daily aggregates from %d events …", len(all_events))
    aggregates = compute_daily_aggregates(all_events)
    upsert_aggregates(aggregates, db_path)

    if latest_time_ms is not None:
        latest_iso = datetime.fromtimestamp(latest_time_ms / 1000, tz=timezone.utc).isoformat()
        set_checkpoint(latest_iso, db_path)

    summary = {
        "start": start.isoformat(),
        "end": end.isoformat() if hasattr(end, "isoformat") else str(end),
        "events_fetched": total_fetched,
        "events_skipped": total_skipped,
        "aggregate_rows": len(aggregates),
        "db_path": str(db_path),
    }

    add_run_history({
        "started_at": run_started_at.isoformat(),
        "ended_at": datetime.now(tz=timezone.utc).isoformat(),
        "events_fetched": total_fetched,
        "events_skipped": total_skipped,
        "aggregate_rows": len(aggregates),
        "status": "success",
    }, db_path)

    logger.info("Pipeline complete: %s", summary)
    return summary
