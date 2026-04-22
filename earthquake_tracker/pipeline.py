"""Orchestrates fetch → parse → store → aggregate pipeline."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Union
from pathlib import Path

import requests

from .api import fetch_events, parse_event
from .db import init_db, upsert_events, upsert_aggregates, get_checkpoint, set_checkpoint, get_aggregates
from .transform import compute_daily_aggregates

logger = logging.getLogger(__name__)

DEFAULT_DB = Path("earthquakes.db")
BATCH_SIZE = 500  # rows to accumulate before flushing to DB


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


def run(
    start: Union[date, datetime, None] = None,
    end: Union[date, datetime, None] = None,
    db_path: Path = DEFAULT_DB,
    min_magnitude: float | None = None,
) -> dict:
    """
    Full pipeline: fetch 30-day window, store raw events, compute and store aggregates.

    Returns a summary dict for logging/monitoring.
    """
    end = end or date.today()

    if start is None:
        init_db(db_path)
        checkpoint = get_checkpoint(db_path)
        if checkpoint:
            # Resume from the exact checkpoint timestamp — duplicates are handled
            # by INSERT OR REPLACE in the DB using the event ID as primary key
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
    latest_time: str | None = None

    try:
        for feature in fetch_events(start, end, session, min_magnitude=min_magnitude):
            event = parse_event(feature)
            if event is None:
                total_skipped += 1
                continue

            batch.append(event)
            all_events.append(event)
            total_fetched += 1
            if latest_time is None or event["time"] > latest_time:
                latest_time = event["time"]

            if len(batch) >= BATCH_SIZE:
                logger.debug("Flushing batch of %d events to DB", len(batch))
                upsert_events(batch, db_path)
                batch.clear()

        # flush remainder
        if batch:
            upsert_events(batch, db_path)

    except Exception as exc:
        logger.error("Pipeline failed during fetch: %s", exc, exc_info=True)
        raise

    logger.info("Computing daily aggregates from %d events …", len(all_events))
    aggregates = compute_daily_aggregates(all_events)
    upsert_aggregates(aggregates, db_path)

    if latest_time:
        set_checkpoint(latest_time, db_path)

    summary = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "events_fetched": total_fetched,
        "events_skipped": total_skipped,
        "aggregate_rows": len(aggregates),
        "db_path": str(db_path),
    }
    logger.info("Pipeline complete: %s", summary)
    return summary
