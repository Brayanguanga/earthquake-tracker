"""Orchestrates fetch → parse → store → aggregate pipeline."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import requests

from .api import fetch_events, parse_event
from .db import init_db, upsert_events, upsert_aggregates
from .transform import compute_daily_aggregates

logger = logging.getLogger(__name__)

DEFAULT_DB = Path("earthquakes.db")
BATCH_SIZE = 500  # rows to accumulate before flushing to DB


def run(
    start: date | None = None,
    end: date | None = None,
    db_path: Path = DEFAULT_DB,
) -> dict:
    """
    Full pipeline: fetch 30-day window, store raw events, compute and store aggregates.

    Returns a summary dict for logging/monitoring.
    """
    end = end or date.today()
    start = start or (end - timedelta(days=30))

    logger.info("Pipeline starting: %s → %s, db=%s", start, end, db_path)

    init_db(db_path)

    session = requests.Session()
    session.headers.update({"User-Agent": "earthquake-tracker/1.0 (brayanguanga@gmail.com)"})

    total_fetched = 0
    total_skipped = 0
    all_events: list[dict] = []
    batch: list[dict] = []

    try:
        for feature in fetch_events(start, end, session):
            event = parse_event(feature)
            if event is None:
                total_skipped += 1
                continue

            batch.append(event)
            all_events.append(event)
            total_fetched += 1

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
