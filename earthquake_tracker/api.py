"""USGS Earthquake API client with pagination support."""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone
from typing import Iterator, Union

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1"
PAGE_SIZE = 1000
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0  # seconds


def _get(url: str, params: dict, session: requests.Session) -> dict:
    """GET with retry/backoff. Raises on unrecoverable errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.debug("GET %s params=%s (attempt %d)", url, params, attempt)
            resp = session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            logger.warning("Request timed out (attempt %d/%d)", attempt, MAX_RETRIES)
        except requests.exceptions.HTTPError as exc:
            # 400 bad request won't fix itself on retry
            if exc.response is not None and exc.response.status_code < 500:
                logger.error("Client error %s — not retrying", exc.response.status_code)
                raise
            logger.warning("Server error %s (attempt %d/%d)", exc, attempt, MAX_RETRIES)
        except requests.exceptions.RequestException as exc:
            logger.warning("Request failed: %s (attempt %d/%d)", exc, attempt, MAX_RETRIES)

        if attempt < MAX_RETRIES:
            sleep = RETRY_BACKOFF * attempt
            logger.info("Retrying in %.1f s …", sleep)
            time.sleep(sleep)

    raise RuntimeError(f"All {MAX_RETRIES} attempts failed for {url}")


def count_events(
    start: Union[date, datetime],
    end: Union[date, datetime],
    session: requests.Session | None = None,
    min_magnitude: float | None = None,
) -> int:
    """Return total event count for the time window (cheap HEAD-style call)."""
    session = session or requests.Session()
    params: dict = {"format": "geojson", "starttime": start.isoformat(), "endtime": end.isoformat()}
    if min_magnitude is not None:
        params["minmagnitude"] = min_magnitude
    data = _get(f"{BASE_URL}/count", params, session)
    total = data.get("count", 0)
    logger.info("API reports %d events between %s and %s", total, start, end)
    return total


def fetch_events(
    start: Union[date, datetime],
    end: Union[date, datetime],
    session: requests.Session | None = None,
    min_magnitude: float | None = None,
) -> Iterator[dict]:
    """
    Yield individual earthquake feature dicts for the given window.

    Handles pagination automatically. Each yielded dict is a GeoJSON
    Feature with at least: id, properties (mag, place, time), geometry.
    """
    session = session or requests.Session()
    total = count_events(start, end, session, min_magnitude=min_magnitude)
    if total == 0:
        logger.info("No events found — nothing to fetch")
        return

    fetched = 0
    offset = 1  # USGS uses 1-based offset

    while fetched < total:
        params: dict = {
            "format": "geojson",
            "starttime": start.isoformat(),
            "endtime": end.isoformat(),
            "orderby": "time-asc",
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        if min_magnitude is not None:
            params["minmagnitude"] = min_magnitude
        logger.info(
            "Fetching page offset=%d limit=%d (fetched %d/%d)",
            offset, PAGE_SIZE, fetched, total,
        )
        data = _get(f"{BASE_URL}/query", params, session)
        features = data.get("features", [])

        if not features:
            logger.warning("Empty page at offset=%d — stopping pagination", offset)
            break

        for feature in features:
            yield feature

        fetched += len(features)
        offset += len(features)
        logger.debug("Progress: %d/%d events fetched", fetched, total)

    logger.info("Fetch complete: %d events retrieved", fetched)


def parse_event(feature: dict) -> dict | None:
    """
    Extract a flat dict from a GeoJSON feature.
    Returns None and logs a warning if the feature is malformed.
    """
    try:
        props = feature["properties"]
        geo = feature.get("geometry") or {}
        coords = geo.get("coordinates") or [None, None, None]

        mag = props.get("mag")
        time_ms = props.get("time")
        if time_ms is None:
            logger.warning("Event %s missing time — skipping", feature.get("id"))
            return None

        event_time = datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc)

        return {
            "id": feature["id"],
            "time": event_time.isoformat(),
            "date": event_time.date().isoformat(),
            "magnitude": mag,
            "place": props.get("place"),
            "longitude": coords[0],
            "latitude": coords[1],
            "depth_km": coords[2],
            "event_type": props.get("type"),
            "status": props.get("status"),
            "url": props.get("url"),
        }
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        logger.warning("Could not parse event %s: %s", feature.get("id"), exc)
        return None
