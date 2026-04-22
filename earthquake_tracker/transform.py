"""Transform raw earthquake events into daily magnitude-bucket aggregates."""

from __future__ import annotations

from collections import defaultdict


BUCKETS = [
    ("0-2", lambda m: m is not None and m < 2),
    ("2-4", lambda m: m is not None and 2 <= m < 4),
    ("4-6", lambda m: m is not None and 4 <= m < 6),
    ("6+",  lambda m: m is not None and m >= 6),
    ("unknown", lambda m: m is None),
]


def magnitude_bucket(mag: float | None) -> str:
    for label, predicate in BUCKETS:
        if predicate(mag):
            return label
    return "unknown"


def compute_daily_aggregates(events: list[dict]) -> list[dict]:
    """
    Given a list of parsed event dicts, return daily aggregate rows.

    Uses the pre-computed mag_bucket field if present, otherwise falls
    back to deriving it from magnitude.

    Each row: {"date": "YYYY-MM-DD", "mag_bucket": str, "count": int}
    """
    counts: dict[tuple[str, str], int] = defaultdict(int)

    for event in events:
        date = event.get("date")
        if not date:
            continue
        bucket = event.get("mag_bucket") or magnitude_bucket(event.get("magnitude"))
        counts[(date, bucket)] += 1

    return [
        {"date": date, "mag_bucket": bucket, "count": cnt}
        for (date, bucket), cnt in sorted(counts.items())
    ]
