#!/usr/bin/env python3
"""
CLI entry point for the earthquake tracker.

Commands:
    python main.py                        fetch new events (incremental)
    python main.py --start DATE           fetch from a specific date
    python main.py --end DATE             fetch up to a specific date
    python main.py --min-magnitude FLOAT  filter by minimum magnitude
    python main.py --db PATH              use a custom SQLite file
    python main.py --report               print daily aggregate table
    python main.py --status               show checkpoint age / staleness
    python main.py --history              show recent run log
    python main.py --log-level DEBUG      verbose output for debugging
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from earthquake_tracker.pipeline import run, status, report, history


def setup_logging(level: str) -> None:
    fmt = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt=datefmt,
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and store USGS earthquake data.")
    parser.add_argument("--start", type=date.fromisoformat, default=None,
                        help="Start date YYYY-MM-DD (default: 30 days ago or last checkpoint)")
    parser.add_argument("--end", type=date.fromisoformat, default=None,
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--db", type=Path, default=Path("earthquakes.db"),
                        help="SQLite database path (default: earthquakes.db)")
    parser.add_argument("--min-magnitude", type=float, default=None,
                        help="Only fetch events at or above this magnitude")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--status", action="store_true",
                        help="Show last run checkpoint and staleness, then exit")
    parser.add_argument("--report", action="store_true",
                        help="Print daily aggregate table, then exit")
    parser.add_argument("--history", action="store_true",
                        help="Print recent run history, then exit")
    return parser.parse_args()


def print_report(rows: list[dict]) -> None:
    if not rows:
        print("No aggregates found. Run the pipeline first.")
        return

    # Group by date for a readable table
    from collections import defaultdict
    by_date: dict = defaultdict(dict)
    for row in rows:
        by_date[row["date"]][row["mag_bucket"]] = row["count"]

    buckets = ["0-2", "2-4", "4-6", "6+", "unknown"]
    header = f"{'Date':<12}" + "".join(f"{b:>10}" for b in buckets) + f"{'Total':>10}"
    print(header)
    print("-" * len(header))
    for day, counts in sorted(by_date.items()):
        total = sum(counts.values())
        row_str = f"{day:<12}" + "".join(f"{counts.get(b, 0):>10}" for b in buckets) + f"{total:>10}"
        print(row_str)


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)

    if args.status:
        result = status(db_path=args.db)
        for key, val in result.items():
            print(f"  {key}: {val}")
        sys.exit(1 if result.get("stale") else 0)

    if args.report:
        rows = report(db_path=args.db)
        print_report(rows)
        return

    if args.history:
        rows = history(db_path=args.db)
        if not rows:
            print("No run history yet.")
            return
        print(f"\n{'#':<5} {'Started':<28} {'Ended':<28} {'Fetched':>8} {'Skipped':>8} {'Status'}")
        print("-" * 90)
        for r in rows:
            print(f"{r['id']:<5} {r['started_at']:<28} {r['ended_at']:<28} {r['events_fetched']:>8} {r['events_skipped']:>8} {r['status']}")
        return

    logger.info("Starting earthquake tracker")
    try:
        summary = run(
            start=args.start,
            end=args.end,
            db_path=args.db,
            min_magnitude=args.min_magnitude,
        )
        print("\nSummary:")
        for key, val in summary.items():
            print(f"  {key}: {val}")
    except Exception as exc:
        logger.critical("Pipeline failed: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
