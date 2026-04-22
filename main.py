#!/usr/bin/env python3
"""Entry point — configure logging then run the pipeline."""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from earthquake_tracker.pipeline import run


def setup_logging(level: str) -> None:
    fmt = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )
    # Quiet noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and store USGS earthquake data.")
    parser.add_argument("--start", type=date.fromisoformat, default=None,
                        help="Start date YYYY-MM-DD (default: 30 days ago)")
    parser.add_argument("--end", type=date.fromisoformat, default=None,
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--db", type=Path, default=Path("earthquakes.db"),
                        help="SQLite database path (default: earthquakes.db)")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    logger = logging.getLogger(__name__)
    logger.info("Starting earthquake tracker")

    try:
        summary = run(start=args.start, end=args.end, db_path=args.db)
        print("\nSummary:")
        for key, val in summary.items():
            print(f"  {key}: {val}")
    except Exception as exc:
        logger.critical("Pipeline failed: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
