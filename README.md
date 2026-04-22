# Earthquake Tracker

Fetches USGS earthquake data for the past 30 days, stores raw events and daily magnitude-bucket aggregates in SQLite.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
# Default: last 30 days, writes to earthquakes.db
python main.py

# Custom window and database
python main.py --start 2024-01-01 --end 2024-01-31 --db my.db

# Verbose output for debugging
python main.py --log-level DEBUG
```

## Run Tests

```bash
pytest                        # all tests
pytest -v                     # verbose
pytest --cov=earthquake_tracker --cov-report=term-missing  # with coverage
```

No live API connection required — all HTTP calls are mocked.

## Design Decisions

**Pagination via offset/limit** — The USGS API caps a single response at 20 000 events but the 30-day global catalogue can exceed that. I use a cheap `/count` call first so the loop knows when to stop, then pages through with `limit=1000` and a 1-based offset. This means I never miss events and I log meaningful progress.

**Batch-flush writes** — Events are accumulated in memory and flushed to SQLite in 500-row batches rather than one row at a time (too slow) or all at once (too much RAM for large windows). The flush also happens for the remainder after the loop, so nothing is lost if the total count isn't a multiple of 500.

**`INSERT OR REPLACE`** — Reruns are safe: a second run over the same window updates existing rows instead of duplicating them. Aggregate rows get the same treatment, so rerunning after new data arrives is idempotent.

**Retry with back-off** — The pipeline runs as a scheduled job. Transient 5xx errors or timeouts shouldn't page anyone at 3 am. Three retries with linear back-off handle the common case; unrecoverable 4xx errors fail fast.

**Structured logging** — Every log line includes the module name and a timestamp. Progress is logged at INFO (good for cron output), wire-level detail at DEBUG (useful when something goes wrong). The top-level handler is configured once in `main.py`; library code only calls `getLogger(__name__)`.

**SQLite over Postgres** — The task says local run, SQLite is zero-config and the WAL journal mode gives safe concurrent reads. If this moved to a service with multiple writers, the swap to Postgres would only touch `db.py`.

## What I'd Add With More Time

- CLI flag to print the aggregate table to stdout (quick sanity check without SQLite CLI)
- A `--since-last-run` flag that reads the latest stored event time from the DB so reruns only fetch new data
- Prometheus metrics (events fetched, DB write latency, last successful run timestamp) for production monitoring
- A `Makefile` target that runs the job in a Docker container so there's no Python version ambiguity in CI
