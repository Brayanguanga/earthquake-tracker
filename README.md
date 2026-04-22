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
# Default: last 30 days (or from last checkpoint), writes to earthquakes.db
python main.py

# Custom window and database
python main.py --start 2024-01-01 --end 2024-01-31 --db my.db

# Only fetch significant earthquakes
python main.py --min-magnitude 2.5

# Check when the pipeline last ran and whether it is stale
python main.py --status

# Print the daily aggregate table to the terminal
python main.py --report

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

## Scheduling (cron)

To run daily at 6 AM and append output to a log file:

```cron
0 6 * * * cd /path/to/earthquake-tracker && .venv/bin/python main.py >> logs/pipeline.log 2>&1
```

Use `--status` in a separate health-check cron to alert if the pipeline hasn't run recently:

```cron
0 8 * * * cd /path/to/earthquake-tracker && .venv/bin/python main.py --status || echo "Earthquake pipeline is stale" | mail -s "ALERT" you@example.com
```

## Design Decisions

**Pagination via offset/limit** — The USGS API caps a single response at 20,000 events but the 30-day global catalogue can exceed that. A cheap `/count` call first tells the loop exactly when to stop, then pages through with `limit=1000` and a 1-based offset.

**Checkpoint / incremental fetch** — After each successful run the latest event timestamp is saved to a `run_state` table in SQLite. On the next run, if no explicit `--start` is given, the pipeline resumes from that exact timestamp instead of re-fetching the full 30-day window. Any overlap is silently handled by `INSERT OR REPLACE` on the event ID primary key.

**Batch-flush writes** — Events are accumulated in memory and flushed to SQLite in 500-row batches rather than one row at a time (too slow) or all at once (too much RAM for large windows).

**`INSERT OR REPLACE`** — Reruns are safe: a second run over the same window updates existing rows instead of duplicating them. Aggregate rows get the same treatment, so rerunning after new data arrives is idempotent.

**Retry with back-off** — The pipeline runs as a scheduled job. Transient 5xx errors or timeouts retry up to three times with linear back-off. Unrecoverable 4xx errors fail fast.

**Structured logging** — Every log line includes the module name and a timestamp. Progress is logged at INFO (good for cron output), wire-level detail at DEBUG. The top-level handler is configured once in `main.py`; library code only calls `getLogger(__name__)`.

**SQLite over Postgres** — Zero-config and the WAL journal mode gives safe concurrent reads. If this moved to a service with multiple writers, the swap to Postgres would only touch `db.py`.

## What I'd Add With More Time

- A `--dry-run` flag: fetch and parse but don't write to DB — safe way to test against the live API
- Prometheus metrics (events fetched, DB write latency, last successful run timestamp)
- Docker + cron setup for containerised deployment
