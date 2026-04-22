# Earthquake Tracker

Fetches global earthquake data from the USGS API, stores raw events and daily magnitude-bucket aggregates in SQLite, and provides a CLI for querying results and monitoring pipeline health.

## Features

- Fetches all USGS earthquakes for a configurable time window (default: 30 days)
- Handles API pagination automatically — no event limit
- Classifies events into magnitude buckets: `0-2`, `2-4`, `4-6`, `6+`, `unknown`
- Stores raw events and daily aggregates in SQLite
- Incremental runs via checkpoint — only fetches new data since the last run
- Full run history with start/end times, event counts, and success/fail status
- Structured logging designed for scheduled job debugging
- 56 tests — no live API connection required

---

## Setup

```bash
git clone https://github.com/Brayanguanga/earthquake-tracker
cd earthquake-tracker

python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

---

## Usage

### Fetch earthquake data

```bash
# Default: resumes from last checkpoint, or fetches 30 days if no checkpoint exists
python main.py

# Custom date range
python main.py --start 2024-01-01 --end 2024-01-31

# Use a different database file
python main.py --db my_data.db

# Only fetch earthquakes at or above magnitude 2.5
python main.py --min-magnitude 2.5

# Verbose output — useful when debugging at 3am
python main.py --log-level DEBUG
```

### View results

```bash
# Daily counts by magnitude bucket
python main.py --report

# Sample output:
# Date               0-2       2-4       4-6        6+   unknown     Total
# ------------------------------------------------------------------------
# 2026-04-19         339        97        27         1         0       464
# 2026-04-20         232        77        22         1         0       332
```

### Monitor pipeline health

```bash
# Show when the pipeline last ran and whether it is stale (>25h since last run)
python main.py --status

# Show recent run history
python main.py --history
```

---

## Run Tests

```bash
pytest              # all 56 tests
pytest -v           # verbose output
pytest --cov=earthquake_tracker --cov-report=term-missing   # with coverage
```

No live API connection required — all HTTP calls are mocked with `unittest.mock`.

---

## Scheduling (cron)

Run daily at 6 AM, append logs to file:

```cron
0 6 * * * cd /path/to/earthquake-tracker && .venv/bin/python main.py >> logs/pipeline.log 2>&1
```

Alert if the pipeline hasn't run in over 25 hours:

```cron
0 8 * * * cd /path/to/earthquake-tracker && .venv/bin/python main.py --status || echo "Earthquake pipeline is stale" | mail -s "ALERT" you@example.com
```

---

## Project Structure

```
earthquake_tracker/
├── api.py          # USGS API client — pagination, retry, event parsing
├── db.py           # SQLite layer — schema, migrations, reads/writes
├── transform.py    # Magnitude bucketing and daily aggregate computation
└── pipeline.py     # Orchestration — wires fetch → enrich → store → aggregate

tests/
├── fixtures.py         # Shared GeoJSON feature stubs
├── test_api.py         # API client: pagination, retry, parse edge cases
├── test_db.py          # DB layer: upserts, conflicts, migrations, run history
├── test_transform.py   # Bucket boundaries and aggregate computation
└── test_pipeline.py    # Full pipeline integration, checkpoint, status, report

main.py             # CLI entry point
requirements.txt    # requests, pytest, pytest-cov
```

---

## Database Schema

```sql
-- One row per earthquake event
events (
    id          TEXT PRIMARY KEY,   -- USGS event ID
    time_ms     INTEGER NOT NULL,   -- epoch milliseconds (USGS native format)
    date        TEXT NOT NULL,      -- YYYY-MM-DD UTC, used for grouping
    magnitude   REAL,
    mag_bucket  TEXT NOT NULL,      -- pre-computed: 0-2 / 2-4 / 4-6 / 6+ / unknown
    place       TEXT,
    longitude   REAL,
    latitude    REAL,
    depth_km    REAL,
    event_type  TEXT,
    status      TEXT,
    url         TEXT
)

-- Daily counts per magnitude bucket
daily_aggregates (
    date        TEXT NOT NULL,
    mag_bucket  TEXT NOT NULL,
    count       INTEGER NOT NULL,
    PRIMARY KEY (date, mag_bucket)
)

-- Incremental fetch checkpoint
run_state (
    key   TEXT PRIMARY KEY,   -- 'last_fetched_time'
    value TEXT NOT NULL       -- ISO 8601 UTC timestamp
)

-- Full run log
run_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL,
    ended_at        TEXT NOT NULL,
    events_fetched  INTEGER NOT NULL,
    events_skipped  INTEGER NOT NULL,
    aggregate_rows  INTEGER NOT NULL,
    status          TEXT NOT NULL    -- 'success' or 'failed'
)
```

Indexes: `events(date)`, `events(magnitude)`, `events(date, mag_bucket)`

---

## Design Decisions

**Pagination via `/count` + offset** — A cheap `/count` call runs first to know exactly how many events exist in the window. The fetch loop then pages with `limit=1000` and a 1-based offset, stopping as soon as all events are retrieved. The `orderby=time-asc` parameter ensures consistent ordering so offset pagination never skips or duplicates events.

**Incremental checkpoint** — After each successful run the latest event's `time_ms` is converted to an ISO timestamp and saved to `run_state`. The next run resumes from that exact timestamp rather than re-fetching the full window. Any overlap from the boundary is handled silently by `INSERT OR REPLACE` keyed on the USGS event ID.

**`time_ms` as INTEGER** — USGS delivers timestamps as epoch milliseconds. Storing them as INTEGER preserves the native format, takes less space than ISO strings, and makes range comparisons faster. The `date` column (TEXT, `YYYY-MM-DD`) is derived at parse time and kept separately for grouping.

**Pre-computed `mag_bucket`** — The bucket is assigned once at insert time and stored on the `events` row. This means aggregate queries can `GROUP BY mag_bucket` directly in SQL rather than loading all events into Python. A composite index on `(date, mag_bucket)` supports those queries efficiently.

**Batch-flush writes** — Events are accumulated in 500-row batches and flushed with `executemany`. This balances memory usage against write frequency — row-by-row is too slow for 10k+ events, all-at-once uses too much RAM.

**`INSERT OR REPLACE`** — All upserts use this strategy so reruns over the same window are fully idempotent. No deduplication logic needed elsewhere.

**Retry with back-off** — Every HTTP request goes through a shared `_get()` helper that retries up to 3 times with linear back-off (2s, 4s). 4xx errors (bad params) fail immediately — retrying won't help. 5xx errors and timeouts retry.

**Run history table** — Every run, success or failure, is logged to `run_history` with timestamps and counts. This directly answers "how do I know if the pipeline has been running?" without checking log files.

**Structured logging** — Log lines include timestamp, level, and module name. INFO captures progress at a cron-friendly level of detail. DEBUG adds per-request and per-batch traces. The root handler is configured once in `main.py`; all library code uses `getLogger(__name__)`.

**SQLite over Postgres** — Zero setup, WAL mode allows safe concurrent reads. Swapping to Postgres would only touch `db.py` — everything else uses plain dicts.

---

## What I'd Add With More Time

- **Prometheus metrics** — events fetched per run, DB write latency, and hours since last successful run. The `run_history` table already captures the raw data; exposing it as metrics would make the pipeline observable without reading log files.
- **Timezone-aware date bucketing** — events near midnight UTC currently land on the UTC date, which may differ from the local date in the affected region. Bucketing by the event's local timezone would be more meaningful for regional analysis.
