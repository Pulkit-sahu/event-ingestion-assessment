# Architecture notes

This service accepts activity events, stores each `event_id` once, and serves both the raw rows and precomputed minute/hour counts. Flask handles HTTP. SQLite stores the data. A separate async worker updates the counts.

## Two tables

`events` is the source of truth. `event_id` is the primary key, so the database itself rejects a second copy. The other columns are `tenant_id`, `source`, `event_type`, a normalized UTC `timestamp`, a JSON `payload`, a server `created_at`, and an `aggregated` flag.

`aggregates` stores counts. One row is one tenant, one bucket size (`minute` or `hour`), one bucket start, one source, and one event type. It keeps `count`, `first_seen`, and `last_seen`. The primary key is those five identity columns, which also matches the metrics lookup (`tenant_id`, `bucket_size`, then a range on `bucket_start`).

Event reads use three indexes: tenant plus time, tenant plus source plus time, and tenant plus event type plus time. A partial index covers only rows that still need aggregation.

## Idempotency and concurrent writes

A single insert starts with `BEGIN IMMEDIATE`, which takes SQLite’s write lock before the code looks for the id.

- Missing id: insert it and return `201 created`.
- Same id and same content: return `200 duplicate`.
- Same id and different content: return `409 conflict` and leave the original row alone.

Two requests that arrive together cannot both pass the “is it missing?” check, because the second waits until the first commits and then sees the row. Bulk ingestion does the same check for the whole batch inside one transaction, then uses `executemany`. If any id in the batch already exists with different content, the transaction rolls back and nothing from that batch is saved. Identical repeats are skipped and counted as duplicates.

SQLite still allows only one writer at a time. That is acceptable for this exercise. The same shape — a unique key, a transaction, and `ON CONFLICT` — is how this would move to PostgreSQL. Connections set WAL mode and a busy timeout so readers can run while a writer is busy, and a short retry covers a lock that outlasts the timeout.

## Aggregation

`GET /metrics` reads only the `aggregates` table. It groups with SQL (`SUM`, `MIN`, `MAX`) and does not load events into Python.

The worker in `app/worker.py` is the piece that fills that table. Each pass, inside one write transaction:

1. Copy up to 500 pending event ids into a temporary table, oldest first.
2. `INSERT ... SELECT ... GROUP BY` with `strftime` to build the minute buckets and the hour buckets.
3. On conflict, add the new count and widen `first_seen` / `last_seen`.
4. Set `aggregated = 1` on that batch.
5. Commit.

If the process dies before commit, the flag and the counts roll back together, so the next run tries the same events again instead of counting them twice. After a successful commit, those events are no longer pending. Running the worker twice does not change the totals. `GET /metrics` can be empty or behind for a moment; it becomes correct after the worker catches up.

Time windows on raw events are half-open: `from <= timestamp < to`. Metrics windows are widened to whole buckets, because a stored hour count cannot be split in half. The response echoes the aligned `from` and `to`.

## Async worker

Request handlers stay ordinary Flask functions. They do a short, explicit transaction and return. The long-running path is the worker. It uses `asyncio` and `aiosqlite`: each statement is awaited, and when a batch is smaller than the limit the loop sleeps instead of spinning. That keeps the worker’s event loop free between writes. Calling `asyncio.run` inside a Flask view would not make the web server concurrent, so the async code lives where it actually waits on the database.

## Performance

Batching and indexes do the work. A local run of `python -m app.benchmark` on 5,000 small events in one transaction measured:

- insert: 0.021 seconds (about 240,000 rows/s)
- aggregate those 5,000 rows: 0.031 seconds
- metrics read of 336 buckets: 0.0007 seconds
- recounting all 5,000 events in Python: 0.0027 seconds

At this size the Python recount is already slower, and it gets slower as the events table grows. The metrics query stays on the compact aggregates table. `EXPLAIN QUERY PLAN` for the event list uses `idx_events_tenant_time` as a covering index (`tenant_id` equality, then the timestamp range). Bulk requests are also rejected before insert when the body is over 8 MB or the batch is over 5,000 events.

## Abuse controls

- Flask rejects bodies over 8 MB with `413`.
- Each payload must be a JSON object of at most 16 KB.
- Timestamps must include a timezone. Offsets are converted to UTC and stored as `YYYY-MM-DDTHH:MM:SS.ffffffZ`, so text order and time order match. Naive timestamps, times before 2000, and times more than five minutes ahead are rejected.
- A per-process sliding window limits single posts and bulk posts separately (120/minute and 12/minute by client address). The limiter lives in memory, so it protects one process. More than one process needs a shared store such as Redis. `429` responses include `Retry-After`.

## Tests

Pytest covers a repeated event, a conflicting repeat, bulk validation and the 5,000 limit, bucket totals (including a second worker run), writers racing the aggregator, half-open time windows, hour and minute boundaries, cursor pages, UTC conversion, and the rate limit.
