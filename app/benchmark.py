"""Small local benchmark. Not part of the request path.

Run: python -m app.benchmark
"""

import asyncio
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.aggregate import aggregate_all
from app.db import connect, init_db
from app.ingest import insert_bulk
from app.queries import list_metrics
from app.validation import format_timestamp, parse_bulk

COUNT = 5000
UTC = timezone.utc


def build_events(count):
    origin = datetime(2024, 6, 1, 10, 0, tzinfo=UTC)
    raw = []
    for index in range(count):
        moment = origin + timedelta(seconds=index)
        raw.append(
            {
                "event_id": f"bench-{index}",
                "tenant_id": "bench",
                "source": "web" if index % 2 == 0 else "mobile",
                "event_type": "click" if index % 3 else "view",
                "timestamp": format_timestamp(moment),
                "payload": {"n": index},
            }
        )
    events, _duplicates = parse_bulk(raw, max_events=count, max_payload_bytes=16 * 1024, skew_minutes=5)
    return events


def naive_minute_counts(db_path):
    connection = connect(db_path)
    try:
        rows = connection.execute("SELECT timestamp, source, event_type FROM events").fetchall()
    finally:
        connection.close()
    counts = {}
    for row in rows:
        minute = row["timestamp"][:17] + "00.000000Z"
        key = (minute, row["source"], row["event_type"])
        counts[key] = counts.get(key, 0) + 1
    return len(counts)


def explain(db_path):
    connection = connect(db_path)
    try:
        plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT event_id FROM events
            WHERE tenant_id = ? AND timestamp >= ? AND timestamp < ?
            ORDER BY timestamp ASC, event_id ASC
            LIMIT 51
            """,
            ("bench", "2024-06-01T10:00:00.000000Z", "2024-06-02T00:00:00.000000Z"),
        ).fetchall()
    finally:
        connection.close()
    return [tuple(row) for row in plan]


def main():
    with tempfile.TemporaryDirectory() as directory:
        db_path = str(Path(directory) / "bench.db")
        init_db(db_path)
        events = build_events(COUNT)

        started = time.perf_counter()
        result = insert_bulk(db_path, events)
        insert_seconds = time.perf_counter() - started

        started = time.perf_counter()
        processed = asyncio.run(aggregate_all(db_path, batch_size=500))
        aggregate_seconds = time.perf_counter() - started

        started = time.perf_counter()
        report = list_metrics(
            db_path,
            tenant_id="bench",
            bucket_size="minute",
            group_by={"source", "event_type"},
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )
        metrics_seconds = time.perf_counter() - started

        started = time.perf_counter()
        naive_groups = naive_minute_counts(db_path)
        naive_seconds = time.perf_counter() - started

        print(f"inserted {result.inserted} events in {insert_seconds:.3f}s ({result.inserted / insert_seconds:.0f} rows/s)")
        print(f"aggregated {processed} events in {aggregate_seconds:.3f}s")
        print(f"metrics query returned {len(report['buckets'])} buckets in {metrics_seconds:.4f}s")
        print(f"naive full-table python count built {naive_groups} groups in {naive_seconds:.4f}s")
        print("query plan for GET /events:")
        for row in explain(db_path):
            print(" ", row)


if __name__ == "__main__":
    main()
