import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    aggregated INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_events_tenant_time
    ON events (tenant_id, timestamp, event_id);

CREATE INDEX IF NOT EXISTS idx_events_tenant_source_time
    ON events (tenant_id, source, timestamp, event_id);

CREATE INDEX IF NOT EXISTS idx_events_tenant_type_time
    ON events (tenant_id, event_type, timestamp, event_id);

CREATE INDEX IF NOT EXISTS idx_events_pending
    ON events (timestamp, event_id)
    WHERE aggregated = 0;

CREATE TABLE IF NOT EXISTS aggregates (
    tenant_id TEXT NOT NULL,
    bucket_size TEXT NOT NULL,
    bucket_start TEXT NOT NULL,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    count INTEGER NOT NULL,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    PRIMARY KEY (tenant_id, bucket_size, bucket_start, source, event_type)
);
"""

MINUTE_BUCKET = "strftime('%Y-%m-%dT%H:%M:00.000000Z', e.timestamp)"
HOUR_BUCKET = "strftime('%Y-%m-%dT%H:00:00.000000Z', e.timestamp)"


def upsert_sql(bucket_expr):
    return f"""
    INSERT INTO aggregates (
        tenant_id, bucket_start, bucket_size, source, event_type,
        count, first_seen, last_seen
    )
    SELECT
        e.tenant_id,
        {bucket_expr},
        ?,
        e.source,
        e.event_type,
        COUNT(*),
        MIN(e.timestamp),
        MAX(e.timestamp)
    FROM events AS e
    INNER JOIN agg_batch AS b ON b.event_id = e.event_id
    GROUP BY e.tenant_id, {bucket_expr}, e.source, e.event_type
    ON CONFLICT (tenant_id, bucket_size, bucket_start, source, event_type)
    DO UPDATE SET
        count = aggregates.count + excluded.count,
        first_seen = MIN(aggregates.first_seen, excluded.first_seen),
        last_seen = MAX(aggregates.last_seen, excluded.last_seen)
    """


MINUTE_UPSERT = upsert_sql(MINUTE_BUCKET)
HOUR_UPSERT = upsert_sql(HOUR_BUCKET)


def connect(path):
    connection = sqlite3.connect(path, timeout=5.0, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA temp_store=MEMORY")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=5000")
    return connection


def init_db(path):
    connection = connect(path)
    try:
        connection.executescript(SCHEMA)
    finally:
        connection.close()


def retry_locked(operation):
    last_error = None
    for attempt in range(5):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            last_error = exc
            if "locked" not in str(exc).lower() or attempt == 4:
                raise
            time.sleep(0.02 * (2 ** attempt))
    raise last_error


def rollback(connection):
    try:
        connection.execute("ROLLBACK")
    except sqlite3.Error:
        pass
