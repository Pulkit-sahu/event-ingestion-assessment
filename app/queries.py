import json
from datetime import datetime, timedelta, timezone

from app.db import connect
from app.errors import ValidationError
from app.validation import add_bucket, floor_bucket, format_timestamp, parse_timestamp

UTC = timezone.utc


def list_events(db_path, *, tenant_id, source=None, event_type=None, start=None, end=None, cursor=None, limit=50):
    clauses = ["tenant_id = ?"]
    params = [tenant_id]
    if source:
        clauses.append("source = ?")
        params.append(source)
    if event_type:
        clauses.append("event_type = ?")
        params.append(event_type)
    if start is not None:
        clauses.append("timestamp >= ?")
        params.append(format_timestamp(start))
    if end is not None:
        clauses.append("timestamp < ?")
        params.append(format_timestamp(end))
    if cursor is not None:
        clauses.append("(timestamp > ? OR (timestamp = ? AND event_id > ?))")
        params.extend([cursor[0], cursor[0], cursor[1]])

    sql = f"""
        SELECT event_id, tenant_id, source, event_type, timestamp, payload, created_at
        FROM events
        WHERE {' AND '.join(clauses)}
        ORDER BY timestamp ASC, event_id ASC
        LIMIT ?
    """
    params.append(limit + 1)
    connection = connect(db_path)
    try:
        rows = connection.execute(sql, params).fetchall()
    finally:
        connection.close()

    has_more = len(rows) > limit
    page = rows[:limit]
    items = [
        {
            "event_id": row["event_id"],
            "tenant_id": row["tenant_id"],
            "source": row["source"],
            "event_type": row["event_type"],
            "timestamp": row["timestamp"],
            "payload": json.loads(row["payload"]),
            "created_at": row["created_at"],
        }
        for row in page
    ]
    next_cursor = None
    if has_more and page:
        last = page[-1]
        next_cursor = (last["timestamp"], last["event_id"])
    return items, next_cursor


def list_metrics(
    db_path,
    *,
    tenant_id,
    bucket_size,
    source=None,
    event_type=None,
    group_by=None,
    start=None,
    end=None,
    default_hours=24,
    max_minute_days=7,
    max_hour_days=366,
):
    group_by = group_by or set()
    now = datetime.now(UTC)
    if start is None and end is None:
        end = now
        start = end - timedelta(hours=default_hours)
    elif start is None:
        start = end - timedelta(hours=default_hours)
    elif end is None:
        end = start + timedelta(hours=default_hours)

    if start >= end:
        raise ValidationError("from must be earlier than to", details=[{"field": "from", "message": "from must be earlier than to"}])

    aligned_from, aligned_to = _align_window(start, end, bucket_size)

    span = aligned_to - aligned_from
    if bucket_size == "minute" and span > timedelta(days=max_minute_days):
        raise ValidationError(
            "minute queries are limited to 7 days",
            details=[{"field": "from", "message": "narrow the window or use hour buckets"}],
        )
    if bucket_size == "hour" and span > timedelta(days=max_hour_days):
        raise ValidationError(
            "hour queries are limited to 366 days",
            details=[{"field": "from", "message": "narrow the window"}],
        )

    source_expr = "source" if "source" in group_by else "NULL"
    type_expr = "event_type" if "event_type" in group_by else "NULL"
    group_cols = ["bucket_start"]
    if "source" in group_by:
        group_cols.append("source")
    if "event_type" in group_by:
        group_cols.append("event_type")

    sql = f"""
        SELECT
            bucket_start,
            {source_expr} AS source,
            {type_expr} AS event_type,
            SUM(count) AS count,
            MIN(first_seen) AS first_seen,
            MAX(last_seen) AS last_seen
        FROM aggregates
        WHERE tenant_id = ?
          AND bucket_size = ?
          AND (? = 0 OR source = ?)
          AND (? = 0 OR event_type = ?)
          AND bucket_start >= ?
          AND bucket_start < ?
        GROUP BY {", ".join(group_cols)}
        ORDER BY bucket_start ASC, source ASC, event_type ASC
    """
    params = [
        tenant_id,
        bucket_size,
        1 if source else 0,
        source or "",
        1 if event_type else 0,
        event_type or "",
        format_timestamp(aligned_from),
        format_timestamp(aligned_to),
    ]
    connection = connect(db_path)
    try:
        rows = connection.execute(sql, params).fetchall()
    finally:
        connection.close()
    buckets = [
        {
            "bucket_start": row["bucket_start"],
            "source": row["source"],
            "event_type": row["event_type"],
            "count": row["count"],
            "first_seen": row["first_seen"],
            "last_seen": row["last_seen"],
        }
        for row in rows
    ]
    return {
        "tenant_id": tenant_id,
        "bucket_size": bucket_size,
        "from": format_timestamp(aligned_from),
        "to": format_timestamp(aligned_to),
        "grouped_by": sorted(group_by),
        "buckets": buckets,
    }


def _align_window(start, end, bucket_size):
    """Include every stored bucket that overlaps [start, end).

    Counts always cover a whole minute or hour. A request that starts or ends
    mid-bucket still returns that bucket, and `to` in the response is the
    exclusive end after alignment.
    """
    aligned_from = floor_bucket(start, bucket_size)
    floored_end = floor_bucket(end, bucket_size)
    if format_timestamp(end) == format_timestamp(floored_end):
        aligned_to = floored_end
    else:
        aligned_to = add_bucket(floored_end, bucket_size)
    if aligned_from >= aligned_to:
        aligned_to = add_bucket(aligned_from, bucket_size)
    return aligned_from, aligned_to


def parse_query_time(value, field):
    if value is None or value == "":
        return None
    try:
        return parse_timestamp(value, enforce_bounds=False)
    except ValidationError as exc:
        details = []
        for detail in exc.details:
            updated = dict(detail)
            updated["field"] = field
            details.append(updated)
        raise ValidationError(exc.message, details=details) from exc
