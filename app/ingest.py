import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from app.db import connect, retry_locked, rollback
from app.errors import ConflictError
from app.validation import Event, format_timestamp

INSERT_SQL = """
INSERT INTO events (
    event_id, tenant_id, source, event_type, timestamp, payload, created_at, aggregated
) VALUES (?, ?, ?, ?, ?, ?, ?, 0)
"""


@dataclass(frozen=True)
class BulkResult:
    received: int
    inserted: int
    duplicates: int


def same_event(row, event):
    return (
        row["tenant_id"] == event.tenant_id
        and row["source"] == event.source
        and row["event_type"] == event.event_type
        and row["timestamp"] == event.timestamp
        and json.loads(row["payload"]) == event.payload
    )


def _row_values(event, created_at):
    return (
        event.event_id,
        event.tenant_id,
        event.source,
        event.event_type,
        event.timestamp,
        event.payload_json(),
        created_at,
    )


def _fetch_existing(connection, event_ids):
    found = {}
    for start in range(0, len(event_ids), 400):
        chunk = event_ids[start:start + 400]
        placeholders = ",".join("?" for _ in chunk)
        rows = connection.execute(
            f"""
            SELECT event_id, tenant_id, source, event_type, timestamp, payload
            FROM events
            WHERE event_id IN ({placeholders})
            """,
            chunk,
        ).fetchall()
        for row in rows:
            found[row["event_id"]] = row
    return found


def insert_event(db_path, event):
    """Insert one event. Returns 'created', 'duplicate', or 'conflict'.

    BEGIN IMMEDIATE takes the write lock before the existence check, so two
    requests with the same event_id cannot both pass the check.
    """

    def operation():
        connection = connect(db_path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT event_id, tenant_id, source, event_type, timestamp, payload
                FROM events WHERE event_id = ?
                """,
                (event.event_id,),
            ).fetchone()
            if row is not None:
                connection.execute("COMMIT")
                return "duplicate" if same_event(row, event) else "conflict"
            created_at = format_timestamp(datetime.now(timezone.utc))
            connection.execute(INSERT_SQL, _row_values(event, created_at))
            connection.execute("COMMIT")
            return "created"
        except sqlite3.IntegrityError:
            rollback(connection)
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT event_id, tenant_id, source, event_type, timestamp, payload
                FROM events WHERE event_id = ?
                """,
                (event.event_id,),
            ).fetchone()
            connection.execute("COMMIT")
            if row is None:
                raise
            return "duplicate" if same_event(row, event) else "conflict"
        except Exception:
            rollback(connection)
            raise
        finally:
            connection.close()

    return retry_locked(operation)


def insert_bulk(db_path, events):
    """Insert a validated batch in one transaction.

    Identical event_ids already stored are skipped. The same id with different
    content aborts the whole batch so a bulk call never half-writes a conflict.
    """

    def operation():
        connection = connect(db_path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = _fetch_existing(connection, [event.event_id for event in events])
            fresh = []
            duplicates = 0
            conflicts = []
            for event in events:
                row = existing.get(event.event_id)
                if row is None:
                    fresh.append(event)
                elif same_event(row, event):
                    duplicates += 1
                else:
                    conflicts.append({"event_id": event.event_id, "message": "event_id already exists with different content"})
            if conflicts:
                connection.execute("ROLLBACK")
                raise ConflictError("one or more event_ids already exist with different content", details=conflicts[:50])
            if fresh:
                created_at = format_timestamp(datetime.now(timezone.utc))
                connection.executemany(INSERT_SQL, [_row_values(event, created_at) for event in fresh])
            connection.execute("COMMIT")
            return BulkResult(received=len(events), inserted=len(fresh), duplicates=duplicates)
        except ConflictError:
            raise
        except Exception:
            rollback(connection)
            raise
        finally:
            connection.close()

    return retry_locked(operation)
