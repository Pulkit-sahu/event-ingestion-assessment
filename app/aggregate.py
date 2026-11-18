import asyncio

import aiosqlite

from app.db import HOUR_UPSERT, MINUTE_UPSERT


async def aggregate_pending(db_path, batch_size=500):
    """Fold one batch of new events into minute and hour aggregates.

    The batch is chosen and applied inside a single write transaction, so a
    crash rolls the counts and the aggregated flag back together. Running this
    again does not double-count. Buckets are computed in SQL, not in Python.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    async with aiosqlite.connect(db_path, isolation_level=None, timeout=5.0) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("PRAGMA busy_timeout=5000")
        await db.execute("BEGIN IMMEDIATE")
        try:
            await db.execute("CREATE TEMP TABLE IF NOT EXISTS agg_batch (event_id TEXT PRIMARY KEY)")
            await db.execute("DELETE FROM agg_batch")
            await db.execute(
                """
                INSERT INTO agg_batch (event_id)
                SELECT event_id
                FROM events
                WHERE aggregated = 0
                ORDER BY timestamp ASC, event_id ASC
                LIMIT ?
                """,
                (batch_size,),
            )
            cursor = await db.execute("SELECT COUNT(*) FROM agg_batch")
            count = (await cursor.fetchone())[0]
            if count == 0:
                await db.execute("COMMIT")
                return 0

            await db.execute(MINUTE_UPSERT, ("minute",))
            await db.execute(HOUR_UPSERT, ("hour",))
            await db.execute(
                """
                UPDATE events
                SET aggregated = 1
                WHERE event_id IN (SELECT event_id FROM agg_batch)
                """
            )
            cursor = await db.execute(
                """
                SELECT COUNT(*)
                FROM events
                WHERE aggregated = 1
                  AND event_id IN (SELECT event_id FROM agg_batch)
                """
            )
            marked = (await cursor.fetchone())[0]
            if marked != count:
                raise RuntimeError("aggregation lost events; the batch was rolled back")
            await db.execute("COMMIT")
            return count
        except Exception:
            try:
                await db.execute("ROLLBACK")
            except Exception:
                pass
            raise


async def aggregate_all(db_path, batch_size=500):
    """Drain every pending event, one batch at a time."""
    total = 0
    while True:
        processed = await aggregate_pending(db_path, batch_size)
        total += processed
        if processed == 0:
            return total
        await asyncio.sleep(0)
