import argparse
import asyncio
import logging
import os
from pathlib import Path

from app.aggregate import aggregate_all, aggregate_pending
from app.config import resolve_database
from app.db import init_db

LOGGER = logging.getLogger("aggregator")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Incrementally aggregate stored events")
    parser.add_argument("--once", action="store_true", help="Catch up on pending events, then exit")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds to sleep when there is nothing to do")
    parser.add_argument("--batch-size", type=int, default=500, help="Events applied per transaction")
    parser.add_argument("--database", default=os.environ.get("DATABASE_PATH", "data/events.db"))
    return parser.parse_args(argv)


async def run(db_path, interval, batch_size, once):
    if once:
        processed = await aggregate_all(db_path, batch_size)
        LOGGER.info("caught up, processed %s events", processed)
        return processed

    LOGGER.info("watching %s", db_path)
    while True:
        processed = await aggregate_pending(db_path, batch_size)
        if processed:
            LOGGER.info("aggregated %s events", processed)
        if processed == batch_size:
            await asyncio.sleep(0)
        else:
            await asyncio.sleep(interval)


def main(argv=None):
    args = parse_args(argv)
    if args.batch_size < 1:
        raise SystemExit("--batch-size must be at least 1")
    if args.interval < 0:
        raise SystemExit("--interval must be >= 0")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    project_root = Path(__file__).resolve().parent.parent
    db_path = resolve_database(args.database, project_root)
    init_db(db_path)
    asyncio.run(run(db_path, args.interval, args.batch_size, args.once))


if __name__ == "__main__":
    main()
