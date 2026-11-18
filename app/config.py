import os
from pathlib import Path

DEFAULTS = {
    "DATABASE": os.environ.get("DATABASE_PATH", "data/events.db"),
    "MAX_CONTENT_LENGTH": 8 * 1024 * 1024,
    "MAX_BULK_EVENTS": 5000,
    "MAX_EVENT_PAYLOAD_BYTES": 16 * 1024,
    "MAX_PAGE_SIZE": 200,
    "DEFAULT_PAGE_SIZE": 50,
    "RATE_LIMIT_ENABLED": True,
    "SINGLE_RATE_LIMIT": 120,
    "BULK_RATE_LIMIT": 12,
    "RATE_WINDOW_SECONDS": 60,
    "AGGREGATE_BATCH_SIZE": 500,
    "METRICS_DEFAULT_HOURS": 24,
    "MAX_MINUTE_RANGE_DAYS": 7,
    "MAX_HOUR_RANGE_DAYS": 366,
    "FUTURE_SKEW_MINUTES": 5,
}


def resolve_database(path, project_root):
    database = Path(path)
    if not database.is_absolute():
        database = Path(project_root) / database
    database.parent.mkdir(parents=True, exist_ok=True)
    return str(database)
