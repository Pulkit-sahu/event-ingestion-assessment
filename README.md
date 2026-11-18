# Event ingestion service

A small Flask service that stores events once, even when the same event arrives twice or at the same time, and keeps minute and hour counts in SQLite.

Python 3.10 or newer is required.

## Setup

```bash
cd assess
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the API

```bash
python -m app
```

The API listens on `http://127.0.0.1:5000`. Open that address in a browser for a rough page that walks through save, duplicate, and counts. The database file is `data/events.db`. Set `PORT` or `DATABASE_PATH` to change either one.

## Run the aggregator

Counts are not calculated during the read request. A separate worker folds new events into saved buckets. Run it in a second terminal:

```bash
python -m app.worker
```

Catch up once and exit:

```bash
python -m app.worker --once
```

Until the worker has run, `GET /metrics` returns an empty list for those events.

## Tests and benchmark

```bash
pytest
python -m app.benchmark
```

## Store one event

```bash
curl -s -X POST http://127.0.0.1:5000/events \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id": "evt-1",
    "tenant_id": "acme",
    "source": "web",
    "event_type": "click",
    "timestamp": "2024-01-15T10:15:30Z",
    "payload": {"path": "/pricing"}
  }'
```

Sending that body again returns `"status": "duplicate"` and does not add a second row. The same `event_id` with different content returns `409`.

## Store a batch

Up to 5,000 events. Send `{"events": [ ... ]}` or a raw JSON array.

```bash
curl -s -X POST http://127.0.0.1:5000/events/bulk \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"event_id": "evt-2", "tenant_id": "acme", "source": "mobile", "event_type": "view", "timestamp": "2024-01-15T10:16:00Z", "payload": {}},
    {"event_id": "evt-3", "tenant_id": "acme", "source": "web", "event_type": "click", "timestamp": "2024-01-15T11:00:00Z", "payload": {}}
  ]}'
```

An invalid item rejects the whole batch. Nothing from that request is saved.

## Read events

`tenant_id` is required. `source`, `event_type`, `from`, and `to` are optional. `from` is inclusive and `to` is exclusive. Results are oldest first, and `event_id` breaks ties so pages do not shuffle.

```bash
curl -s 'http://127.0.0.1:5000/events?tenant_id=acme&source=web&from=2024-01-15T00:00:00Z&to=2024-01-16T00:00:00Z&limit=50'
```

Pass `next_cursor` back as `cursor` to get the next page.

## Read counts

Run the worker first. `bucket` is `minute` or `hour`. Add `group_by=source`, `group_by=event_type`, or both. With no `from` and `to`, the window is the last 24 hours.

```bash
python -m app.worker --once
curl -s 'http://127.0.0.1:5000/metrics?tenant_id=acme&bucket=hour&from=2024-01-15T10:00:00Z&to=2024-01-15T12:00:00Z&group_by=source,event_type'
```

A bucket count covers the whole minute or hour. A `from` or `to` in the middle of a bucket still returns that full bucket. The response `from` and `to` show the aligned window.

## Checks

- `GET /health` answers that the process is up.
- `GET /ready` also runs `SELECT 1` against SQLite.

Design notes are in [ARCHITECTURE.md](ARCHITECTURE.md).
