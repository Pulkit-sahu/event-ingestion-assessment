import asyncio
from datetime import datetime, timedelta, timezone

from app.aggregate import aggregate_all
from tests.conftest import event_body


def drain(app):
    return asyncio.run(aggregate_all(app.config["DATABASE"], batch_size=2))


def metrics(client, **params):
    params.setdefault("tenant_id", "acme")
    params.setdefault("from", "2024-01-15T10:00:00Z")
    params.setdefault("to", "2024-01-15T12:00:00Z")
    return client.get("/metrics", query_string=params)


def test_metrics_stay_empty_until_the_worker_runs(client, app):
    client.post("/events", json=event_body())
    before = metrics(client, bucket="minute", group_by="source,event_type")
    assert before.get_json()["buckets"] == []

    processed = drain(app)
    assert processed == 1

    after = metrics(client, bucket="minute", group_by="source,event_type")
    buckets = after.get_json()["buckets"]
    assert buckets == [
        {
            "bucket_start": "2024-01-15T10:15:00.000000Z",
            "source": "web",
            "event_type": "click",
            "count": 1,
            "first_seen": "2024-01-15T10:15:30.000000Z",
            "last_seen": "2024-01-15T10:15:30.000000Z",
        }
    ]


def test_counts_group_and_do_not_double_when_the_worker_reruns(client, app):
    events = [
        event_body(event_id="a", timestamp="2024-01-15T10:15:01Z", source="web", event_type="click"),
        event_body(event_id="b", timestamp="2024-01-15T10:15:40Z", source="web", event_type="click"),
        event_body(event_id="c", timestamp="2024-01-15T10:16:00Z", source="mobile", event_type="view"),
        event_body(event_id="d", timestamp="2024-01-15T11:00:00Z", source="web", event_type="click"),
    ]
    created = client.post("/events/bulk", json={"events": events})
    assert created.status_code == 201

    assert drain(app) == 4
    assert drain(app) == 0

    minute = metrics(client, bucket="minute", group_by="source,event_type").get_json()["buckets"]
    assert [(row["bucket_start"], row["source"], row["event_type"], row["count"]) for row in minute] == [
        ("2024-01-15T10:15:00.000000Z", "web", "click", 2),
        ("2024-01-15T10:16:00.000000Z", "mobile", "view", 1),
        ("2024-01-15T11:00:00.000000Z", "web", "click", 1),
    ]
    assert minute[0]["first_seen"] == "2024-01-15T10:15:01.000000Z"
    assert minute[0]["last_seen"] == "2024-01-15T10:15:40.000000Z"

    hour = metrics(client, bucket="hour", group_by="source").get_json()["buckets"]
    assert [(row["bucket_start"], row["source"], row["event_type"], row["count"]) for row in hour] == [
        ("2024-01-15T10:00:00.000000Z", "mobile", None, 1),
        ("2024-01-15T10:00:00.000000Z", "web", None, 2),
        ("2024-01-15T11:00:00.000000Z", "web", None, 1),
    ]

    totals = metrics(client, bucket="hour").get_json()["buckets"]
    assert [(row["bucket_start"], row["count"], row["source"], row["event_type"]) for row in totals] == [
        ("2024-01-15T10:00:00.000000Z", 3, None, None),
        ("2024-01-15T11:00:00.000000Z", 1, None, None),
    ]


def test_aggregation_is_incremental(client, app):
    client.post("/events", json=event_body(event_id="first", timestamp="2024-01-15T10:15:01Z"))
    drain(app)
    client.post("/events", json=event_body(event_id="second", timestamp="2024-01-15T10:15:50Z"))
    drain(app)

    buckets = metrics(client, bucket="minute", group_by="event_type").get_json()["buckets"]
    assert buckets[0]["count"] == 2
    assert buckets[0]["first_seen"] == "2024-01-15T10:15:01.000000Z"
    assert buckets[0]["last_seen"] == "2024-01-15T10:15:50.000000Z"


def test_default_metrics_window_is_recent(client, app):
    recent = (datetime.now(timezone.utc) - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    client.post("/events", json=event_body(event_id="old"))
    client.post("/events", json=event_body(event_id="new", timestamp=recent))
    drain(app)

    buckets = client.get("/metrics", query_string={"tenant_id": "acme", "bucket": "minute"}).get_json()["buckets"]
    assert len(buckets) == 1
    assert buckets[0]["count"] == 1
