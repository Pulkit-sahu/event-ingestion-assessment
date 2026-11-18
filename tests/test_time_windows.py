import asyncio

from app.aggregate import aggregate_all
from tests.conftest import event_body


def test_event_window_is_half_open(client):
    client.post("/events", json=event_body(event_id="start", timestamp="2024-01-15T10:00:00Z"))
    client.post("/events", json=event_body(event_id="middle", timestamp="2024-01-15T10:30:00Z"))
    client.post("/events", json=event_body(event_id="end", timestamp="2024-01-15T11:00:00Z"))

    response = client.get(
        "/events",
        query_string={"tenant_id": "acme", "from": "2024-01-15T10:00:00Z", "to": "2024-01-15T11:00:00Z"},
    )
    ids = [item["event_id"] for item in response.get_json()["items"]]
    assert ids == ["start", "middle"]


def test_filters_and_stable_cursor_pagination(client):
    rows = [
        event_body(event_id="b", timestamp="2024-01-15T10:00:00Z", source="web"),
        event_body(event_id="a", timestamp="2024-01-15T10:00:00Z", source="web"),
        event_body(event_id="c", timestamp="2024-01-15T10:05:00Z", source="web"),
        event_body(event_id="other", timestamp="2024-01-15T10:00:00Z", source="mobile"),
    ]
    assert client.post("/events/bulk", json={"events": rows}).status_code == 201

    first = client.get(
        "/events",
        query_string={"tenant_id": "acme", "source": "web", "limit": 2},
    )
    body = first.get_json()
    assert [item["event_id"] for item in body["items"]] == ["a", "b"]
    assert body["next_cursor"]

    second = client.get(
        "/events",
        query_string={"tenant_id": "acme", "source": "web", "limit": 2, "cursor": body["next_cursor"]},
    )
    assert [item["event_id"] for item in second.get_json()["items"]] == ["c"]
    assert second.get_json()["next_cursor"] is None


def test_offsets_are_converted_to_utc_and_naive_times_are_rejected(client):
    # 15:45 at +05:30 is 10:15 UTC.
    accepted = client.post("/events", json=event_body(timestamp="2024-01-15T15:45:30+05:30"))
    assert accepted.status_code == 201
    listed = client.get("/events", query_string={"tenant_id": "acme"})
    assert listed.get_json()["items"][0]["timestamp"] == "2024-01-15T10:15:30.000000Z"

    naive = client.post("/events", json=event_body(event_id="naive", timestamp="2024-01-15T10:15:30"))
    assert naive.status_code == 400
    future = client.post("/events", json=event_body(event_id="future", timestamp="2099-01-01T00:00:00Z"))
    assert future.status_code == 400
    ancient = client.post("/events", json=event_body(event_id="ancient", timestamp="1999-12-31T23:59:59Z"))
    assert ancient.status_code == 400


def test_hour_and_minute_boundaries(client, app):
    events = [
        event_body(event_id="open", timestamp="2024-01-15T10:00:00Z"),
        event_body(event_id="late-minute", timestamp="2024-01-15T10:59:59.999999Z"),
        event_body(event_id="next-hour", timestamp="2024-01-15T11:00:00Z"),
    ]
    assert client.post("/events/bulk", json={"events": events}).status_code == 201
    asyncio.run(aggregate_all(app.config["DATABASE"]))

    minutes = client.get(
        "/metrics",
        query_string={
            "tenant_id": "acme",
            "bucket": "minute",
            "from": "2024-01-15T10:00:00Z",
            "to": "2024-01-15T12:00:00Z",
            "group_by": "event_type",
        },
    ).get_json()["buckets"]
    assert [(row["bucket_start"], row["count"]) for row in minutes] == [
        ("2024-01-15T10:00:00.000000Z", 1),
        ("2024-01-15T10:59:00.000000Z", 1),
        ("2024-01-15T11:00:00.000000Z", 1),
    ]

    hours = client.get(
        "/metrics",
        query_string={
            "tenant_id": "acme",
            "bucket": "hour",
            "from": "2024-01-15T10:00:00Z",
            "to": "2024-01-15T11:00:00Z",
        },
    ).get_json()["buckets"]
    assert [(row["bucket_start"], row["count"]) for row in hours] == [
        ("2024-01-15T10:00:00.000000Z", 2),
    ]


def test_partial_hour_query_returns_the_overlapping_bucket(client, app):
    client.post("/events", json=event_body(timestamp="2024-01-15T10:30:00Z"))
    asyncio.run(aggregate_all(app.config["DATABASE"]))
    report = client.get(
        "/metrics",
        query_string={
            "tenant_id": "acme",
            "bucket": "hour",
            "from": "2024-01-15T10:15:00Z",
            "to": "2024-01-15T10:45:00Z",
        },
    ).get_json()
    assert report["from"] == "2024-01-15T10:00:00.000000Z"
    assert report["to"] == "2024-01-15T11:00:00.000000Z"
    assert report["buckets"][0]["count"] == 1


def test_tenant_is_required_and_other_tenants_stay_hidden(client):
    missing = client.get("/events")
    assert missing.status_code == 400

    client.post("/events", json=event_body(tenant_id="acme", event_id="a1"))
    client.post("/events", json=event_body(tenant_id="other", event_id="b1"))
    listed = client.get("/events", query_string={"tenant_id": "acme"})
    assert [item["event_id"] for item in listed.get_json()["items"]] == ["a1"]
