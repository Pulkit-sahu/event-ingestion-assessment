from tests.conftest import event_body


def test_health_and_ready(client):
    assert client.get("/health").get_json() == {"status": "ok"}
    ready = client.get("/ready")
    assert ready.status_code == 200
    assert ready.get_json()["database"] == "up"


def test_rate_limit_blocks_the_extra_call(tmp_path):
    from app import create_app

    application = create_app(
        {
            "DATABASE": str(tmp_path / "limited.db"),
            "TESTING": True,
            "RATE_LIMIT_ENABLED": True,
            "SINGLE_RATE_LIMIT": 2,
            "RATE_WINDOW_SECONDS": 60,
        }
    )
    client = application.test_client()
    assert client.post("/events", json=event_body(event_id="one")).status_code == 201
    assert client.post("/events", json=event_body(event_id="two")).status_code == 201
    blocked = client.post("/events", json=event_body(event_id="three"))
    assert blocked.status_code == 429
    assert blocked.headers["Retry-After"] == "60"
    assert blocked.headers["X-RateLimit-Remaining"] == "0"


def test_invalid_cursor_and_limit(client):
    assert client.get("/events", query_string={"tenant_id": "acme", "cursor": "@@@"}).status_code == 400
    assert client.get("/events", query_string={"tenant_id": "acme", "limit": 0}).status_code == 400
    assert client.get("/metrics", query_string={"tenant_id": "acme", "bucket": "day"}).status_code == 400
