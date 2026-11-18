from tests.conftest import event_body


def test_same_event_is_stored_once(client):
    first = client.post("/events", json=event_body())
    second = client.post("/events", json=event_body())

    assert first.status_code == 201
    assert first.get_json()["status"] == "created"
    assert second.status_code == 200
    assert second.get_json()["status"] == "duplicate"

    listed = client.get("/events", query_string={"tenant_id": "acme", "from": "2024-01-15T00:00:00Z", "to": "2024-01-16T00:00:00Z"})
    assert listed.status_code == 200
    assert len(listed.get_json()["items"]) == 1


def test_same_id_with_different_content_conflicts_and_keeps_original(client):
    created = client.post("/events", json=event_body())
    conflict = client.post("/events", json=event_body(payload={"path": "/other"}))

    assert created.status_code == 201
    assert conflict.status_code == 409
    assert conflict.get_json()["error"] == "conflict"

    listed = client.get("/events", query_string={"tenant_id": "acme"})
    items = listed.get_json()["items"]
    assert len(items) == 1
    assert items[0]["payload"] == {"path": "/pricing"}


def test_reordered_payload_keys_are_still_the_same_event(client):
    client.post("/events", json=event_body(payload={"b": 1, "a": 2}))
    replay = client.post("/events", json=event_body(payload={"a": 2, "b": 1}))
    assert replay.status_code == 200
    assert replay.get_json()["status"] == "duplicate"
