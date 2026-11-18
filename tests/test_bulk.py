from tests.conftest import event_body


def _events(count, **overrides):
    items = []
    for index in range(count):
        body = event_body(event_id=f"evt-{index}", payload={"n": index})
        body.update(overrides)
        items.append(body)
    return items


def test_bulk_inserts_a_batch(client):
    response = client.post("/events/bulk", json={"events": _events(3)})
    assert response.status_code == 201
    body = response.get_json()
    assert body["received"] == 3
    assert body["inserted"] == 3
    assert body["duplicates"] == 0

    listed = client.get("/events", query_string={"tenant_id": "acme"})
    assert len(listed.get_json()["items"]) == 3


def test_bulk_replay_is_idempotent(client):
    payload = {"events": _events(2)}
    first = client.post("/events/bulk", json=payload)
    second = client.post("/events/bulk", json=payload)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.get_json()["inserted"] == 0
    assert second.get_json()["duplicates"] == 2

    listed = client.get("/events", query_string={"tenant_id": "acme"})
    assert len(listed.get_json()["items"]) == 2


def test_bulk_accepts_a_raw_list_and_collapses_identical_duplicates(client):
    items = _events(1) + _events(1)
    response = client.post("/events/bulk", json=items)
    assert response.status_code == 201
    body = response.get_json()
    assert body["received"] == 2
    assert body["inserted"] == 1
    assert body["duplicates"] == 1


def test_bulk_rejects_more_than_5000_events(client):
    items = _events(5001)
    response = client.post("/events/bulk", json={"events": items})
    assert response.status_code == 413
    listed = client.get("/events", query_string={"tenant_id": "acme"})
    assert listed.get_json()["items"] == []


def test_bulk_rejects_invalid_items_and_writes_nothing(client):
    items = _events(2)
    items.append(event_body(event_id="bad", timestamp="not-a-time"))
    response = client.post("/events/bulk", json={"events": items})
    assert response.status_code == 400
    details = response.get_json()["details"]
    assert details[0]["index"] == 2
    listed = client.get("/events", query_string={"tenant_id": "acme"})
    assert listed.get_json()["items"] == []


def test_bulk_conflict_does_not_insert_the_rest_of_the_batch(client):
    client.post("/events", json=event_body(event_id="evt-0", payload={"n": 999}))
    response = client.post("/events/bulk", json={"events": _events(2)})
    assert response.status_code == 409
    listed = client.get("/events", query_string={"tenant_id": "acme"})
    items = listed.get_json()["items"]
    assert [item["event_id"] for item in items] == ["evt-0"]
    assert items[0]["payload"] == {"n": 999}


def test_oversized_payload_is_rejected(client):
    huge = {"blob": "x" * 20000}
    response = client.post("/events", json=event_body(payload=huge))
    assert response.status_code == 413
    listed = client.get("/events", query_string={"tenant_id": "acme"})
    assert listed.get_json()["items"] == []


def test_payload_must_be_an_object(client):
    response = client.post("/events", json=event_body(payload=["nope"]))
    assert response.status_code == 400
    assert response.get_json()["details"][0]["field"] == "payload"
