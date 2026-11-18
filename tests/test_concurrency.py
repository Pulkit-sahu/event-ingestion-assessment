import asyncio
import json
import threading
from urllib.request import Request, urlopen

from werkzeug.serving import make_server

from app.aggregate import aggregate_all, aggregate_pending
from app.ingest import insert_event
from app.validation import parse_event
from tests.conftest import event_body


def _parse(body):
    return parse_event(body, max_payload_bytes=16 * 1024, skew_minutes=5)


def test_concurrent_retries_of_one_event_store_a_single_row(app):
    event = _parse(event_body())
    outcomes = []
    barrier = threading.Barrier(8)

    def send():
        barrier.wait()
        outcomes.append(insert_event(app.config["DATABASE"], event))

    threads = [threading.Thread(target=send) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert outcomes.count("created") == 1
    assert outcomes.count("duplicate") == 7
    assert outcomes.count("conflict") == 0

    connection_count = _count_events(app)
    assert connection_count == 1


def test_concurrent_distinct_events_are_all_stored(app):
    barrier = threading.Barrier(10)

    def send(index):
        barrier.wait()
        event = _parse(event_body(event_id=f"thread-{index}", payload={"n": index}))
        assert insert_event(app.config["DATABASE"], event) == "created"

    threads = [threading.Thread(target=send, args=(index,)) for index in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert _count_events(app) == 10


def test_writers_and_aggregator_do_not_lose_or_double_count(app):
    total = 40
    errors = []
    stop = threading.Event()

    def send(start):
        for offset in range(8):
            index = start + offset
            try:
                event = _parse(
                    event_body(
                        event_id=f"mix-{index}",
                        timestamp="2024-01-15T10:15:30Z",
                        payload={"n": index},
                    )
                )
                insert_event(app.config["DATABASE"], event)
            except Exception as exc:
                errors.append(exc)

    async def overlap():
        while not stop.is_set():
            try:
                await aggregate_pending(app.config["DATABASE"], batch_size=5)
            except Exception as exc:
                errors.append(exc)
            await asyncio.sleep(0.01)
        await aggregate_all(app.config["DATABASE"], batch_size=5)

    worker = threading.Thread(target=lambda: asyncio.run(overlap()))
    worker.start()
    threads = [threading.Thread(target=send, args=(index * 8,)) for index in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    stop.set()
    worker.join(timeout=10)
    assert worker.is_alive() is False
    assert errors == []
    assert asyncio.run(aggregate_all(app.config["DATABASE"], batch_size=7)) == 0

    from app.db import connect

    connection = connect(app.config["DATABASE"])
    try:
        minute = connection.execute(
            "SELECT COALESCE(SUM(count), 0) FROM aggregates WHERE bucket_size = 'minute'"
        ).fetchone()[0]
        hour = connection.execute(
            "SELECT COALESCE(SUM(count), 0) FROM aggregates WHERE bucket_size = 'hour'"
        ).fetchone()[0]
        pending = connection.execute("SELECT COUNT(*) FROM events WHERE aggregated = 0").fetchone()[0]
    finally:
        connection.close()
    assert minute == total
    assert hour == total
    assert pending == 0


def test_http_retries_are_idempotent(app):
    server = make_server("127.0.0.1", 0, app)
    port = server.server_port
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    statuses = []
    try:
        def send():
            request = Request(
                f"http://127.0.0.1:{port}/events",
                data=json.dumps(event_body(event_id="http-race")).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(request, timeout=5) as response:
                statuses.append(response.status)

        workers = [threading.Thread(target=send) for _ in range(6)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
    finally:
        server.shutdown()
        thread.join(timeout=5)

    assert statuses.count(201) == 1
    assert statuses.count(200) == 5
    assert _count_events(app) == 1


def _count_events(app):
    from app.db import connect

    connection = connect(app.config["DATABASE"])
    try:
        return connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    finally:
        connection.close()
