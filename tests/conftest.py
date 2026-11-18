import pytest

from app import create_app


@pytest.fixture
def app(tmp_path):
    database = tmp_path / "events.db"
    application = create_app(
        {
            "DATABASE": str(database),
            "TESTING": True,
            "RATE_LIMIT_ENABLED": False,
        }
    )
    return application


@pytest.fixture
def client(app):
    return app.test_client()


def event_body(**overrides):
    body = {
        "event_id": "evt-1",
        "tenant_id": "acme",
        "source": "web",
        "event_type": "click",
        "timestamp": "2024-01-15T10:15:30Z",
        "payload": {"path": "/pricing"},
    }
    body.update(overrides)
    return body
