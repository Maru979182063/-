from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_healthz_returns_ok_with_request_id() -> None:
    client = TestClient(app)
    try:
        response = client.get("/healthz")
    finally:
        client.close()

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert response.headers["X-Request-ID"]


def test_readyz_returns_structured_payload() -> None:
    client = TestClient(app)
    try:
        response = client.get("/readyz")
    finally:
        client.close()

    assert response.status_code == 200
    payload = response.json()
    assert payload["service"] == "passage_service"
    assert payload["status"] == "ready"
    assert isinstance(payload["checks"], list)
    assert payload["settings"]["database_mode"] in {"primary", "dev", "mvp", "custom", "non_sqlite"}
    assert "db_pool_size" in payload["settings"]
