import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

import api.app as app_module
import api.services as services
import models.database as database


def _auth_headers() -> dict:
    return {"x-api-key": "test-key"}


def test_health_endpoint_uses_temp_database(tmp_path, monkeypatch):
    tmp_db = tmp_path / "data" / "skiing.db"
    monkeypatch.setattr(database, "DB_PATH", tmp_db)
    monkeypatch.setattr(services, "DB_PATH", tmp_db)
    monkeypatch.setenv("CROSS_COUNTRY_API_KEY", "test-key")

    with TestClient(app_module.app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "ok"
        assert payload["database"] == str(tmp_db)


def test_jobs_endpoint_requires_api_key(tmp_path, monkeypatch):
    tmp_db = tmp_path / "data" / "skiing.db"
    monkeypatch.setattr(database, "DB_PATH", tmp_db)
    monkeypatch.setattr(services, "DB_PATH", tmp_db)
    monkeypatch.setenv("CROSS_COUNTRY_API_KEY", "test-key")

    with TestClient(app_module.app) as client:
        response = client.get("/v1/jobs")
        assert response.status_code == 401


def test_create_and_list_jobs(tmp_path, monkeypatch):
    tmp_db = tmp_path / "data" / "skiing.db"
    monkeypatch.setattr(database, "DB_PATH", tmp_db)
    monkeypatch.setattr(services, "DB_PATH", tmp_db)
    monkeypatch.setenv("CROSS_COUNTRY_API_KEY", "test-key")

    with TestClient(app_module.app) as client:
        created = client.post("/v1/jobs/elo-build", headers=_auth_headers())
        assert created.status_code == 200
        job_id = created.json()["id"]

        listed = client.get("/v1/jobs?type=elo-build", headers=_auth_headers())
        assert listed.status_code == 200
        items = listed.json()["items"]
        assert isinstance(items, list)
        assert any(item["id"] == job_id for item in items)
