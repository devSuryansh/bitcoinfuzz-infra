"""Tests for the FastAPI REST API endpoints.

Uses FastAPI's TestClient for synchronous testing without Docker.
Target definitions come from ``tests/fixtures/``, so CI and shared
hosts do not need a sibling bitcoinfuzz checkout.
"""

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.app import build_app
from orchestrator.config import OrchestratorConfig

API_TOKEN = "test-token-not-a-real-secret"
AUTH_HEADER = {"Authorization": f"Bearer {API_TOKEN}"}


def _build_config(tmp_path, **overrides) -> OrchestratorConfig:
    """Config pointing at fixture targets and a throwaway database."""
    fixture_compose = (
        Path(__file__).resolve().parent
        / "fixtures"
        / "docker-compose.targets.yml"
    )
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    return OrchestratorConfig(
        compose_file=fixture_compose,
        data_dir=data_dir,
        db_path=tmp_path / "test.db",
        metrics_port=0,
        **overrides,
    )


@contextmanager
def _client_for(config: OrchestratorConfig):
    """Yield a TestClient with Docker forced unavailable."""
    with patch("docker.from_env", side_effect=Exception("docker disabled in tests")):
        app = build_app(config=config)
        with TestClient(app) as c:
            yield c


@pytest.fixture
def client(tmp_path):
    """Create a test client in API-only mode with fixture targets.

    Docker is forced unavailable so tests never launch real containers.
    Metrics HTTP binding is disabled (port 0) to avoid port clashes
    across parallel TestClient lifecycles. No API token is configured, so
    the mutating routes stay open the way a loopback dev run leaves them.
    """
    with _client_for(_build_config(tmp_path)) as c:
        yield c


@pytest.fixture
def token_client(tmp_path):
    """Client whose config requires a bearer token on mutating routes."""
    config = _build_config(tmp_path, api_token=API_TOKEN)
    with _client_for(config) as c:
        yield c


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "version" in data
        assert "uptime_seconds" in data

    def test_health_has_campaign_count(self, client):
        resp = client.get("/health")
        data = resp.json()
        assert "active_campaigns" in data


class TestTargetsEndpoints:
    def test_list_targets(self, client):
        resp = client.get("/targets")
        assert resp.status_code == 200
        targets = resp.json()
        assert isinstance(targets, list)
        assert len(targets) > 0

    def test_target_has_required_fields(self, client):
        resp = client.get("/targets")
        targets = resp.json()
        for target in targets:
            assert "name" in target
            assert "cxxflags" in target
            assert "fuzz" in target

    def test_list_targets_includes_known_target(self, client):
        resp = client.get("/targets")
        names = [t["name"] for t in resp.json()]
        assert "script" in names

    def test_get_specific_target(self, client):
        resp = client.get("/targets/script")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "script"
        assert "BITCOIN_CORE" in data["cxxflags"]

    def test_get_nonexistent_target(self, client):
        resp = client.get("/targets/nonexistent_target_xyz")
        assert resp.status_code == 404

    def test_target_has_resource_limits(self, client):
        resp = client.get("/targets/script")
        data = resp.json()
        assert "resource_limits" in data
        assert "cpu_quota" in data["resource_limits"]
        assert "memory_mb" in data["resource_limits"]


class TestCampaignEndpoints:
    """Campaign endpoints — test error handling and basic flows."""

    def test_create_campaign_returns_campaign(self, client):
        resp = client.post(
            "/campaigns",
            json={"target_name": "script"},
        )
        # Campaign is created (201) even if container launch fails
        # (it transitions to FAILED state internally)
        assert resp.status_code in (201, 503)
        if resp.status_code == 201:
            data = resp.json()
            assert data["target_name"] == "script"

    def test_create_campaign_invalid_target(self, client):
        resp = client.post(
            "/campaigns",
            json={"target_name": "nonexistent_target_xyz"},
        )
        assert resp.status_code in (400, 503)

    def test_list_campaigns(self, client):
        resp = client.get("/campaigns")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_get_nonexistent_campaign(self, client):
        resp = client.get("/campaigns/nonexistent123")
        assert resp.status_code == 404

    def test_delete_nonexistent_campaign(self, client):
        resp = client.delete("/campaigns/nonexistent123")
        # 404 (not found) or 503 (no docker)
        assert resp.status_code in (404, 503)

    def test_pause_nonexistent_campaign(self, client):
        resp = client.post("/campaigns/nonexistent123/pause")
        assert resp.status_code in (404, 503)

    def test_resume_nonexistent_campaign(self, client):
        resp = client.post("/campaigns/nonexistent123/resume")
        assert resp.status_code in (404, 503)


class TestDashboardEndpoints:
    def test_dashboard_summary(self, client):
        resp = client.get("/dashboard/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert "active_campaigns" in data
        assert "total_targets" in data
        assert data["total_targets"] > 0

    def test_resource_utilization(self, client):
        resp = client.get("/dashboard/resources")
        assert resp.status_code == 200


class TestAlertWebhook:
    def test_alert_webhook(self, client):
        payload = {
            "alerts": [
                {
                    "status": "firing",
                    "labels": {"alertname": "CampaignCrashed", "target": "script"},
                    "annotations": {"summary": "Campaign crashed"},
                }
            ]
        }
        resp = client.post("/api/alerts/webhook", json=payload)
        assert resp.status_code == 200
        assert resp.json()["count"] == 1

    def test_alert_webhook_empty(self, client):
        resp = client.post("/api/alerts/webhook", json={"alerts": []})
        assert resp.status_code == 200
        assert resp.json()["count"] == 0


class TestCIHook:
    def test_ci_hook(self, client):
        payload = {"commit": "abc123", "trigger": "push"}
        resp = client.post("/api/ci-hook", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["commit"] == "abc123"


class TestSchedulesEndpoint:
    def test_list_schedules_empty(self, client):
        resp = client.get("/schedules")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


class TestHistoryEndpoints:
    def test_target_history_unknown_target(self, client):
        resp = client.get("/targets/nonexistent_xyz/history")
        assert resp.status_code == 404

    def test_target_history_empty(self, client):
        resp = client.get("/targets/script/history")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_compare_unknown_target(self, client):
        resp = client.get(
            "/targets/nonexistent_xyz/compare",
            params={"campaign_ids": "abc,def"},
        )
        assert resp.status_code == 404

    def test_compare_known_target_empty(self, client):
        resp = client.get(
            "/targets/script/compare",
            params={"campaign_ids": "abc"},
        )
        assert resp.status_code == 200
        assert resp.json() == []

    def test_compare_rejects_other_target(self, client):
        from orchestrator import app as app_mod
        from orchestrator.models import Campaign, CampaignState

        other = Campaign(target_name="ecdh")
        other.transition_to(CampaignState.STARTING)
        other.transition_to(CampaignState.RUNNING)
        other.transition_to(CampaignState.COMPLETED)
        app_mod._db.create_campaign(other)
        app_mod._db.archive_campaign(other)

        resp = client.get(
            "/targets/script/compare",
            params={"campaign_ids": other.id},
        )
        assert resp.status_code == 400


class TestCORSHeaders:
    def test_cors_preflight_allows_configured_origin(self, client):
        resp = client.options(
            "/health",
            headers={
                "Origin": "http://127.0.0.1:8000",
                "Access-Control-Request-Method": "GET",
            },
        )
        assert resp.status_code == 200
        assert resp.headers["access-control-allow-origin"] == "http://127.0.0.1:8000"

    def test_cors_preflight_rejects_unknown_origin(self, client):
        resp = client.options(
            "/campaigns",
            headers={
                "Origin": "http://evil.example",
                "Access-Control-Request-Method": "POST",
            },
        )
        assert "access-control-allow-origin" not in resp.headers

    def test_cors_does_not_allow_credentials(self, client):
        """Credentials plus a wildcard makes Starlette echo any Origin."""
        resp = client.options(
            "/campaigns",
            headers={
                "Origin": "http://127.0.0.1:8000",
                "Access-Control-Request-Method": "POST",
            },
        )
        assert "access-control-allow-credentials" not in resp.headers


class TestBearerAuth:
    """Mutating routes reach the Docker socket, so they require the token."""

    MUTATING_REQUESTS = [
        ("post", "/campaigns", {"json": {"target_name": "script"}}),
        ("delete", "/campaigns/abc123", {}),
        ("post", "/campaigns/abc123/pause", {}),
        ("post", "/campaigns/abc123/resume", {}),
        ("post", "/api/alerts/webhook", {"json": {"alerts": []}}),
        ("post", "/api/ci-hook", {"json": {"commit": "abc123"}}),
    ]

    @pytest.mark.parametrize("method,path,kwargs", MUTATING_REQUESTS)
    def test_rejects_missing_token(self, token_client, method, path, kwargs):
        resp = getattr(token_client, method)(path, **kwargs)
        assert resp.status_code == 401

    @pytest.mark.parametrize("method,path,kwargs", MUTATING_REQUESTS)
    def test_accepts_valid_token(self, token_client, method, path, kwargs):
        resp = getattr(token_client, method)(path, headers=AUTH_HEADER, **kwargs)
        assert resp.status_code != 401

    def test_rejects_wrong_token(self, token_client):
        resp = token_client.post(
            "/api/ci-hook",
            json={"commit": "abc123"},
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert resp.status_code == 401

    def test_rejects_wrong_scheme(self, token_client):
        resp = token_client.post(
            "/api/ci-hook",
            json={"commit": "abc123"},
            headers={"Authorization": f"Basic {API_TOKEN}"},
        )
        assert resp.status_code == 401

    def test_challenges_with_www_authenticate(self, token_client):
        resp = token_client.post("/api/ci-hook", json={})
        assert resp.headers["www-authenticate"] == "Bearer"

    def test_read_only_routes_stay_open(self, token_client):
        """Reads carry no side effects, so the token is not required."""
        for path in ("/health", "/targets", "/campaigns", "/dashboard/summary"):
            assert token_client.get(path).status_code == 200

    def test_no_token_configured_leaves_routes_open(self, client):
        """Loopback dev runs work without setting a token at all."""
        resp = client.post("/api/ci-hook", json={"commit": "abc123"})
        assert resp.status_code == 200
