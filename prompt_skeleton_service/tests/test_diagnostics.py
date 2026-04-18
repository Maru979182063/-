from __future__ import annotations

import os
from unittest import TestCase
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from app.core.settings import get_settings
from app.main import app


class PromptDiagnosticsTest(TestCase):
    def setUp(self) -> None:
        os.environ["PROMPT_SERVICE_SECURITY_ENABLED"] = "true"
        os.environ["PROMPT_SERVICE_API_TOKEN"] = "demo-token"
        get_settings.cache_clear()
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.client.close()
        os.environ.pop("PROMPT_SERVICE_SECURITY_ENABLED", None)
        os.environ.pop("PROMPT_SERVICE_API_TOKEN", None)
        get_settings.cache_clear()

    @patch("app.services.diagnostics.httpx.Client")
    def test_readyz_is_public_and_returns_structured_payload(self, client_cls: MagicMock) -> None:
        response = MagicMock()
        response.status_code = 200
        response.is_success = True
        response.headers = {"content-type": "application/json"}
        response.json.return_value = {
            "service": "passage_service",
            "status": "ready",
            "settings": {
                "database_mode": "primary",
                "resolved_database_path": "C:/demo/passage_service.db",
            },
            "checks": [
                {
                    "name": "database",
                    "details": {
                        "primary_material_count": 10,
                        "v2_indexed_primary_count": 8,
                    },
                }
            ],
        }
        client = MagicMock()
        client.get.return_value = response
        client_cls.return_value.__enter__.return_value = client

        result = self.client.get("/readyz")

        self.assertEqual(result.status_code, 200)
        payload = result.json()
        self.assertEqual(payload["service"], "prompt_skeleton_service")
        self.assertEqual(payload["status"], "ready")
        self.assertIn("checks", payload)
        self.assertIn("runtime_state", payload)
        self.assertIn("generation_queue", payload["runtime_state"])
        passage_check = next(check for check in payload["checks"] if check["name"] == "passage_service")
        self.assertEqual(passage_check["details"]["database_mode"], "primary")
        self.assertEqual(passage_check["details"]["resolved_database_path"], "C:/demo/passage_service.db")

    @patch("app.services.diagnostics.httpx.Client")
    def test_diagnostics_endpoint_requires_auth_but_returns_payload_with_token(self, client_cls: MagicMock) -> None:
        response = MagicMock()
        response.status_code = 200
        response.is_success = True
        response.headers = {"content-type": "application/json"}
        response.json.return_value = {
            "service": "passage_service",
            "status": "ready",
            "settings": {"database_mode": "primary"},
            "checks": [],
        }
        client = MagicMock()
        client.get.return_value = response
        client_cls.return_value.__enter__.return_value = client

        unauthorized = self.client.get("/api/v1/diagnostics/dependencies")
        self.assertEqual(unauthorized.status_code, 401)

        authorized = self.client.get(
            "/api/v1/diagnostics/dependencies",
            headers={"Authorization": "Bearer demo-token"},
        )
        self.assertEqual(authorized.status_code, 200)
        self.assertEqual(authorized.json()["service"], "prompt_skeleton_service")
