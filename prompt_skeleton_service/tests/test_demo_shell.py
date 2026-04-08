from __future__ import annotations

import os
from unittest import TestCase

from fastapi.testclient import TestClient

from app.core.settings import get_settings
from app.main import app


class DemoShellSmokeTest(TestCase):
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

    def test_demo_index_is_public_even_when_security_enabled(self) -> None:
        response = self.client.get("/demo")

        self.assertEqual(response.status_code, 200)
        self.assertIn("前端工作台 Demo", response.text)
        self.assertIn('id="builderScreen"', response.text)
        self.assertIn('id="loadingScreen"', response.text)
        self.assertIn('id="resultScreen"', response.text)
        self.assertIn("/demo-static/app_v2.js", response.text)
        self.assertIn("/demo-static/app_v2_zh_patch.js", response.text)

    def test_demo_static_asset_is_public_even_when_security_enabled(self) -> None:
        response = self.client.get("/demo-static/app_v2.js")

        self.assertEqual(response.status_code, 200)
        self.assertIn("QUESTION_FOCUS_OPTIONS", response.text)
        self.assertIn("buildGeneratePayload", response.text)
