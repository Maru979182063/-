import json
import unittest
from unittest.mock import patch

import httpx

from app.infra.llm.openai_provider import OpenAIResponsesProvider


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict) -> None:
        self.status_code = status_code
        self._payload = payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            request = httpx.Request("POST", "https://example.test/responses")
            response = httpx.Response(self.status_code, request=request, json=self._payload)
            raise httpx.HTTPStatusError("http error", request=request, response=response)

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    responses = []
    calls = 0

    def __init__(self, *args, **kwargs) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, path: str, json: dict):
        _FakeClient.calls += 1
        next_item = _FakeClient.responses.pop(0)
        if isinstance(next_item, Exception):
            raise next_item
        return next_item


class OpenAIProviderRetryTests(unittest.TestCase):
    def setUp(self) -> None:
        _FakeClient.responses = []
        _FakeClient.calls = 0

    def test_generate_json_retries_transient_failures_up_to_success(self) -> None:
        provider = OpenAIResponsesProvider.__new__(OpenAIResponsesProvider)
        provider.settings = type("Settings", (), {"openai_api_key": "test-key", "openai_base_url": "https://example.test"})()
        provider.timeout_seconds = 1
        provider.max_attempts = 3

        _FakeClient.responses = [
            httpx.ConnectError("connect failed"),
            _FakeResponse(429, {"error": "rate limited"}),
            _FakeResponse(
                200,
                {
                    "output": [
                        {
                            "content": [
                                {
                                    "type": "output_text",
                                    "text": json.dumps({"decision": "accept"}),
                                }
                            ]
                        }
                    ]
                },
            ),
        ]

        with patch("app.infra.llm.openai_provider.httpx.Client", _FakeClient), patch("app.infra.llm.openai_provider.time.sleep", lambda *_: None):
            result = provider.generate_json(
                model="gpt-5.4-mini",
                instructions="test",
                input_payload={
                    "prompt": "hello",
                    "schema_name": "test_schema",
                    "schema": {
                        "type": "object",
                        "properties": {"decision": {"type": "string"}},
                        "required": ["decision"],
                        "additionalProperties": False,
                    },
                },
            )

        self.assertEqual(result["decision"], "accept")
        self.assertEqual(_FakeClient.calls, 3)

    def test_generate_json_does_not_retry_non_retryable_400(self) -> None:
        provider = OpenAIResponsesProvider.__new__(OpenAIResponsesProvider)
        provider.settings = type("Settings", (), {"openai_api_key": "test-key", "openai_base_url": "https://example.test"})()
        provider.timeout_seconds = 1
        provider.max_attempts = 3

        _FakeClient.responses = [_FakeResponse(400, {"error": "bad request"})]

        with patch("app.infra.llm.openai_provider.httpx.Client", _FakeClient), patch("app.infra.llm.openai_provider.time.sleep", lambda *_: None):
            with self.assertRaises(httpx.HTTPStatusError):
                provider.generate_json(
                    model="gpt-5.4-nano",
                    instructions="test",
                    input_payload={
                        "prompt": "hello",
                        "schema_name": "test_schema",
                        "schema": {
                            "type": "object",
                            "properties": {"decision": {"type": "string"}},
                            "required": ["decision"],
                            "additionalProperties": False,
                        },
                    },
                )

        self.assertEqual(_FakeClient.calls, 1)
