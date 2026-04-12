import json
import unittest
from unittest.mock import patch

import httpx

from app.infra.llm.openai_provider import OpenAIResponsesProvider


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict, *, path: str = "/responses") -> None:
        self.status_code = status_code
        self._payload = payload
        self._path = path

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            request = httpx.Request("POST", f"https://example.test{self._path}")
            response = httpx.Response(self.status_code, request=request, json=self._payload)
            raise httpx.HTTPStatusError("http error", request=request, response=response)

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    responses = []
    calls = 0
    paths = []

    def __init__(self, *args, **kwargs) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, path: str, json: dict):
        _FakeClient.calls += 1
        _FakeClient.paths.append(path)
        next_item = _FakeClient.responses.pop(0)
        if isinstance(next_item, Exception):
            raise next_item
        return next_item


class OpenAIProviderRetryTests(unittest.TestCase):
    def setUp(self) -> None:
        _FakeClient.responses = []
        _FakeClient.calls = 0
        _FakeClient.paths = []

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

    def test_generate_json_falls_back_to_chat_completions_when_responses_gateway_is_unavailable(self) -> None:
        provider = OpenAIResponsesProvider.__new__(OpenAIResponsesProvider)
        provider.settings = type("Settings", (), {"openai_api_key": "test-key", "openai_base_url": "https://example.test"})()
        provider.timeout_seconds = 1
        provider.max_attempts = 2

        _FakeClient.responses = [
            _FakeResponse(502, {"error": "bad gateway"}, path="/responses"),
            _FakeResponse(
                200,
                {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps({"decision": "fallback_accept"})
                            }
                        }
                    ]
                },
                path="/chat/completions",
            ),
        ]

        with patch("app.infra.llm.openai_provider.httpx.Client", _FakeClient), patch("app.infra.llm.openai_provider.time.sleep", lambda *_: None):
            result = provider.generate_json(
                model="gpt-4.1-mini",
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

        self.assertEqual(result["decision"], "fallback_accept")
        self.assertEqual(_FakeClient.paths, ["/responses", "/chat/completions"])
