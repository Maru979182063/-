from __future__ import annotations

import json
import os
from copy import deepcopy
from typing import Any

import httpx

from app.core.exceptions import DomainError
from app.schemas.runtime import OperationRouteConfig, ProviderConfig, QuestionRuntimeConfig
from app.services.text_readability import extract_json_object


class LLMGatewayService:
    def __init__(self, runtime_config: QuestionRuntimeConfig) -> None:
        self.runtime_config = runtime_config

    def generate_json(
        self,
        *,
        route: OperationRouteConfig,
        system_prompt: str,
        user_prompt: str,
        schema_name: str,
        schema: dict[str, Any],
    ) -> dict[str, Any]:
        provider = self._get_provider(route.provider)
        model_name = getattr(provider.models, route.model_key)
        api_key = os.getenv(provider.api_key_env)
        if not api_key:
            raise DomainError(
                "LLM API key is not configured.",
                status_code=503,
                details={"provider": route.provider, "api_key_env": provider.api_key_env},
            )

        base_url = os.getenv(provider.base_url_env, provider.default_base_url) if provider.base_url_env else provider.default_base_url
        chat_payload = {
            "model": model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": schema_name,
                    "schema": self._build_strict_schema(schema),
                    "strict": True,
                },
            },
            "max_tokens": provider.params.max_output_tokens,
        }
        if not str(model_name).startswith("gpt-5"):
            chat_payload["temperature"] = provider.params.temperature

        try:
            data = self._post_json(
                base_url=base_url,
                api_key=api_key,
                timeout_seconds=provider.params.timeout_seconds,
                path="/chat/completions",
                payload=chat_payload,
            )
        except httpx.HTTPStatusError as exc:
            response_text = exc.response.text
            if len(response_text) > 4000:
                response_text = response_text[:4000]
            raise DomainError(
                "Failed to call configured LLM provider.",
                status_code=502,
                details={
                    "provider": route.provider,
                    "model": model_name,
                    "base_url": base_url,
                    "status_code": exc.response.status_code,
                    "reason": str(exc),
                    "provider_error_body": response_text,
                },
            ) from exc
        except httpx.HTTPError as exc:
            raise DomainError(
                "Failed to call configured LLM provider.",
                status_code=502,
                details={
                    "provider": route.provider,
                    "model": model_name,
                    "base_url": base_url,
                    "reason": str(exc),
                },
            ) from exc

        text_output = self._extract_text_output(data)
        parse_error: ValueError | None = None
        if text_output:
            try:
                return extract_json_object(text_output)
            except ValueError as exc:
                parse_error = exc
        else:
            parse_error = ValueError("empty_output")

        retry_payload = self._build_plain_json_retry_payload(
            original_payload=chat_payload,
            schema=schema,
        )
        retry_data: dict[str, Any] | None = None
        retry_text_output = ""
        retry_parse_error: ValueError | None = None
        try:
            retry_data = self._post_json(
                base_url=base_url,
                api_key=api_key,
                timeout_seconds=provider.params.timeout_seconds,
                path="/chat/completions",
                payload=retry_payload,
            )
            retry_text_output = self._extract_text_output(retry_data)
            if retry_text_output:
                return extract_json_object(retry_text_output)
            retry_parse_error = ValueError("empty_output")
        except httpx.HTTPError:
            retry_parse_error = ValueError("retry_http_error")
        except ValueError as exc:
            retry_parse_error = exc

        raise DomainError(
            "Configured LLM returned structured text that could not be parsed as a JSON object.",
            status_code=502,
            details={
                "provider": route.provider,
                "model": model_name,
                "reason": str(parse_error) if parse_error else "json_object_not_found",
                "response_id": data.get("id"),
                "text_preview": text_output[:800],
                "fallback_retry_attempted": True,
                "fallback_retry_reason": str(retry_parse_error) if retry_parse_error else "",
                "fallback_retry_response_id": (retry_data or {}).get("id"),
                "fallback_retry_text_preview": retry_text_output[:800],
            },
        )

    def _get_provider(self, provider_name: str) -> ProviderConfig:
        provider = self.runtime_config.llm.providers.get(provider_name)
        if provider is None:
            raise DomainError(
                "Unknown LLM provider in runtime config.",
                status_code=500,
                details={"provider": provider_name},
            )
        return provider

    def _post_json(
        self,
        *,
        base_url: str,
        api_key: str,
        timeout_seconds: int,
        path: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        with httpx.Client(
            base_url=base_url,
            timeout=timeout_seconds,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        ) as client:
            response = client.post(path, json=payload)
            response.raise_for_status()
            return response.json()

    def _extract_text_output(self, data: dict[str, Any]) -> str:
        text_output = ""
        for item in data.get("output", []):
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    text_output += content.get("text", "")
        if text_output:
            return text_output
        if isinstance(data.get("output_text"), str):
            return str(data.get("output_text") or "")
        choices = list(data.get("choices") or [])
        if not choices:
            return ""
        message = choices[0].get("message") or {}
        content = message.get("content")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, dict):
                    text = item.get("text")
                    if text:
                        parts.append(str(text))
            return "".join(parts)
        return ""

    def _build_strict_schema(self, schema: dict[str, Any]) -> dict[str, Any]:
        strict_schema = deepcopy(schema)
        self._normalize_schema_node(strict_schema)
        return strict_schema

    def _build_plain_json_retry_payload(
        self,
        *,
        original_payload: dict[str, Any],
        schema: dict[str, Any],
    ) -> dict[str, Any]:
        payload = deepcopy(original_payload)
        payload.pop("response_format", None)
        messages = list(payload.get("messages") or [])
        messages.append(
            {
                "role": "system",
                "content": (
                    "The previous output was not valid structured JSON. "
                    "Retry now and return ONLY one JSON object that matches the schema. "
                    "Do not include markdown/code fences/explanations.\n"
                    f"Schema: {json.dumps(self._build_strict_schema(schema), ensure_ascii=False)}"
                ),
            }
        )
        payload["messages"] = messages
        if "temperature" in payload:
            payload["temperature"] = 0
        return payload

    def _normalize_schema_node(self, node: Any) -> None:
        if isinstance(node, dict):
            node_type = node.get("type")
            if node_type == "object":
                node.setdefault("additionalProperties", False)
                properties = node.get("properties")
                if isinstance(properties, dict) and properties:
                    node["required"] = sorted(properties.keys())
            for key in ("properties", "$defs", "definitions", "patternProperties"):
                child_map = node.get(key)
                if isinstance(child_map, dict):
                    for child in child_map.values():
                        self._normalize_schema_node(child)
            for key in ("items", "anyOf", "oneOf", "allOf", "prefixItems"):
                child = node.get(key)
                if isinstance(child, list):
                    for item in child:
                        self._normalize_schema_node(item)
                else:
                    self._normalize_schema_node(child)
        elif isinstance(node, list):
            for item in node:
                self._normalize_schema_node(item)
