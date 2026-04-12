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
        payload = {
            "model": model_name,
            "input": [
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": system_prompt}],
                },
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": user_prompt}],
                },
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": schema_name,
                    "schema": self._build_strict_schema(schema),
                    "strict": True,
                }
            },
            "max_output_tokens": provider.params.max_output_tokens,
        }
        if not str(model_name).startswith("gpt-5"):
            payload["temperature"] = provider.params.temperature

        try:
            with httpx.Client(
                base_url=base_url,
                timeout=provider.params.timeout_seconds,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
            ) as client:
                response = client.post("/responses", json=payload)
                response.raise_for_status()
                data = response.json()
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

        text_output = ""
        for item in data.get("output", []):
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    text_output += content.get("text", "")
        if not text_output and isinstance(data.get("output_text"), str):
            text_output = str(data.get("output_text") or "")
        if not text_output:
            raise DomainError(
                "Configured LLM returned no structured output.",
                status_code=502,
                details={
                    "provider": route.provider,
                    "model": model_name,
                    "response_id": data.get("id"),
                },
            )
        try:
            return extract_json_object(text_output)
        except ValueError as exc:
            raise DomainError(
                "Configured LLM returned structured text that could not be parsed as a JSON object.",
                status_code=502,
                details={
                    "provider": route.provider,
                    "model": model_name,
                    "reason": str(exc),
                    "response_id": data.get("id"),
                    "text_preview": text_output[:800],
                },
            ) from exc

    def _get_provider(self, provider_name: str) -> ProviderConfig:
        provider = self.runtime_config.llm.providers.get(provider_name)
        if provider is None:
            raise DomainError(
                "Unknown LLM provider in runtime config.",
                status_code=500,
                details={"provider": provider_name},
            )
        return provider

    def _build_strict_schema(self, schema: dict[str, Any]) -> dict[str, Any]:
        strict_schema = deepcopy(schema)
        self._normalize_schema_node(strict_schema)
        return strict_schema

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
