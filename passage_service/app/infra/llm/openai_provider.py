from typing import Any
import json
import os
import time
from pathlib import Path

import httpx

from app.core.config import get_settings
from app.core.config import get_config_bundle
from app.infra.llm.base import BaseLLMProvider


class OpenAIResponsesProvider(BaseLLMProvider):
    def __init__(self, timeout_seconds: int = 45) -> None:
        self.settings = get_settings()
        self.timeout_seconds = timeout_seconds
        llm_config = get_config_bundle().llm
        retries = dict(llm_config.get("retries") or {})
        self.max_attempts = max(1, int(retries.get("max_attempts", 3)))

    def is_enabled(self) -> bool:
        return bool(self._resolved_api_key())

    def generate_json(self, *, model: str, instructions: str, input_payload: dict[str, Any]) -> dict[str, Any]:
        if not self.is_enabled():
            raise RuntimeError("OpenAI API key is not configured.")

        payload = {
            "model": model,
            "input": [
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": instructions}],
                },
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": input_payload["prompt"]}],
                },
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": input_payload["schema_name"],
                    "schema": input_payload["schema"],
                    "strict": True,
                }
            },
        }

        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                with httpx.Client(
                    base_url=self._resolved_base_url(),
                    timeout=self.timeout_seconds,
                    headers={
                        "Authorization": f"Bearer {self._resolved_api_key()}",
                        "Content-Type": "application/json",
                    },
                ) as client:
                    response = client.post("/responses", json=payload)
                    if response.status_code in {429, 500, 502, 503, 504}:
                        response.raise_for_status()
                    elif response.status_code >= 400:
                        response.raise_for_status()
                    data = response.json()
                break
            except httpx.HTTPStatusError as exc:
                last_error = exc
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code not in {429, 500, 502, 503, 504} or attempt >= self.max_attempts:
                    raise
                time.sleep(min(0.8 * attempt, 2.0))
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteError, httpx.RemoteProtocolError) as exc:
                last_error = exc
                if attempt >= self.max_attempts:
                    raise
                time.sleep(min(0.8 * attempt, 2.0))
        else:
            if last_error is not None:
                raise last_error
            raise RuntimeError("No response returned by provider.")

        text_output = ""
        for item in data.get("output", []):
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    text_output += content.get("text", "")
        if not text_output:
            raise RuntimeError("No structured output returned by model.")
        return json.loads(text_output)

    def _resolved_api_key(self) -> str | None:
        return (
            self.settings.openai_api_key
            or self._read_local_env_value("PASSAGE_OPENAI_API_KEY")
            or os.getenv("MATERIAL_LLM_API_KEY")
            or os.getenv("GENERATION_LLM_API_KEY")
        )

    def _resolved_base_url(self) -> str:
        return (
            (self.settings.openai_base_url or "").strip()
            or self._read_local_env_value("PASSAGE_OPENAI_BASE_URL", "")
            or os.getenv("MATERIAL_LLM_BASE_URL", "").strip()
            or os.getenv("GENERATION_LLM_BASE_URL", "").strip()
            or "https://api.openai.com/v1"
        )

    def _read_local_env_value(self, key: str, default: str | None = None) -> str | None:
        env_path = Path(__file__).resolve().parents[3] / ".env"
        if not env_path.exists():
            return default
        try:
            for raw_line in env_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                name, value = line.split("=", 1)
                if name.strip().lstrip("\ufeff") == key:
                    return value.strip()
        except OSError:
            return default
        return default
