from typing import Any
import json

import httpx

from app.core.config import get_settings
from app.infra.llm.base import BaseLLMProvider


class OpenAIResponsesProvider(BaseLLMProvider):
    def __init__(self, timeout_seconds: int = 45) -> None:
        self.settings = get_settings()
        self.timeout_seconds = timeout_seconds

    def is_enabled(self) -> bool:
        return bool(self.settings.openai_api_key)

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

        with httpx.Client(
            base_url=self.settings.openai_base_url,
            timeout=self.timeout_seconds,
            headers={
                "Authorization": f"Bearer {self.settings.openai_api_key}",
                "Content-Type": "application/json",
            },
        ) as client:
            response = client.post("/responses", json=payload)
            response.raise_for_status()
            data = response.json()

        text_output = ""
        for item in data.get("output", []):
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    text_output += content.get("text", "")
        if not text_output:
            raise RuntimeError("No structured output returned by model.")
        return json.loads(text_output)
