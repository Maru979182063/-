from pathlib import Path

from app.core.config import get_config_bundle
from app.infra.llm.base import BaseLLMProvider
from app.infra.llm.openai_provider import OpenAIResponsesProvider


def get_llm_provider() -> BaseLLMProvider:
    timeout_seconds = int(get_config_bundle().llm.get("timeouts", {}).get("request_seconds", 45))
    return OpenAIResponsesProvider(timeout_seconds=timeout_seconds)


def read_prompt_file(name: str) -> str:
    prompt_path = Path(__file__).resolve().parent.parent / "prompts" / name
    return prompt_path.read_text(encoding="utf-8").strip()
