from functools import lru_cache
import os
from pathlib import Path

from pydantic import BaseModel


class SecuritySettings(BaseModel):
    enabled: bool = False
    api_token: str | None = None
    rate_limit_per_minute: int = 120


class AppSettings(BaseModel):
    base_dir: Path
    config_dir: Path
    runtime_config_path: Path
    prompt_template_config_path: Path
    data_dir: Path
    question_db_path: Path
    security: SecuritySettings


def _read_bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    base_dir = Path(__file__).resolve().parents[2]
    data_dir = base_dir / "data"
    api_token = os.getenv("PROMPT_SERVICE_API_TOKEN")
    security_enabled = _read_bool_env("PROMPT_SERVICE_SECURITY_ENABLED", default=bool(api_token))
    rate_limit_per_minute = int(os.getenv("PROMPT_SERVICE_RATE_LIMIT_PER_MINUTE", "120"))
    return AppSettings(
        base_dir=base_dir,
        config_dir=base_dir / "configs" / "types",
        runtime_config_path=base_dir / "configs" / "question_runtime.yaml",
        prompt_template_config_path=base_dir / "configs" / "prompt_templates.yaml",
        data_dir=data_dir,
        question_db_path=data_dir / "question_workbench.db",
        security=SecuritySettings(
            enabled=security_enabled,
            api_token=api_token,
            rate_limit_per_minute=rate_limit_per_minute,
        ),
    )
