from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from app.core.exceptions import DomainError


_CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "question_generation_prompt_assets.yaml"


@lru_cache(maxsize=1)
def load_question_generation_prompt_assets() -> dict[str, Any]:
    if not _CONFIG_PATH.exists():
        raise DomainError(
            "Question generation prompt assets config does not exist.",
            status_code=500,
            details={"config_path": str(_CONFIG_PATH)},
        )

    raw = yaml.safe_load(_CONFIG_PATH.read_text(encoding="utf-8")) or {}
    assets = raw.get("question_generation")
    if not isinstance(assets, dict):
        raise DomainError(
            "Question generation prompt assets config is invalid.",
            status_code=500,
            details={"config_path": str(_CONFIG_PATH)},
        )
    return assets
