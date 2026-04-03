from __future__ import annotations

from pathlib import Path

import yaml

from app.schemas.runtime import QuestionRuntimeConfig


class RuntimeConfigRegistry:
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self._config: QuestionRuntimeConfig | None = None

    def load(self) -> QuestionRuntimeConfig:
        raw = yaml.safe_load(self.config_path.read_text(encoding="utf-8")) or {}
        self._config = QuestionRuntimeConfig.model_validate(raw)
        return self._config

    def get(self) -> QuestionRuntimeConfig:
        if self._config is None:
            return self.load()
        return self._config
