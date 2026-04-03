from __future__ import annotations

from pathlib import Path

import yaml

from app.core.exceptions import DomainError
from app.schemas.config import QuestionTypeConfig


class ConfigRegistry:
    def __init__(self, config_dir: Path) -> None:
        self.config_dir = config_dir
        self._types: dict[str, QuestionTypeConfig] = {}
        self._aliases: dict[str, str] = {}
        self._warnings: list[str] = []
        self._loaded = False

    def load(self) -> None:
        if not self.config_dir.exists():
            raise DomainError(
                "Config directory does not exist.",
                status_code=500,
                details={"config_dir": str(self.config_dir)},
            )

        loaded_types: dict[str, QuestionTypeConfig] = {}
        aliases: dict[str, str] = {}
        warnings: list[str] = []

        for path in sorted(self.config_dir.glob("*.yaml")):
            with path.open("r", encoding="utf-8") as fh:
                raw_config = yaml.safe_load(fh) or {}

            try:
                config = QuestionTypeConfig.model_validate(raw_config)
            except Exception as exc:  # noqa: BLE001
                raise DomainError(
                    "Failed to validate question type config.",
                    status_code=500,
                    details={"file": str(path), "reason": str(exc)},
                ) from exc

            if not config.enabled:
                warnings.append(f"Skipped disabled type: {config.type_id}")
                continue

            if config.type_id in loaded_types:
                raise DomainError(
                    "Duplicate type_id detected.",
                    status_code=500,
                    details={"type_id": config.type_id, "file": str(path)},
                )

            loaded_types[config.type_id] = config
            aliases[config.type_id.lower()] = config.type_id
            for alias in config.aliases:
                normalized = alias.lower()
                if normalized in aliases and aliases[normalized] != config.type_id:
                    raise DomainError(
                        "Duplicate alias detected.",
                        status_code=500,
                        details={"alias": alias, "file": str(path)},
                    )
                aliases[normalized] = config.type_id

        self._types = loaded_types
        self._aliases = aliases
        self._warnings = warnings
        self._loaded = True

    def reload(self) -> dict[str, int | list[str]]:
        self.load()
        return {
            "loaded_types": len(self._types),
            "loaded_patterns": sum(len(self.list_enabled_patterns(t.type_id)) for t in self._types.values()),
            "warnings": self._warnings,
        }

    def list_types(self) -> list[QuestionTypeConfig]:
        self._ensure_loaded()
        return list(self._types.values())

    def list_enabled_patterns(self, question_type: str) -> list[str]:
        config = self.get_type(question_type)
        return [pattern.pattern_id for pattern in config.patterns if pattern.enabled]

    def get_type(self, question_type: str) -> QuestionTypeConfig:
        self._ensure_loaded()
        normalized = question_type.lower()
        resolved_type = self._aliases.get(normalized)
        if not resolved_type or resolved_type not in self._types:
            raise DomainError(
                "Unknown question_type.",
                status_code=404,
                details={"question_type": question_type},
            )
        return self._types[resolved_type]

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            self.load()
