from __future__ import annotations

from pathlib import Path

import yaml

from app.core.exceptions import DomainError
from app.schemas.prompt_registry import PromptTemplateRecord


class PromptTemplateRegistry:
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self._templates: list[PromptTemplateRecord] = []

    def load(self) -> list[PromptTemplateRecord]:
        if not self.config_path.exists():
            raise DomainError(
                "Prompt template config does not exist.",
                status_code=500,
                details={"config_path": str(self.config_path)},
            )
        raw = yaml.safe_load(self.config_path.read_text(encoding="utf-8")) or {}
        items = raw.get("templates", [])
        self._templates = [PromptTemplateRecord.model_validate(item) for item in items if item.get("is_active", True)]
        return self._templates

    def get(self) -> list[PromptTemplateRecord]:
        if not self._templates:
            return self.load()
        return self._templates

    def list_templates(self) -> list[PromptTemplateRecord]:
        return list(self.get())

    def get_by_name(self, template_name: str) -> list[PromptTemplateRecord]:
        items = [item for item in self.get() if item.template_name == template_name]
        if not items:
            raise DomainError(
                "Prompt template not found.",
                status_code=404,
                details={"template_name": template_name},
            )
        return items

    def resolve_default(
        self,
        *,
        question_type: str,
        business_subtype: str | None,
        action_type: str,
    ) -> PromptTemplateRecord:
        candidates = [
            item
            for item in self.get()
            if item.question_type == question_type
            and item.action_type == action_type
            and item.is_active
            and item.business_subtype == business_subtype
        ]
        if not candidates and business_subtype:
            candidates = [
                item
                for item in self.get()
                if item.question_type == question_type and item.action_type == action_type and item.is_active and item.business_subtype is None
            ]
        if not candidates:
            raise DomainError(
                "No active prompt template matched the request.",
                status_code=500,
                details={
                    "question_type": question_type,
                    "business_subtype": business_subtype,
                    "action_type": action_type,
                },
            )
        candidates.sort(key=lambda item: item.template_version, reverse=True)
        return candidates[0]
