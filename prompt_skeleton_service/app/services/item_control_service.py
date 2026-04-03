from __future__ import annotations

from app.core.exceptions import DomainError
from app.services.config_registry import ConfigRegistry
from app.services.meta_service import MetaService
from app.services.question_repository import QuestionRepository


class ItemControlService:
    def __init__(self, repository: QuestionRepository, registry: ConfigRegistry) -> None:
        self.repository = repository
        self.registry = registry

    def get_item_controls(self, item_id: str) -> dict:
        item = self.repository.get_item(item_id)
        if item is None:
            raise DomainError("Question item not found.", status_code=404, details={"item_id": item_id})

        meta = MetaService(self.registry).get_controls(item["question_type"])
        snapshot = item.get("request_snapshot") or {}
        current_slots = snapshot.get("type_slots") or {}
        material_policy = snapshot.get("material_policy") or {}
        controls = []
        for control in meta["controls"]:
            key = control["control_key"]
            current_value = self._resolve_current_value(key, item, snapshot, current_slots, material_policy)
            controls.append({**control, "current_value": current_value})

        return {
            "item_id": item["item_id"],
            "question_type": item["question_type"],
            "business_subtype": item.get("business_subtype"),
            "pattern_id": item.get("pattern_id"),
            "difficulty_target": item.get("difficulty_target"),
            "controls": controls,
        }

    def _resolve_current_value(
        self,
        key: str,
        item: dict,
        snapshot: dict,
        current_slots: dict,
        material_policy: dict,
    ):
        if key == "difficulty_target":
            return item.get("difficulty_target")
        if key == "pattern_id":
            return item.get("pattern_id")
        if key == "material_policy.preferred_document_genres":
            return material_policy.get("preferred_document_genres", [])
        if key in current_slots:
            return current_slots[key]
        return snapshot.get(key)
