from __future__ import annotations

from app.services.config_registry import ConfigRegistry


class MetaService:
    def __init__(self, registry: ConfigRegistry) -> None:
        self.registry = registry

    def list_question_types(self) -> dict:
        configs = self.registry.list_types()
        return {
            "count": len(configs),
            "items": [
                {
                    "question_type": config.type_id,
                    "display_name": config.display_name,
                    "task_definition": config.task_definition,
                    "business_subtypes": [
                        {
                            "subtype_id": subtype.subtype_id,
                            "display_name": subtype.display_name,
                            "description": subtype.description,
                        }
                        for subtype in config.business_subtypes
                    ],
                }
                for config in configs
            ],
        }

    def get_controls(self, question_type: str) -> dict:
        config = self.registry.get_type(question_type)
        affects_difficulty_keys = self._collect_difficulty_slot_keys(config)
        controls = []
        for key, slot in config.slot_schema.items():
            controls.append(
                {
                    "control_key": key,
                    "label": self._humanize_key(key),
                    "control_type": slot.type,
                    "options": [{"value": value, "label": str(value)} for value in (slot.allowed or [])],
                    "default_value": config.default_slots.get(key, slot.default),
                    "required": slot.required,
                    "affects_difficulty": key in affects_difficulty_keys,
                    "editable_by": "generator_and_reviewer",
                    "mapped_action": "question_modify",
                    "read_only": False,
                    "description": slot.description,
                }
            )

        controls.extend(
            [
                {
                    "control_key": "difficulty_target",
                    "label": "Difficulty Target",
                    "control_type": "string",
                    "options": [{"value": value, "label": value} for value in ("easy", "medium", "hard")],
                    "default_value": "medium",
                    "required": True,
                    "affects_difficulty": True,
                    "editable_by": "generator_and_reviewer",
                    "mapped_action": "question_modify",
                    "read_only": False,
                    "description": "Unified difficulty target for prompt projection and review modification.",
                },
                {
                    "control_key": "pattern_id",
                    "label": "Pattern",
                    "control_type": "string",
                    "options": [
                        {"value": pattern.pattern_id, "label": pattern.pattern_name}
                        for pattern in config.patterns
                        if pattern.enabled
                    ],
                    "default_value": config.default_pattern_id,
                    "required": False,
                    "affects_difficulty": False,
                    "editable_by": "reviewer_only",
                    "mapped_action": "question_modify",
                    "read_only": False,
                    "description": "Optional forced pattern selection.",
                },
                {
                    "control_key": "material_policy.preferred_document_genres",
                    "label": "Preferred Document Genres",
                    "control_type": "array",
                    "options": [],
                    "default_value": [],
                    "required": False,
                    "affects_difficulty": False,
                    "editable_by": "reviewer_only",
                    "mapped_action": "text_modify",
                    "read_only": False,
                    "description": "Material governance hint for replacement selection.",
                },
            ]
        )
        return {"question_type": config.type_id, "controls": controls}

    def _collect_difficulty_slot_keys(self, config) -> set[str]:
        keys: set[str] = set()
        for pattern in config.patterns:
            for metric in pattern.difficulty_rules.model_dump().values():
                for slot_key in (metric.get("by_slot") or {}).keys():
                    keys.add(slot_key)
        return keys

    def _humanize_key(self, key: str) -> str:
        return key.replace("_", " ").title()
