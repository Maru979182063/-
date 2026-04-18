from __future__ import annotations

from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


def _mapping_root() -> Path:
    return Path(__file__).resolve().parents[3] / "card_specs" / "normalized" / "runtime_mappings"


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


@lru_cache(maxsize=1)
def load_distill_family_hierarchy_mapping() -> dict[str, Any]:
    return _read_yaml(_mapping_root() / "distill_family_hierarchy_mapping.yaml")


@lru_cache(maxsize=1)
def load_distill_material_card_mapping() -> dict[str, Any]:
    return _read_yaml(_mapping_root() / "distill_material_card_id_mapping.yaml")


class DistillRuntimeOverlayService:
    def __init__(self) -> None:
        self.hierarchy = load_distill_family_hierarchy_mapping()
        self.material_mapping = load_distill_material_card_mapping()

    def resolve(
        self,
        *,
        question_type: str | None,
        business_subtype: str | None,
        question_card: dict[str, Any] | None,
        material_source: dict[str, Any] | None,
        resolved_slots: dict[str, Any] | None,
    ) -> dict[str, Any]:
        mother_family_id = self._resolve_mother_family_id(
            question_type=question_type,
            business_subtype=business_subtype,
            question_card=question_card or {},
        )
        if not mother_family_id:
            return {}

        selected_material_card = self._selected_material_card(material_source)
        selected_business_card = self._selected_business_card(material_source)
        prompt_extras = self._prompt_extras(material_source)

        child_family_id = self._resolve_child_family_id(
            mother_family_id=mother_family_id,
            selected_material_card=selected_material_card,
            prompt_extras=prompt_extras,
            resolved_slots=resolved_slots or {},
        )
        leaf_key = self._resolve_leaf_key(
            question_type=question_type,
            selected_material_card=selected_material_card,
            selected_business_card=selected_business_card,
            prompt_extras=prompt_extras,
        )

        mother_overlay = self._overlay_for_mother(mother_family_id)
        child_overlay = self._overlay_for_child(child_family_id)

        resolved_slot_defaults = dict(mother_overlay.get("resolved_slot_defaults") or {})
        resolved_slot_defaults.update(dict(child_overlay.get("resolved_slot_defaults") or {}))

        control_logic_special_fields = dict(mother_overlay.get("control_logic_special_fields") or {})
        control_logic_special_fields.update(dict(child_overlay.get("control_logic_special_fields") or {}))

        prompt_guard_lines = [
            *[str(line).strip() for line in (mother_overlay.get("prompt_guard_lines") or []) if str(line).strip()],
            *[str(line).strip() for line in (child_overlay.get("prompt_guard_lines") or []) if str(line).strip()],
        ]

        return {
            "overlay_mode": "leaf_primary_parent_supplement",
            "mother_family_id": mother_family_id,
            "child_family_id": child_family_id,
            "leaf_key": leaf_key,
            "selected_material_card": selected_material_card,
            "selected_business_card": selected_business_card,
            "resolved_slot_defaults": resolved_slot_defaults,
            "control_logic_special_fields": control_logic_special_fields,
            "prompt_guard_lines": prompt_guard_lines,
        }

    def _resolve_mother_family_id(
        self,
        *,
        question_type: str | None,
        business_subtype: str | None,
        question_card: dict[str, Any],
    ) -> str | None:
        business_family_id = str(question_card.get("business_family_id") or "").strip()
        if business_family_id:
            return business_family_id

        mother_families = self.hierarchy.get("mother_families") or {}
        normalized_question_type = str(question_type or "").strip()
        normalized_business_subtype = str(business_subtype or "").strip() or None
        for family_id, payload in mother_families.items():
            if not isinstance(payload, dict):
                continue
            runtime_question_type = str(payload.get("runtime_question_type") or "").strip()
            runtime_business_subtype = str(payload.get("runtime_business_subtype") or "").strip() or None
            if runtime_question_type == normalized_question_type and runtime_business_subtype == normalized_business_subtype:
                return str(family_id)
        return None

    def _resolve_child_family_id(
        self,
        *,
        mother_family_id: str,
        selected_material_card: str | None,
        prompt_extras: dict[str, Any],
        resolved_slots: dict[str, Any],
    ) -> str | None:
        explicit = self._resolve_child_from_fill_or_order_runtime(
            mother_family_id=mother_family_id,
            selected_material_card=selected_material_card,
            prompt_extras=prompt_extras,
            resolved_slots=resolved_slots,
        )
        if explicit:
            return explicit

        child_families = self.hierarchy.get("child_families") or {}
        matches: list[str] = []
        for child_family_id, payload in child_families.items():
            if not isinstance(payload, dict):
                continue
            if str(payload.get("mother_family_id") or "").strip() != mother_family_id:
                continue
            material_card_ids = {
                str(card_id).strip()
                for card_id in (payload.get("material_card_ids") or [])
                if str(card_id).strip()
            }
            if selected_material_card and selected_material_card in material_card_ids:
                matches.append(str(child_family_id))
        if len(matches) == 1:
            return matches[0]
        return None

    def _resolve_child_from_fill_or_order_runtime(
        self,
        *,
        mother_family_id: str,
        selected_material_card: str | None,
        prompt_extras: dict[str, Any],
        resolved_slots: dict[str, Any],
    ) -> str | None:
        if mother_family_id == "sentence_fill":
            blank_position = str(
                prompt_extras.get("blank_position")
                or resolved_slots.get("blank_position")
                or ""
            ).strip()
            if blank_position == "opening":
                return "sentence_fill_head_start"
            if blank_position == "middle":
                return "sentence_fill_middle"
            if blank_position == "ending":
                return "sentence_fill_tail_end"

        if mother_family_id == "sentence_order":
            if selected_material_card == "order_material.tail_sentence_gate":
                return "sentence_order_tail_sentence"
            if selected_material_card == "order_material.carry_parallel_expand":
                return "sentence_order_fixed_bundle"
            if selected_material_card in {
                "order_material.timeline_progression",
                "order_material.viewpoint_reason_action",
                "order_material.problem_solution_case_blocks",
            }:
                return "sentence_order_sequence"
            if selected_material_card == "order_material.first_sentence_gate":
                return "sentence_order_first_sentence"
        return None

    def _resolve_leaf_key(
        self,
        *,
        question_type: str | None,
        selected_material_card: str | None,
        selected_business_card: str | None,
        prompt_extras: dict[str, Any],
    ) -> str | None:
        hard_logic_leaf_key = str(prompt_extras.get("hard_logic_leaf_key") or "").strip()
        if hard_logic_leaf_key:
            return hard_logic_leaf_key

        if question_type == "main_idea":
            business_card_leaf_map = {
                "turning_relation_focus__main_idea": "center_understanding.relation_words.turning",
                "parallel_comprehensive_summary__main_idea": "center_understanding.relation_words.parallel",
                "necessary_condition_countermeasure__main_idea": "center_understanding.relation_words.countermeasure",
                "cause_effect__conclusion_focus__main_idea": "center_understanding.relation_words.variant",
            }
            mapped = business_card_leaf_map.get(str(selected_business_card or "").strip())
            if mapped:
                return mapped

        unique_material_card_leaf = self._unique_material_card_leaf_map()
        return unique_material_card_leaf.get(str(selected_material_card or "").strip())

    def _unique_material_card_leaf_map(self) -> dict[str, str]:
        runtime_batches = self.material_mapping.get("batches") or {}
        card_to_labels: dict[str, set[str]] = {}
        for batch_id, payload in runtime_batches.items():
            if not isinstance(payload, dict):
                continue
            runtime_row_mapping = payload.get("runtime_row_mapping") or {}
            for mapping_group, group_payload in runtime_row_mapping.items():
                if not isinstance(group_payload, dict):
                    continue
                for label, details in group_payload.items():
                    if not isinstance(details, dict):
                        continue
                    material_card_id = str(details.get("material_card_id") or "").strip()
                    if not material_card_id:
                        continue
                    leaf_label = f"{batch_id}::{mapping_group}::{label}"
                    card_to_labels.setdefault(material_card_id, set()).add(leaf_label)
        unique: dict[str, str] = {}
        for material_card_id, labels in card_to_labels.items():
            if len(labels) == 1:
                unique[material_card_id] = next(iter(labels))
        return unique

    def _overlay_for_mother(self, mother_family_id: str | None) -> dict[str, Any]:
        mother_families = self.hierarchy.get("mother_families") or {}
        payload = mother_families.get(mother_family_id or "") or {}
        if not isinstance(payload, dict):
            return {}
        return deepcopy(payload.get("shared_runtime_overlay") or {})

    def _overlay_for_child(self, child_family_id: str | None) -> dict[str, Any]:
        child_families = self.hierarchy.get("child_families") or {}
        payload = child_families.get(child_family_id or "") or {}
        if not isinstance(payload, dict):
            return {}
        return deepcopy(payload.get("shared_runtime_overlay") or {})

    @staticmethod
    def _selected_material_card(material_source: dict[str, Any] | None) -> str | None:
        if not isinstance(material_source, dict):
            return None
        value = str(material_source.get("selected_material_card") or "").strip()
        return value or None

    @staticmethod
    def _selected_business_card(material_source: dict[str, Any] | None) -> str | None:
        if not isinstance(material_source, dict):
            return None
        value = str(material_source.get("selected_business_card") or "").strip()
        return value or None

    @staticmethod
    def _prompt_extras(material_source: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(material_source, dict):
            return {}
        prompt_extras = material_source.get("prompt_extras") or {}
        return deepcopy(prompt_extras) if isinstance(prompt_extras, dict) else {}
