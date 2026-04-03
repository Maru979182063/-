from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


def _normalized_root() -> Path:
    root = Path(__file__).resolve().parents[3] / "card_specs" / "normalized"
    if not root.exists():
        raise FileNotFoundError(f"Normalized card specs not found: {root}")
    return root


def _business_slot_root() -> Path:
    root = Path(__file__).resolve().parents[3] / "card_specs" / "business_feature_slots"
    if not root.exists():
        raise FileNotFoundError(f"Business feature slot specs not found: {root}")
    return root


def _read_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


@lru_cache
def load_card_registry_v2() -> dict[str, Any]:
    root = _normalized_root()
    signal_layers: dict[str, dict[str, Any]] = {}
    material_registries: dict[str, dict[str, Any]] = {}
    question_cards: dict[str, dict[str, Any]] = {}
    question_cards_by_family: dict[str, list[dict[str, Any]]] = {}
    business_cards: dict[str, dict[str, Any]] = {}

    for path in sorted((root / "signal_layers").glob("*.yaml")):
        payload = _read_yaml(path)
        signal_layers[payload["business_family_id"]] = payload

    for path in sorted((root / "material_cards").glob("*.yaml")):
        payload = _read_yaml(path)
        material_registries[payload["business_family_id"]] = payload

    for path in sorted((root / "question_cards").glob("*.yaml")):
        payload = _read_yaml(path)
        question_cards[payload["card_id"]] = payload
        question_cards_by_family.setdefault(payload["business_family_id"], []).append(payload)

    business_root = _business_slot_root()
    for path in sorted(business_root.glob("**/*.yaml")):
        if "templates" in path.parts:
            continue
        payload = _read_yaml(path)
        meta = payload.get("card_meta") or {}
        business_card_id = meta.get("business_card_id")
        if not business_card_id:
            continue
        business_cards[business_card_id] = payload

    return {
        "root": root,
        "signal_layers": signal_layers,
        "material_registries": material_registries,
        "question_cards": question_cards,
        "question_cards_by_family": question_cards_by_family,
        "business_cards": business_cards,
    }


class CardRegistryV2:
    def __init__(self) -> None:
        self.payload = load_card_registry_v2()

    def get_signal_layer(self, business_family_id: str) -> dict[str, Any]:
        return self.payload["signal_layers"][business_family_id]

    def get_material_registry(self, business_family_id: str) -> dict[str, Any]:
        return self.payload["material_registries"][business_family_id]

    def get_material_cards(self, business_family_id: str) -> list[dict[str, Any]]:
        return list(self.get_material_registry(business_family_id).get("cards", []))

    def get_question_card(self, card_id: str) -> dict[str, Any]:
        return self.payload["question_cards"][card_id]

    def get_default_question_card(self, business_family_id: str) -> dict[str, Any]:
        cards = self.payload["question_cards_by_family"].get(business_family_id, [])
        if not cards:
            raise KeyError(f"No question cards for family: {business_family_id}")
        return cards[0]

    def get_business_cards(
        self,
        business_family_id: str,
        *,
        runtime_question_type: str | None = None,
        runtime_business_subtype: str | None = None,
    ) -> list[dict[str, Any]]:
        aliases = self._family_aliases(
            business_family_id,
            runtime_question_type=runtime_question_type,
        )
        cards: list[dict[str, Any]] = []
        for card in self.payload.get("business_cards", {}).values():
            meta = card.get("card_meta") or {}
            if not meta.get("enabled", True):
                continue
            mother_family_id = str(meta.get("mother_family_id") or "").strip()
            if mother_family_id not in aliases:
                continue
            cards.append(
                {
                    **card,
                    "_runtime_match": {
                        "mother_family_id": mother_family_id,
                        "runtime_question_type": runtime_question_type,
                        "runtime_business_subtype": runtime_business_subtype,
                        "subtype_exact_match": bool(runtime_business_subtype and meta.get("business_subtype") == runtime_business_subtype),
                    },
                }
            )
        return cards

    def _family_aliases(self, business_family_id: str, *, runtime_question_type: str | None = None) -> set[str]:
        aliases = {business_family_id}
        if runtime_question_type:
            aliases.add(runtime_question_type)
        if business_family_id == "title_selection" or runtime_question_type == "main_idea":
            aliases.update({"main_idea", "title_selection"})
        return aliases
