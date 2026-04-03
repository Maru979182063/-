from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(r"C:\Users\Maru\Documents\agent")
SOURCE_ROOT = Path(r"C:\Users\Maru\Downloads")
OUTPUT_ROOT = ROOT / "card_specs" / "normalized"

KNOWN_CANDIDATE_TYPES = [
    "whole_passage",
    "closed_span",
    "multi_paragraph_unit",
    "tail_segment",
    "bridge_segment",
    "sentence_block_group",
    "inserted_blank_unit",
    "insertion_context_unit",
    "phrase_or_clause_group",
]


@dataclass(frozen=True)
class FileGroup:
    business_family_id: str
    signal_file: str
    material_file: str
    question_file: str
    runtime_question_type: str
    runtime_business_subtype: str | None


FILE_GROUPS = [
    FileGroup(
        business_family_id="title_selection",
        signal_file="title_selection_signal_layer.yaml",
        material_file="title_selection_intermediate_material_cards.yaml",
        question_file="title_selection_standard_question_card.yaml",
        runtime_question_type="main_idea",
        runtime_business_subtype="title_selection",
    ),
    FileGroup(
        business_family_id="continuation",
        signal_file="continuation_signal_layer.yaml",
        material_file="continuation_intermediate_material_cards.yaml",
        question_file="continuation_standard_question_card.yaml",
        runtime_question_type="continuation",
        runtime_business_subtype=None,
    ),
    FileGroup(
        business_family_id="sentence_order",
        signal_file="sentence_order_signal_layer.yaml",
        material_file="sentence_order_intermediate_material_cards.yaml",
        question_file="sentence_order_standard_question_card.yaml",
        runtime_question_type="sentence_order",
        runtime_business_subtype=None,
    ),
    FileGroup(
        business_family_id="sentence_fill",
        signal_file="sentence_fill_signal_layer.yaml",
        material_file="sentence_fill_intermediate_material_cards.yaml",
        question_file="sentence_fill_standard_question_card.yaml",
        runtime_question_type="sentence_fill",
        runtime_business_subtype=None,
    ),
]


def load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def dump_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False, width=1000),
        encoding="utf-8",
    )


def normalize_signal_entry(entry: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {
        "signal_id": entry.get("signal_id") or entry.get("name"),
        "type": normalize_signal_type(str(entry.get("type", ""))),
        "description": entry.get("description") or entry.get("meaning", ""),
    }
    allowed_values = entry.get("allowed_values") or entry.get("values")
    if allowed_values is not None:
        normalized["allowed_values"] = allowed_values
    if "range" in entry:
        normalized["range"] = entry["range"]
    return normalized


def normalize_signal_type(signal_type: str) -> str:
    return {
        "categorical": "enum",
        "numeric_0_1": "float",
    }.get(signal_type, signal_type)


def normalize_signal_layer(group: FileGroup, source: dict[str, Any], source_file: str) -> tuple[dict[str, Any], dict[str, Any]]:
    family_index = source.get("family_index") or {}
    normalized = {
        "schema_version": "signal_layer.v1",
        "layer_id": source.get("layer_id") or source.get("registry_id"),
        "business_family_id": group.business_family_id,
        "layer_role": source.get("layer_role") or "neutral_signal_profile",
        "description": source.get("description") or source.get("core_principle"),
        "notes": source.get("notes") or family_index.get("notes") or [],
        "signals": [normalize_signal_entry(item) for item in source.get("signals") or source.get("neutral_signals") or []],
        "derived_signals": [normalize_signal_entry(item) for item in source.get("derived_signals") or []],
        "recommended_candidate_types": source.get("recommended_candidate_types", []),
        "candidate_profile_template": source.get("candidate_profile_template", {}),
        "normalized_from": source_file,
    }
    normalized = {key: value for key, value in normalized.items() if value not in (None, "", [], {})}
    report = {
        "source_file": source_file,
        "output_file": f"signal_layers/{Path(source_file).stem}.normalized.yaml",
        "schema_version": normalized["schema_version"],
        "business_family_id": group.business_family_id,
        "signal_count": len(normalized.get("signals", [])),
        "derived_signal_count": len(normalized.get("derived_signals", [])),
    }
    return normalized, report


def extract_candidate_contract(raw_policy: str) -> dict[str, Any]:
    allowed = [candidate_type for candidate_type in KNOWN_CANDIDATE_TYPES if candidate_type in raw_policy]
    preferred = allowed if "优先" in raw_policy else []
    return {
        "raw_policy": raw_policy,
        "allowed_candidate_types": allowed,
        "preferred_candidate_types": preferred,
    }


def normalize_material_card(card: dict[str, Any]) -> dict[str, Any]:
    bias_key = next(
        (
            key
            for key in card.keys()
            if key.endswith("_bias") and key not in {"distractor_bias"}
        ),
        None,
    )
    normalized = {
        "card_id": card["card_id"],
        "display_name": card["display_name"],
        "selection_core": card["selection_core"],
        "structures": card.get("structures", []),
        "required_signals": card.get("required_signals", {}),
        "preferred_signals": card.get("preferred_signals", []),
        "avoid_signals": card.get("avoid_signals", []),
        "candidate_contract": extract_candidate_contract(card.get("candidate_policy", "")),
        "card_bias": card.get(bias_key),
        "default_generation_archetype": card.get("recommended_generation_archetype"),
        "distractor_bias": card.get("distractor_bias", []),
    }
    return {key: value for key, value in normalized.items() if value not in (None, "", [], {})}


def normalize_material_registry(group: FileGroup, source: dict[str, Any], source_file: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    cards = [normalize_material_card(card) for card in source.get("cards", [])]
    card_index = {card["card_id"]: card for card in cards}
    normalized = {
        "schema_version": "material_card_registry.v1",
        "registry_id": source.get("registry_id"),
        "business_family_id": group.business_family_id,
        "registry_role": source.get("registry_role"),
        "notes": source.get("notes", []),
        "cards": cards,
        "normalized_from": source_file,
    }
    normalized = {key: value for key, value in normalized.items() if value not in (None, "", [], {})}
    report = {
        "source_file": source_file,
        "output_file": f"material_cards/{Path(source_file).stem}.normalized.yaml",
        "schema_version": normalized["schema_version"],
        "business_family_id": group.business_family_id,
        "card_count": len(cards),
    }
    return normalized, report, card_index


def normalize_question_card(
    group: FileGroup,
    source: dict[str, Any],
    source_file: str,
    material_cards: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    upstream_contract = dict(source.get("upstream_contract", {}))
    preferred_material_cards = list(upstream_contract.get("preferred_material_cards", []))

    required_candidate_types = set(upstream_contract.get("required_candidate_types", []))
    required_profiles = set(upstream_contract.get("required_profiles", []))
    for card_id in preferred_material_cards:
        material_card = material_cards.get(card_id, {})
        candidate_contract = material_card.get("candidate_contract", {})
        required_candidate_types.update(candidate_contract.get("allowed_candidate_types", []))
        required_profiles.update(material_card.get("required_signals", {}).keys())

    material_card_overrides = []
    for item in source.get("material_to_generation_mapping", []):
        material_card_overrides.append(
            {
                "material_card": item["material_card"],
                "slot_overrides": item.get("slot_overrides", {}),
            }
        )

    normalization_notes: list[str] = []
    if source.get("family_id") != group.business_family_id:
        normalization_notes.append(
            f"business_family_id normalized from legacy family_id={source.get('family_id')} to {group.business_family_id}"
        )
    if list(upstream_contract.get("required_candidate_types", [])) != sorted(required_candidate_types):
        normalization_notes.append("required_candidate_types expanded to cover all preferred material cards")
    if list(upstream_contract.get("required_profiles", [])) != sorted(required_profiles):
        normalization_notes.append("required_profiles expanded to include signals required by preferred material cards")
    if source.get("material_to_generation_mapping"):
        normalization_notes.append("generation_archetype duplication removed; question card now follows material_card.default_generation_archetype")

    normalized = {
        "schema_version": "question_card.v1",
        "card_id": source["card_id"],
        "card_type": source["card_type"],
        "business_family_id": group.business_family_id,
        "business_subtype_id": source.get("business_subtype"),
        "runtime_binding": {
            "question_type": group.runtime_question_type,
            "business_subtype": group.runtime_business_subtype,
        },
        "display_name": source["display_name"],
        "description": source.get("description"),
        "upstream_contract": {
            "required_candidate_types": sorted(required_candidate_types),
            "required_profiles": sorted(required_profiles),
            "preferred_material_cards": preferred_material_cards,
        },
        "base_slots": source.get("base_slots", {}),
        "slot_extensions": source.get("slot_extensions", {}),
        "generation_archetype_source": "material_card.default_generation_archetype",
        "material_card_overrides": material_card_overrides,
        "generation_archetypes": source.get("generation_archetypes", {}),
        "validator_contract": {
            "extension_rules": source.get("validator_extensions", []),
        },
        "normalized_from": source_file,
        "normalization_notes": normalization_notes,
    }
    normalized = {key: value for key, value in normalized.items() if value not in (None, "", [], {})}
    report = {
        "source_file": source_file,
        "output_file": f"question_cards/{Path(source_file).stem}.normalized.yaml",
        "schema_version": normalized["schema_version"],
        "business_family_id": group.business_family_id,
        "runtime_binding": normalized["runtime_binding"],
        "required_candidate_types": normalized["upstream_contract"]["required_candidate_types"],
        "required_profiles_count": len(normalized["upstream_contract"]["required_profiles"]),
        "normalization_notes": normalization_notes,
    }
    return normalized, report


def main() -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "normalized_root": str(OUTPUT_ROOT),
        "files": [],
    }

    for group in FILE_GROUPS:
        signal_source_path = SOURCE_ROOT / group.signal_file
        material_source_path = SOURCE_ROOT / group.material_file
        question_source_path = SOURCE_ROOT / group.question_file

        signal_source = load_yaml(signal_source_path)
        material_source = load_yaml(material_source_path)
        question_source = load_yaml(question_source_path)

        normalized_signal, signal_report = normalize_signal_layer(group, signal_source, signal_source_path.name)
        normalized_material, material_report, material_index = normalize_material_registry(group, material_source, material_source_path.name)
        normalized_question, question_report = normalize_question_card(group, question_source, question_source_path.name, material_index)

        dump_yaml(OUTPUT_ROOT / "signal_layers" / f"{signal_source_path.stem}.normalized.yaml", normalized_signal)
        dump_yaml(OUTPUT_ROOT / "material_cards" / f"{material_source_path.stem}.normalized.yaml", normalized_material)
        dump_yaml(OUTPUT_ROOT / "question_cards" / f"{question_source_path.stem}.normalized.yaml", normalized_question)

        report["files"].extend([signal_report, material_report, question_report])

    (OUTPUT_ROOT / "normalization_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
