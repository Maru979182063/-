from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


_TYPE_CONFIG_PATH = Path(__file__).resolve().parents[3] / "prompt_skeleton_service" / "configs" / "types" / "sentence_fill.yaml"


@lru_cache(maxsize=1)
def _protocol_config() -> dict[str, Any]:
    raw = yaml.safe_load(_TYPE_CONFIG_PATH.read_text(encoding="utf-8")) or {}
    structure_schema = dict(raw.get("structure_schema") or {})
    legacy_slot_mapping = dict(raw.get("legacy_slot_mapping") or {})
    default_slots = dict(raw.get("default_slots") or {})
    return {
        "blank_positions": {str(item) for item in (structure_schema.get("blank_position", {}) or {}).get("values", []) if str(item).strip()},
        "function_types": {str(item) for item in (structure_schema.get("function_type", {}) or {}).get("values", []) if str(item).strip()},
        "logic_relations": {str(item) for item in (raw.get("slot_schema", {}).get("logic_relation", {}) or {}).get("allowed", []) if str(item).strip()},
        "legacy_function_type_mapping": {
            str(key): str(value)
            for key, value in dict(legacy_slot_mapping.get("function_type") or {}).items()
            if str(key).strip() and str(value).strip()
        },
        "legacy_logic_relation_mapping": {
            str(key): str(value)
            for key, value in dict(legacy_slot_mapping.get("logic_relation") or {}).items()
            if str(key).strip() and str(value).strip()
        },
        "default_slots": default_slots,
    }


def sentence_fill_default_slot(field_name: str, fallback: str = "") -> str:
    return str((_protocol_config().get("default_slots") or {}).get(field_name) or fallback).strip()


def normalize_sentence_fill_blank_position(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return raw if raw in _protocol_config()["blank_positions"] else raw


def normalize_sentence_fill_function_type(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    config = _protocol_config()
    if raw in config["function_types"]:
        return raw
    return str(config["legacy_function_type_mapping"].get(raw) or raw)


def normalize_sentence_fill_logic_relation(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    config = _protocol_config()
    if raw in config["logic_relations"]:
        return raw
    return str(config["legacy_logic_relation_mapping"].get(raw) or raw)


def normalize_sentence_fill_constraints(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    normalized = dict(payload)
    blank_position = normalize_sentence_fill_blank_position(payload.get("blank_position") or payload.get("position"))
    function_type = normalize_sentence_fill_function_type(payload.get("function_type"))
    logic_relation = normalize_sentence_fill_logic_relation(payload.get("logic_relation"))

    if blank_position:
        normalized["blank_position"] = blank_position
    else:
        normalized.pop("blank_position", None)

    if function_type:
        normalized["function_type"] = function_type
    else:
        normalized.pop("function_type", None)

    if logic_relation:
        normalized["logic_relation"] = logic_relation
    else:
        normalized.pop("logic_relation", None)

    return normalized


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump()
        if isinstance(dumped, dict):
            return dumped
    return {}


def strict_sentence_fill_export_field(
    field_name: str,
    value: Any,
    *,
    source_name: str = "",
) -> dict[str, Any]:
    raw = str(value or "").strip()
    result = {
        "field": field_name,
        "value": None,
        "status": "blocked",
        "alias_trace": None,
        "blocked_reason": None,
        "raw_value": raw,
        "source": source_name,
    }
    if not raw:
        result["blocked_reason"] = f"missing_sentence_fill_{field_name}"
        return result

    config = _protocol_config()
    if field_name == "blank_position":
        if raw in config["blank_positions"]:
            result["value"] = raw
            result["status"] = "direct"
            return result
        result["blocked_reason"] = f"non_canonical_sentence_fill_blank_position:{raw}"
        return result

    if field_name == "function_type":
        if raw in config["function_types"]:
            result["value"] = raw
            result["status"] = "direct"
            return result
        mapped = str(config["legacy_function_type_mapping"].get(raw) or "").strip()
        if mapped and mapped in config["function_types"]:
            result["value"] = mapped
            result["status"] = "mapped"
            result["alias_trace"] = {
                "field": field_name,
                "raw_value": raw,
                "mapped_value": mapped,
                "source": source_name,
            }
            return result
        result["blocked_reason"] = f"unknown_sentence_fill_function_type_alias:{raw}"
        return result

    if field_name == "logic_relation":
        if raw in config["logic_relations"]:
            result["value"] = raw
            result["status"] = "direct"
            return result
        mapped = str(config["legacy_logic_relation_mapping"].get(raw) or "").strip()
        if mapped and mapped in config["logic_relations"]:
            result["value"] = mapped
            result["status"] = "mapped"
            result["alias_trace"] = {
                "field": field_name,
                "raw_value": raw,
                "mapped_value": mapped,
                "source": source_name,
            }
            return result
        result["blocked_reason"] = f"unknown_sentence_fill_logic_relation_alias:{raw}"
        return result

    result["blocked_reason"] = f"unsupported_sentence_fill_export_field:{field_name}"
    return result


def sentence_fill_export_sources(item: dict[str, Any] | None) -> list[tuple[str, dict[str, Any]]]:
    payload = _as_mapping(item)
    request_snapshot = _as_mapping(payload.get("request_snapshot"))
    source_question_analysis = _as_mapping(request_snapshot.get("source_question_analysis"))
    return [
        ("item.resolved_slots", _as_mapping(payload.get("resolved_slots"))),
        ("item.material_selection.resolved_slots", _as_mapping(_as_mapping(payload.get("material_selection")).get("resolved_slots"))),
        ("item.request_snapshot.type_slots", _as_mapping(request_snapshot.get("type_slots"))),
        (
            "item.request_snapshot.source_question_analysis.retrieval_structure_constraints",
            _as_mapping(source_question_analysis.get("retrieval_structure_constraints")),
        ),
    ]


def project_sentence_fill_strict_export_view(item: dict[str, Any] | None) -> dict[str, Any] | None:
    payload = _as_mapping(item)
    if str(payload.get("question_type") or "").strip() != "sentence_fill":
        return None

    field_results: dict[str, dict[str, Any]] = {}
    alias_trace: list[dict[str, Any]] = []
    blocked_reason: str | None = None
    overall_status = "direct"

    for field_name in ("blank_position", "function_type", "logic_relation"):
        resolved: dict[str, Any] | None = None
        for source_name, source_payload in sentence_fill_export_sources(payload):
            if field_name not in source_payload:
                continue
            resolved = strict_sentence_fill_export_field(field_name, source_payload.get(field_name), source_name=source_name)
            break
        if resolved is None:
            resolved = strict_sentence_fill_export_field(field_name, "", source_name="")
        field_results[field_name] = resolved
        if resolved.get("alias_trace"):
            alias_trace.append(dict(resolved["alias_trace"]))
        if resolved["status"] == "blocked":
            overall_status = "blocked"
            blocked_reason = blocked_reason or str(resolved.get("blocked_reason") or f"blocked_sentence_fill_{field_name}")
        elif resolved["status"] == "mapped" and overall_status != "blocked":
            overall_status = "mapped"

    return {
        "status": overall_status,
        "blank_position": field_results["blank_position"]["value"],
        "function_type": field_results["function_type"]["value"],
        "logic_relation": field_results["logic_relation"]["value"],
        "alias_trace": alias_trace,
        "blocked_reason": blocked_reason,
        "field_results": field_results,
    }
