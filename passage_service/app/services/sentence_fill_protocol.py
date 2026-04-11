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
