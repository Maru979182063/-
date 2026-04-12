from __future__ import annotations

from typing import Any


_CANONICAL_CANDIDATE_TYPES = {"sentence_block_group"}
_CANONICAL_OPENING_ANCHOR_TYPES = {
    "explicit_topic",
    "upper_context_link",
    "viewpoint_opening",
    "problem_opening",
    "weak_opening",
    "none",
}
_CANONICAL_CLOSING_ANCHOR_TYPES = {
    "conclusion",
    "summary",
    "call_to_action",
    "case_support",
    "none",
}

_CANDIDATE_TYPE_ALIASES = {
    "ordered_unit_group": "sentence_block_group",
    "weak_formal_order_group": "sentence_block_group",
}
_OPENING_ANCHOR_ALIASES = {
    "definition_opening": "explicit_topic",
    "background_opening": "upper_context_link",
    "explicit_opening": "explicit_topic",
    "viewpoint_opening": "viewpoint_opening",
    "problem_opening": "problem_opening",
    "weak_opening": "weak_opening",
}
_CLOSING_ANCHOR_ALIASES = {
    "summary": "summary",
    "conclusion": "conclusion",
    "countermeasure": "call_to_action",
    "none": "none",
}
_AMBIGUOUS_BLOCKED_VALUES = {
    "closing_anchor_type": {"summary_or_conclusion"},
    "closing_rule": {"summary_or_conclusion"},
}


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump()
        if isinstance(dumped, dict):
            return dumped
    return {}


def sentence_order_export_sources(item: dict | None) -> list[tuple[str, dict[str, Any]]]:
    payload = _as_mapping(item)
    material_selection = _as_mapping(payload.get("material_selection"))
    request_snapshot = _as_mapping(payload.get("request_snapshot"))
    source_question_analysis = _as_mapping(request_snapshot.get("source_question_analysis"))
    retrieval_constraints = _as_mapping(source_question_analysis.get("retrieval_structure_constraints"))
    return [
        ("item.resolved_slots", _as_mapping(payload.get("resolved_slots"))),
        ("item.material_selection.resolved_slots", _as_mapping(material_selection.get("resolved_slots"))),
        ("item.material_selection.runtime_binding", _as_mapping(material_selection.get("runtime_binding"))),
        ("item.request_snapshot.type_slots", _as_mapping(request_snapshot.get("type_slots"))),
        (
            "item.request_snapshot.source_question_analysis.retrieval_structure_constraints",
            retrieval_constraints,
        ),
    ]


def strict_sentence_order_export_field(
    *,
    canonical_field_name: str,
    source_field_name: str,
    value: Any,
    source_name: str,
) -> dict[str, Any]:
    raw_value = str(value or "").strip()
    result = {
        "field": canonical_field_name,
        "value": None,
        "status": "blocked",
        "alias_trace": None,
        "blocked_reason": None,
        "raw_value": raw_value or None,
        "source": source_name,
        "source_field": source_field_name,
    }
    if not raw_value:
        result["blocked_reason"] = f"missing_sentence_order_{canonical_field_name}"
        return result

    if canonical_field_name == "candidate_type":
        if raw_value in _CANONICAL_CANDIDATE_TYPES:
            result["value"] = raw_value
            result["status"] = "direct"
            return result
        mapped = _CANDIDATE_TYPE_ALIASES.get(raw_value)
        if mapped is None:
            result["blocked_reason"] = f"unknown_sentence_order_candidate_type:{raw_value}"
            return result
        result["value"] = mapped
        result["status"] = "mapped"
        result["alias_trace"] = {
            "field": canonical_field_name,
            "source_field": source_field_name,
            "source": source_name,
            "raw_value": raw_value,
            "mapped_value": mapped,
        }
        return result

    if canonical_field_name == "opening_anchor_type":
        if raw_value in _CANONICAL_OPENING_ANCHOR_TYPES:
            result["value"] = raw_value
            result["status"] = "direct"
            return result
        mapped = _OPENING_ANCHOR_ALIASES.get(raw_value)
        if mapped is None:
            result["blocked_reason"] = f"unknown_sentence_order_opening_anchor:{raw_value}"
            return result
        result["value"] = mapped
        result["status"] = "mapped"
        result["alias_trace"] = {
            "field": canonical_field_name,
            "source_field": source_field_name,
            "source": source_name,
            "raw_value": raw_value,
            "mapped_value": mapped,
        }
        return result

    if canonical_field_name == "closing_anchor_type":
        if raw_value in _CANONICAL_CLOSING_ANCHOR_TYPES:
            result["value"] = raw_value
            result["status"] = "direct"
            return result
        if raw_value in _AMBIGUOUS_BLOCKED_VALUES.get(source_field_name, set()):
            result["blocked_reason"] = f"ambiguous_sentence_order_closing_anchor:{raw_value}"
            return result
        mapped = _CLOSING_ANCHOR_ALIASES.get(raw_value)
        if mapped is None:
            result["blocked_reason"] = f"unknown_sentence_order_closing_anchor:{raw_value}"
            return result
        result["value"] = mapped
        result["status"] = "mapped"
        result["alias_trace"] = {
            "field": canonical_field_name,
            "source_field": source_field_name,
            "source": source_name,
            "raw_value": raw_value,
            "mapped_value": mapped,
        }
        return result

    result["blocked_reason"] = f"unsupported_sentence_order_export_field:{canonical_field_name}"
    return result


def project_sentence_order_strict_export_view(item: dict | None) -> dict | None:
    payload = _as_mapping(item)
    if str(payload.get("question_type") or "").strip() != "sentence_order":
        return None

    sources = sentence_order_export_sources(payload)
    field_specs = (
        ("candidate_type", ("candidate_type",)),
        ("opening_anchor_type", ("opening_anchor_type", "opening_rule")),
        ("closing_anchor_type", ("closing_anchor_type", "closing_rule")),
    )
    field_results: dict[str, dict[str, Any]] = {}
    alias_trace: list[dict[str, Any]] = []

    for canonical_field_name, candidate_fields in field_specs:
        chosen_result: dict[str, Any] | None = None
        for source_name, source_payload in sources:
            if not source_payload:
                continue
            for source_field_name in candidate_fields:
                if source_field_name not in source_payload:
                    continue
                chosen_result = strict_sentence_order_export_field(
                    canonical_field_name=canonical_field_name,
                    source_field_name=source_field_name,
                    value=source_payload.get(source_field_name),
                    source_name=source_name,
                )
                break
            if chosen_result is not None:
                break
        if chosen_result is None:
            chosen_result = {
                "field": canonical_field_name,
                "value": None,
                "status": "blocked",
                "alias_trace": None,
                "blocked_reason": f"missing_sentence_order_{canonical_field_name}",
                "raw_value": None,
                "source": None,
                "source_field": None,
            }
        if chosen_result.get("alias_trace") is not None:
            alias_trace.append(dict(chosen_result["alias_trace"]))
        field_results[canonical_field_name] = chosen_result

    statuses = {result.get("status") for result in field_results.values()}
    overall_status = "mapped" if "mapped" in statuses else "direct"
    blocked_reason = None
    if "blocked" in statuses:
        overall_status = "blocked"
        blocked_reason = next(
            (
                result.get("blocked_reason")
                for result in field_results.values()
                if result.get("status") == "blocked" and result.get("blocked_reason")
            ),
            "sentence_order_export_projection_blocked",
        )

    return {
        "status": overall_status,
        "candidate_type": field_results["candidate_type"].get("value"),
        "opening_anchor_type": field_results["opening_anchor_type"].get("value"),
        "closing_anchor_type": field_results["closing_anchor_type"].get("value"),
        "alias_trace": alias_trace,
        "blocked_reason": blocked_reason,
        "field_results": field_results,
    }
