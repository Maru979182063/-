from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
TEST_ASSETS_DIR = ROOT / "test" / "material_card_eval_assets"
REPORTS_DIR = ROOT / "reports"
DISTILL_BATCHES_DIR = REPORTS_DIR / "distill_batches"
PRESSURE_DEPTH1_DIR = REPORTS_DIR / "pressure_tests" / "depth1"

TRUTH_MANIFEST_PATH = TEST_ASSETS_DIR / "truth_segment_sample_manifest.yaml"
MANUAL_CURATION_PATH = TEST_ASSETS_DIR / "manual_sample_curation.yaml"
FAMILY_HIERARCHY_PATH = ROOT / "card_specs" / "normalized" / "runtime_mappings" / "distill_family_hierarchy_mapping.yaml"
MATERIAL_MAPPING_PATH = ROOT / "card_specs" / "normalized" / "runtime_mappings" / "distill_material_card_id_mapping.yaml"
EXPANDED_PREVIOUS_ARTICLES_PATH = PRESSURE_DEPTH1_DIR / "depth1_expanded_all_2_per_group.jsonl"
DIRTY_TRAIN_PATH = DISTILL_BATCHES_DIR / "controlled_dirty_samples_2026-04-15" / "dirty_sample_train.jsonl"
DIRTY_TEST_PATH = DISTILL_BATCHES_DIR / "controlled_dirty_samples_2026-04-15" / "dirty_sample_test.jsonl"

OUT_MANIFEST_PATH = TEST_ASSETS_DIR / "depth1_pressure_batch_manifest.yaml"
OUT_REPORT_PATH = REPORTS_DIR / "pressure_tests" / "depth1" / "depth1_pressure_batch_manifest_report.md"


QUESTION_CARD_PATHS = {
    "center_understanding": ROOT / "card_specs" / "normalized" / "question_cards" / "center_understanding_standard_question_card.normalized.yaml",
    "sentence_fill": ROOT / "card_specs" / "normalized" / "question_cards" / "sentence_fill_standard_question_card.normalized.yaml",
    "sentence_order": ROOT / "card_specs" / "normalized" / "question_cards" / "sentence_order_standard_question_card.normalized.yaml",
}


def _read_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _load_truth_row_lookup() -> dict[str, dict[str, dict[str, Any]]]:
    lookup: dict[str, dict[str, dict[str, Any]]] = {}
    for jsonl_path in sorted(DISTILL_BATCHES_DIR.glob("*/cleaned_truth_materials.jsonl")):
        batch_scope = jsonl_path.parent.name
        current: dict[str, dict[str, Any]] = {}
        for row in _read_jsonl(jsonl_path):
            row_key = _build_row_key(row)
            current[row_key] = row
        lookup[batch_scope] = current
    return lookup


def _build_row_key(row: dict[str, Any]) -> str:
    sample_id = str(row.get("sample_id") or "")
    pattern_tag = str(row.get("pattern_tag") or "")
    source_doc = str(row.get("source_doc") or "")
    return f"{sample_id}::{pattern_tag}::{source_doc}"


def _question_card_feature_pack(question_card: dict[str, Any], expected_material_card_id: str) -> dict[str, Any]:
    overrides = question_card.get("material_card_overrides") or []
    matched_override = {}
    for item in overrides:
        if str(item.get("material_card") or "") == expected_material_card_id:
            matched_override = dict(item.get("slot_overrides") or {})
            break

    family_id = str(question_card.get("business_family_id") or "")
    base_slots = dict(question_card.get("base_slots") or {})
    slot_extensions = dict(question_card.get("slot_extensions") or {})

    if family_id == "sentence_fill":
        base_subset = {
            key: base_slots.get(key)
            for key in ("blank_position", "function_type", "logic_relation", "semantic_scope", "context_dependency", "bidirectional_validation")
            if key in base_slots
        }
        extension_subset = {
            key: slot_extensions.get(key)
            for key in ("backward_link_required", "forward_link_required", "object_match_required", "strongest_distractor_gap")
            if key in slot_extensions
        }
    elif family_id == "sentence_order":
        base_subset = {
            key: base_slots.get(key)
            for key in ("opening_anchor_type", "middle_structure_type", "closing_anchor_type", "block_order_complexity", "candidate_type")
            if key in base_slots
        }
        extension_subset = {
            key: slot_extensions.get(key)
            for key in ("first_sentence_legality_required", "unique_opener_required", "min_binding_pair_count", "sequence_integrity_min", "max_exchange_risk")
            if key in slot_extensions
        }
    else:
        base_subset = {
            key: base_slots.get(key)
            for key in ("argument_structure", "main_axis_source", "abstraction_level", "coverage_requirement", "target_form")
            if key in base_slots
        }
        extension_subset = {
            key: slot_extensions.get(key)
            for key in ("core_object_anchor_required", "require_global_axis_extraction", "strongest_distractor_gap")
            if key in slot_extensions
        }

    return {
        "question_card_id": question_card.get("card_id"),
        "runtime_binding": dict(question_card.get("runtime_binding") or {}),
        "preferred_material_cards": list((question_card.get("upstream_contract") or {}).get("preferred_material_cards") or []),
        "base_slots_subset": base_subset,
        "slot_extensions_subset": extension_subset,
        "target_material_override": matched_override,
    }


def _resolve_runtime_expectation(batch_mapping: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    runtime_row_mapping = dict(batch_mapping.get("runtime_row_mapping") or {})
    for field_name in ("pattern_tag", "subfamily", "source_doc"):
        field_map = dict(runtime_row_mapping.get(field_name) or {})
        value = str(row.get(field_name) or "")
        if value and value in field_map:
            return dict(field_map[value])
    return {}


def _match_child_family(
    row: dict[str, Any],
    hierarchy: dict[str, Any],
    mapping_config: dict[str, Any],
) -> tuple[str, dict[str, Any], dict[str, Any]] | tuple[None, None, None]:
    business_family_id = str(row.get("business_family_id") or "")
    child_families = dict(hierarchy.get("child_families") or {})
    batch_mappings = dict(mapping_config.get("batches") or {})

    for child_family_id, child_payload in child_families.items():
        mother_family_id = str(child_payload.get("mother_family_id") or "")
        if mother_family_id != business_family_id:
            continue
        batch_scope = str(child_payload.get("batch_scope") or "")
        batch_mapping = dict(batch_mappings.get(batch_scope) or {})
        if not batch_mapping:
            continue
        expectation = _resolve_runtime_expectation(batch_mapping, row)
        if expectation:
            return child_family_id, child_payload, expectation
    return None, None, None


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _build_coverage_row(
    *,
    item: dict[str, Any],
    truth_row: dict[str, Any],
    child_family_id: str,
    child_payload: dict[str, Any],
    question_card: dict[str, Any],
    article_text: str,
    article_text_source: str,
) -> dict[str, Any]:
    expected_material_card_id = str(item.get("expected_material_card_id") or "")
    feature_pack = _question_card_feature_pack(question_card, expected_material_card_id)
    material_text = _safe_text(item.get("material_text_override") or truth_row.get("material_text"))
    return {
        "sample_id": item.get("sample_id"),
        "question_id": item.get("question_id"),
        "row_key": item.get("row_key"),
        "split": item.get("split"),
        "business_family_id": child_payload.get("mother_family_id"),
        "mother_family_id": child_payload.get("mother_family_id"),
        "child_family_id": child_family_id,
        "batch_scope": child_payload.get("batch_scope"),
        "question_card_id": question_card.get("card_id"),
        "expected_material_card_id": expected_material_card_id,
        "subfamily": truth_row.get("subfamily"),
        "pattern_tag": truth_row.get("pattern_tag"),
        "source_doc": truth_row.get("source_doc"),
        "group_key": item.get("group_key"),
        "material_text": material_text,
        "article_text": article_text,
        "article_text_source": article_text_source,
        "article_ready_state": "ready_previous_expanded" if article_text_source == "previous_expanded_article" else "fallback_original_material",
        "question_card_features": feature_pack,
        "truth_blank_position": item.get("truth_blank_position"),
        "truth_function_type": item.get("truth_function_type"),
        "repair_notes": list(item.get("repair_notes") or []),
    }


def _build_previous_article_row(
    *,
    row: dict[str, Any],
    child_family_id: str,
    child_payload: dict[str, Any],
    question_card: dict[str, Any],
    expectation: dict[str, Any],
) -> dict[str, Any]:
    expected_material_card_id = str(expectation.get("material_card_id") or "")
    feature_pack = _question_card_feature_pack(question_card, expected_material_card_id)
    original_text = _safe_text(row.get("original_text"))
    expanded_text = _safe_text(row.get("expanded_text"))
    return {
        "sample_id": row.get("sample_id"),
        "question_id": str(row.get("sample_id") or "").split(".")[-1],
        "row_key": f"{row.get('sample_id')}::{row.get('pattern_tag') or ''}::",
        "split": "previous_articles",
        "business_family_id": row.get("business_family_id"),
        "mother_family_id": child_payload.get("mother_family_id"),
        "child_family_id": child_family_id,
        "batch_scope": child_payload.get("batch_scope"),
        "question_card_id": question_card.get("card_id"),
        "expected_material_card_id": expected_material_card_id,
        "subfamily": row.get("subfamily"),
        "pattern_tag": row.get("pattern_tag"),
        "source_doc": row.get("source_doc"),
        "group_key": f"{row.get('business_family_id')}||{row.get('subfamily') or ''}||{row.get('pattern_tag') or ''}",
        "material_text": original_text,
        "article_text": expanded_text or original_text,
        "article_text_source": "previous_expanded_article" if expanded_text else "original_material_text",
        "article_ready_state": "ready_previous_expanded" if expanded_text else "fallback_original_material",
        "question_card_features": feature_pack,
        "truth_blank_position": expectation.get("truth_blank_position"),
        "truth_function_type": expectation.get("truth_function_type"),
        "repair_notes": [],
    }


def _build_dirty_probe_row(
    *,
    row: dict[str, Any],
    question_card: dict[str, Any],
) -> dict[str, Any]:
    gold_sample_id = _safe_text(row.get("gold_sample_id"))
    if ".center_understanding." in gold_sample_id:
        mother_family_id = "center_understanding"
    elif ".sentence_order." in gold_sample_id:
        mother_family_id = "sentence_order"
    elif ".sentence_fill." in gold_sample_id:
        mother_family_id = "sentence_fill"
    else:
        source_family = _safe_text(row.get("source_family"))
        if "中心理解" in source_family:
            mother_family_id = "center_understanding"
        elif "语句排序" in source_family:
            mother_family_id = "sentence_order"
        else:
            mother_family_id = "sentence_fill"

    feature_pack = _question_card_feature_pack(question_card, "")
    return {
        "sample_id": row.get("sample_id"),
        "question_id": row.get("gold_sample_id"),
        "row_key": row.get("sample_id"),
        "split": "dirty_probe",
        "business_family_id": mother_family_id,
        "mother_family_id": mother_family_id,
        "child_family_id": None,
        "batch_scope": "controlled_dirty_samples_2026-04-15",
        "question_card_id": question_card.get("card_id"),
        "expected_material_card_id": "",
        "subfamily": row.get("source_subfamily"),
        "pattern_tag": row.get("source_pattern_tag"),
        "source_doc": row.get("source_doc"),
        "group_key": row.get("primary_dirty_state"),
        "material_text": _safe_text(row.get("gold_text")),
        "article_text": _safe_text(row.get("dirty_text")),
        "article_text_source": "controlled_dirty_article",
        "article_ready_state": "ready_dirty_probe",
        "question_card_features": feature_pack,
        "dirty_state": row.get("primary_dirty_state"),
        "recommended_action": row.get("recommended_action"),
        "severity": row.get("severity"),
        "repair_notes": [str(row.get("why_dirty") or "")],
    }


def main() -> int:
    family_hierarchy = _read_yaml(FAMILY_HIERARCHY_PATH)
    material_mapping = _read_yaml(MATERIAL_MAPPING_PATH)
    truth_manifest = _read_yaml(TRUTH_MANIFEST_PATH)
    question_cards = {
        family_id: _read_yaml(path)
        for family_id, path in QUESTION_CARD_PATHS.items()
    }
    truth_row_lookup = _load_truth_row_lookup()
    previous_expanded_rows = _read_jsonl(EXPANDED_PREVIOUS_ARTICLES_PATH)
    previous_expanded_by_sample_id = {
        str(row.get("sample_id") or ""): row
        for row in previous_expanded_rows
    }

    execution_batches: dict[str, dict[str, Any]] = {}
    child_family_stats: dict[str, Counter[str]] = defaultdict(Counter)

    child_families = dict(family_hierarchy.get("child_families") or {})
    truth_batches = dict(truth_manifest.get("batches") or {})
    for child_family_id, child_payload in child_families.items():
        batch_scope = str(child_payload.get("batch_scope") or "")
        truth_batch = dict(truth_batches.get(batch_scope) or {})
        truth_rows = dict(truth_row_lookup.get(batch_scope) or {})
        mother_family_id = str(child_payload.get("mother_family_id") or "")
        question_card = question_cards[mother_family_id]

        for split_name in ("train", "holdout", "negative_probe"):
            selected_rows: list[dict[str, Any]] = []
            for item in truth_batch.get(split_name) or []:
                row_key = str(item.get("row_key") or "")
                truth_row = dict(truth_rows.get(row_key) or {})
                if not truth_row:
                    continue
                sample_id = str(item.get("sample_id") or "")
                previous_expanded = previous_expanded_by_sample_id.get(sample_id) or {}
                article_text = _safe_text(previous_expanded.get("expanded_text"))
                article_text_source = "previous_expanded_article" if article_text else "original_material_text"
                if not article_text:
                    article_text = _safe_text(item.get("material_text_override") or truth_row.get("material_text"))
                selected_rows.append(
                    _build_coverage_row(
                        item=item,
                        truth_row=truth_row,
                        child_family_id=child_family_id,
                        child_payload=child_payload,
                        question_card=question_card,
                        article_text=article_text,
                        article_text_source=article_text_source,
                    )
                )

            if not selected_rows:
                continue

            batch_id = f"coverage_{split_name}__{child_family_id}"
            execution_batches[batch_id] = {
                "batch_id": batch_id,
                "batch_kind": "coverage",
                "split": split_name,
                "mother_family_id": mother_family_id,
                "child_family_id": child_family_id,
                "batch_scope": batch_scope,
                "question_card_id": question_card.get("card_id"),
                "row_count": len(selected_rows),
                "ready_previous_expanded_count": sum(1 for row in selected_rows if row["article_text_source"] == "previous_expanded_article"),
                "fallback_original_count": sum(1 for row in selected_rows if row["article_text_source"] != "previous_expanded_article"),
                "rows": selected_rows,
            }
            child_family_stats[child_family_id]["coverage_rows"] += len(selected_rows)
            child_family_stats[child_family_id]["coverage_previous_ready"] += execution_batches[batch_id]["ready_previous_expanded_count"]

    previous_batches_grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in previous_expanded_rows:
        child_family_id, child_payload, expectation = _match_child_family(row, family_hierarchy, material_mapping)
        if not child_family_id or not child_payload or not expectation:
            continue
        mother_family_id = str(child_payload.get("mother_family_id") or "")
        question_card = question_cards[mother_family_id]
        previous_batches_grouped[child_family_id].append(
            _build_previous_article_row(
                row=row,
                child_family_id=child_family_id,
                child_payload=child_payload,
                question_card=question_card,
                expectation=expectation,
            )
        )

    for child_family_id, rows in sorted(previous_batches_grouped.items()):
        child_payload = child_families[child_family_id]
        mother_family_id = str(child_payload.get("mother_family_id") or "")
        question_card = question_cards[mother_family_id]
        batch_id = f"previous_articles__{child_family_id}"
        execution_batches[batch_id] = {
            "batch_id": batch_id,
            "batch_kind": "previous_articles",
            "split": "previous_articles",
            "mother_family_id": mother_family_id,
            "child_family_id": child_family_id,
            "batch_scope": child_payload.get("batch_scope"),
            "question_card_id": question_card.get("card_id"),
            "row_count": len(rows),
            "ready_previous_expanded_count": len(rows),
            "fallback_original_count": 0,
            "rows": rows,
        }
        child_family_stats[child_family_id]["previous_article_rows"] += len(rows)

    for dirty_path, batch_kind in ((DIRTY_TRAIN_PATH, "dirty_train"), (DIRTY_TEST_PATH, "dirty_holdout")):
        dirty_rows = _read_jsonl(dirty_path)
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in dirty_rows:
            gold_sample_id = _safe_text(row.get("gold_sample_id"))
            if ".center_understanding." in gold_sample_id:
                mother_family_id = "center_understanding"
            elif ".sentence_order." in gold_sample_id:
                mother_family_id = "sentence_order"
            elif ".sentence_fill." in gold_sample_id:
                mother_family_id = "sentence_fill"
            else:
                source_family = _safe_text(row.get("source_family"))
                if "中心理解" in source_family:
                    mother_family_id = "center_understanding"
                elif "语句排序" in source_family:
                    mother_family_id = "sentence_order"
                else:
                    mother_family_id = "sentence_fill"
            question_card = question_cards[mother_family_id]
            grouped[mother_family_id].append(_build_dirty_probe_row(row=row, question_card=question_card))

        for mother_family_id, rows in grouped.items():
            batch_id = f"{batch_kind}__{mother_family_id}"
            execution_batches[batch_id] = {
                "batch_id": batch_id,
                "batch_kind": "dirty_probe",
                "split": batch_kind,
                "mother_family_id": mother_family_id,
                "child_family_id": None,
                "batch_scope": "controlled_dirty_samples_2026-04-15",
                "question_card_id": question_cards[mother_family_id].get("card_id"),
                "row_count": len(rows),
                "ready_previous_expanded_count": len(rows),
                "fallback_original_count": 0,
                "rows": rows,
            }

    batch_counts = Counter(payload["batch_kind"] for payload in execution_batches.values())
    manifest = {
        "schema_version": "depth1_pressure_batch_manifest.v1",
        "source_files": {
            "family_hierarchy": str(FAMILY_HIERARCHY_PATH),
            "material_mapping": str(MATERIAL_MAPPING_PATH),
            "truth_segment_manifest": str(TRUTH_MANIFEST_PATH),
            "manual_curation": str(MANUAL_CURATION_PATH),
            "previous_expanded_articles": str(EXPANDED_PREVIOUS_ARTICLES_PATH),
            "dirty_train": str(DIRTY_TRAIN_PATH),
            "dirty_test": str(DIRTY_TEST_PATH),
        },
        "batching_policy": {
            "coverage_batches": "per_child_family_per_split",
            "previous_article_batches": "per_child_family_previous_articles_only",
            "dirty_probe_batches": "per_mother_family_per_split",
            "recommended_execution_order": [
                "previous_articles__*",
                "coverage_train__*",
                "coverage_holdout__*",
                "coverage_negative_probe__*",
                "dirty_train__*",
                "dirty_holdout__*",
            ],
        },
        "stats": {
            "execution_batch_count": len(execution_batches),
            "batch_kind_counts": dict(batch_counts),
            "child_family_stats": {key: dict(value) for key, value in sorted(child_family_stats.items())},
        },
        "execution_batches": dict(sorted(execution_batches.items())),
    }

    OUT_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_MANIFEST_PATH.write_text(yaml.safe_dump(manifest, allow_unicode=True, sort_keys=False), encoding="utf-8")

    report_lines = [
        "# Depth1 Pressure Batch Manifest",
        "",
        f"- execution_batch_count: `{len(execution_batches)}`",
        f"- previous_article_batch_count: `{batch_counts.get('previous_articles', 0)}`",
        f"- coverage_batch_count: `{batch_counts.get('coverage', 0)}`",
        f"- dirty_probe_batch_count: `{batch_counts.get('dirty_probe', 0)}`",
        "",
        "## Child Family Coverage",
    ]
    for child_family_id, stats in sorted(child_family_stats.items()):
        report_lines.append(
            "- "
            f"`{child_family_id}` "
            f"coverage_rows=`{stats.get('coverage_rows', 0)}` "
            f"previous_article_rows=`{stats.get('previous_article_rows', 0)}` "
            f"coverage_previous_ready=`{stats.get('coverage_previous_ready', 0)}`"
        )

    OUT_REPORT_PATH.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print(OUT_MANIFEST_PATH)
    print(OUT_REPORT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
