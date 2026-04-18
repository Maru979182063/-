from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
PRESSURE_DEPTH1_DIR = ROOT / "reports" / "pressure_tests" / "depth1"
TEST_ASSETS_DIR = ROOT / "test" / "material_card_eval_assets"
EXPANDED_INPUT_PATH = PRESSURE_DEPTH1_DIR / "depth1_expanded_all_2_per_group.jsonl"
FAMILY_HIERARCHY_PATH = ROOT / "card_specs" / "normalized" / "runtime_mappings" / "distill_family_hierarchy_mapping.yaml"
MATERIAL_MAPPING_PATH = ROOT / "card_specs" / "normalized" / "runtime_mappings" / "distill_material_card_id_mapping.yaml"
OUT_MANIFEST_PATH = TEST_ASSETS_DIR / "depth1_expanded_split_manifest.yaml"
OUT_REPORT_PATH = PRESSURE_DEPTH1_DIR / "depth1_expanded_split_manifest_report.md"


QUESTION_CARD_PATHS = {
    "center_understanding": ROOT / "card_specs" / "normalized" / "question_cards" / "center_understanding_standard_question_card.normalized.yaml",
    "sentence_fill": ROOT / "card_specs" / "normalized" / "question_cards" / "sentence_fill_standard_question_card.normalized.yaml",
    "sentence_order": ROOT / "card_specs" / "normalized" / "question_cards" / "sentence_order_standard_question_card.normalized.yaml",
}

SHARD_SIZE_BY_FAMILY = {
    "center_understanding": 8,
    "sentence_fill": 6,
    "sentence_order": 8,
}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _read_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


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
    child_families = dict(hierarchy.get("child_families") or {})
    batch_mappings = dict(mapping_config.get("batches") or {})
    business_family_id = str(row.get("business_family_id") or "")
    for child_family_id, child_payload in child_families.items():
        if str(child_payload.get("mother_family_id") or "") != business_family_id:
            continue
        batch_scope = str(child_payload.get("batch_scope") or "")
        batch_mapping = dict(batch_mappings.get(batch_scope) or {})
        if not batch_mapping:
            continue
        expectation = _resolve_runtime_expectation(batch_mapping, row)
        if expectation:
            return child_family_id, child_payload, expectation
    return None, None, None


def _question_card_feature_pack(question_card: dict[str, Any], expected_material_card_id: str) -> dict[str, Any]:
    overrides = question_card.get("material_card_overrides") or []
    matched_override = {}
    for item in overrides:
        if str(item.get("material_card") or "") == expected_material_card_id:
            matched_override = dict(item.get("slot_overrides") or {})
            break

    family_id = str(question_card.get("business_family_id") or "")
    base_slots = dict(question_card.get("base_slots") or {})
    if family_id == "sentence_fill":
        base_subset = {
            key: base_slots.get(key)
            for key in ("blank_position", "function_type", "logic_relation", "semantic_scope")
            if key in base_slots
        }
    elif family_id == "sentence_order":
        base_subset = {
            key: base_slots.get(key)
            for key in ("opening_anchor_type", "middle_structure_type", "closing_anchor_type", "block_order_complexity")
            if key in base_slots
        }
    else:
        base_subset = {
            key: base_slots.get(key)
            for key in ("argument_structure", "main_axis_source", "abstraction_level")
            if key in base_slots
        }

    return {
        "question_card_id": question_card.get("card_id"),
        "runtime_binding": dict(question_card.get("runtime_binding") or {}),
        "preferred_material_cards": list((question_card.get("upstream_contract") or {}).get("preferred_material_cards") or []),
        "base_slots_subset": base_subset,
        "target_material_override": matched_override,
    }


def _chunk_rows(rows: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [rows[idx : idx + size] for idx in range(0, len(rows), size)]


def _round_robin_interleave(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_child_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_child_family[str(row.get("child_family_id") or "")].append(row)
    for child_family_id, members in by_child_family.items():
        by_child_family[child_family_id] = sorted(
            members,
            key=lambda item: (
                str(item.get("pattern_tag") or ""),
                str(item.get("sample_id") or ""),
            ),
        )

    ordered_child_family_ids = sorted(by_child_family)
    interleaved: list[dict[str, Any]] = []
    while True:
        appended = False
        for child_family_id in ordered_child_family_ids:
            bucket = by_child_family[child_family_id]
            if not bucket:
                continue
            interleaved.append(bucket.pop(0))
            appended = True
        if not appended:
            break
    return interleaved


def main() -> int:
    hierarchy = _read_yaml(FAMILY_HIERARCHY_PATH)
    material_mapping = _read_yaml(MATERIAL_MAPPING_PATH)
    question_cards = {
        family_id: _read_yaml(path)
        for family_id, path in QUESTION_CARD_PATHS.items()
    }
    rows = _read_jsonl(EXPANDED_INPUT_PATH)

    grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    child_family_stats: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        group_key = f"{row.get('business_family_id')}||{row.get('subfamily') or ''}||{row.get('pattern_tag') or ''}"
        grouped_rows[group_key].append(dict(row))

    split_rows: list[dict[str, Any]] = []
    for group_key, members in sorted(grouped_rows.items()):
        ordered = sorted(members, key=lambda item: str(item.get("sample_id") or ""))
        if len(ordered) != 2:
            continue
        for split_name, row in zip(("train", "holdout"), ordered):
            child_family_id, child_payload, expectation = _match_child_family(row, hierarchy, material_mapping)
            if not child_family_id:
                continue
            mother_family_id = str(child_payload.get("mother_family_id") or "")
            question_card = question_cards[mother_family_id]
            expected_material_card_id = str(expectation.get("material_card_id") or "")
            split_rows.append(
                {
                    "split": split_name,
                    "sample_id": row.get("sample_id"),
                    "group_key": group_key,
                    "business_family_id": mother_family_id,
                    "mother_family_id": mother_family_id,
                    "child_family_id": child_family_id,
                    "batch_scope": child_payload.get("batch_scope"),
                    "question_card_id": question_card.get("card_id"),
                    "expected_material_card_id": expected_material_card_id,
                    "subfamily": row.get("subfamily"),
                    "pattern_tag": row.get("pattern_tag"),
                    "original_text": row.get("original_text"),
                    "article_text": row.get("expanded_text"),
                    "article_text_source": "self_expanded_truth_article",
                    "question_card_features": _question_card_feature_pack(question_card, expected_material_card_id),
                    "truth_blank_position": expectation.get("truth_blank_position"),
                    "truth_function_type": expectation.get("truth_function_type"),
                }
            )
            child_family_stats[child_family_id][split_name] += 1

    execution_batches: dict[str, dict[str, Any]] = {}
    family_split_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in split_rows:
        family_split_groups[(row["business_family_id"], row["split"])].append(row)

    for (family_id, split_name), members in sorted(family_split_groups.items()):
        shard_size = SHARD_SIZE_BY_FAMILY.get(family_id, 8)
        ordered = _round_robin_interleave(members)
        for shard_index, chunk in enumerate(_chunk_rows(ordered, shard_size), start=1):
            batch_id = f"{split_name}__{family_id}__shard{shard_index:02d}"
            execution_batches[batch_id] = {
                "batch_id": batch_id,
                "batch_kind": "expanded_truth_eval",
                "split": split_name,
                "mother_family_id": family_id,
                "row_count": len(chunk),
                "child_family_ids": sorted({str(row.get('child_family_id') or '') for row in chunk}),
                "rows": chunk,
            }

    manifest = {
        "schema_version": "depth1_expanded_split_manifest.v1",
        "source_files": {
            "expanded_input": str(EXPANDED_INPUT_PATH),
            "family_hierarchy": str(FAMILY_HIERARCHY_PATH),
            "material_mapping": str(MATERIAL_MAPPING_PATH),
        },
        "split_policy": {
            "group_unit": "business_family_id||subfamily||pattern_tag",
            "assignment_rule": "sorted sample_id -> first=train, second=holdout",
            "article_source": "self_expanded_truth_article_only",
        },
        "stats": {
            "group_count": len(grouped_rows),
            "row_count": len(split_rows),
            "train_count": sum(1 for row in split_rows if row["split"] == "train"),
            "holdout_count": sum(1 for row in split_rows if row["split"] == "holdout"),
            "child_family_stats": {key: dict(value) for key, value in sorted(child_family_stats.items())},
            "child_family_distribution": {
                "train_min": min((value.get("train", 0) for value in child_family_stats.values()), default=0),
                "train_max": max((value.get("train", 0) for value in child_family_stats.values()), default=0),
                "holdout_min": min((value.get("holdout", 0) for value in child_family_stats.values()), default=0),
                "holdout_max": max((value.get("holdout", 0) for value in child_family_stats.values()), default=0),
            },
            "execution_batch_count": len(execution_batches),
        },
        "execution_batches": dict(sorted(execution_batches.items())),
    }

    OUT_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_MANIFEST_PATH.write_text(yaml.safe_dump(manifest, allow_unicode=True, sort_keys=False), encoding="utf-8")

    report_lines = [
        "# Depth1 Expanded Split Manifest",
        "",
        f"- group_count: `{manifest['stats']['group_count']}`",
        f"- row_count: `{manifest['stats']['row_count']}`",
        f"- train_count: `{manifest['stats']['train_count']}`",
        f"- holdout_count: `{manifest['stats']['holdout_count']}`",
        f"- execution_batch_count: `{manifest['stats']['execution_batch_count']}`",
        "",
        "## Child Family Split Coverage",
    ]
    for child_family_id, counters in sorted(child_family_stats.items()):
        report_lines.append(
            f"- `{child_family_id}` train=`{counters.get('train', 0)}` holdout=`{counters.get('holdout', 0)}`"
        )
    distribution = manifest["stats"]["child_family_distribution"]
    report_lines.extend(
        [
            "",
            "## Distribution Note",
            f"- source-level train child-family min=`{distribution['train_min']}` max=`{distribution['train_max']}`",
            f"- source-level holdout child-family min=`{distribution['holdout_min']}` max=`{distribution['holdout_max']}`",
            "- execution batches are round-robin interleaved by child family inside each mother-family split to reduce per-round skew.",
        ]
    )
    OUT_REPORT_PATH.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print(OUT_MANIFEST_PATH)
    print(OUT_REPORT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
