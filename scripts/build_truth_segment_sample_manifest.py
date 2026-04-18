from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
DISTILL_BATCHES_DIR = ROOT / "reports" / "distill_batches"
MAPPING_CONFIG_PATH = ROOT / "card_specs" / "normalized" / "runtime_mappings" / "distill_material_card_id_mapping.yaml"
DEFAULT_OUTPUT_PATH = ROOT / "test" / "material_card_eval_assets" / "truth_segment_sample_manifest.yaml"
MANUAL_CURATION_PATH = ROOT / "test" / "material_card_eval_assets" / "manual_sample_curation.yaml"

TRAIN_COUNT_PER_BATCH = 10
HOLDOUT_COUNT_PER_BATCH = 5


def _group_key(row: dict[str, Any]) -> str:
    for field_name in ("pattern_tag", "subfamily", "source_doc"):
        value = str(row.get(field_name) or "").strip()
        if value:
            return f"{field_name}:{value}"
    return "ungrouped"


def _row_key(row: dict[str, Any]) -> str:
    sample_id = str(row.get("sample_id") or "")
    pattern_tag = str(row.get("pattern_tag") or "")
    source_doc = str(row.get("source_doc") or "")
    return f"{sample_id}::{pattern_tag}::{source_doc}"


def _iter_truth_rows(batch_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with (batch_dir / "cleaned_truth_materials.jsonl").open("r", encoding="utf-8-sig") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    rows.sort(key=lambda row: str(row.get("sample_id") or row.get("question_id") or ""))
    return rows


def _load_rebuilt_lookup(batch_dir: Path) -> dict[str, dict[str, Any]]:
    rebuilt_path = batch_dir / "material_samples_rebuilt.jsonl"
    if not rebuilt_path.exists():
        return {}
    lookup: dict[str, dict[str, Any]] = {}
    with rebuilt_path.open("r", encoding="utf-8-sig") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line:
                continue
            row = json.loads(line)
            question_id = str(row.get("question_id") or "")
            if question_id:
                lookup[question_id] = row
    return lookup


def _load_manual_curation() -> dict[str, Any]:
    if not MANUAL_CURATION_PATH.exists():
        return {}
    return yaml.safe_load(MANUAL_CURATION_PATH.read_text(encoding="utf-8")) or {}


def _manual_text_override(curated_batch: dict[str, Any], question_id: str) -> str | None:
    overrides = dict(curated_batch.get("text_overrides") or {})
    value = overrides.get(str(question_id))
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _resolve_expectation(payload: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    runtime_row_mapping = dict(payload.get("runtime_row_mapping") or {})
    for field_name in ("pattern_tag", "subfamily", "source_doc"):
        value = str(row.get(field_name) or "").strip()
        if not value:
            continue
        matched = ((runtime_row_mapping.get(field_name) or {}).get(value) or {})
        if matched:
            return dict(matched)
    return {}


def _strip_fill_prompt_tail(text: str) -> tuple[str, list[str]]:
    cleaned = text.strip()
    notes: list[str] = []
    markers = (
        "填入画横线部分最恰当的一项是",
        "填入画横线处最恰当的一项是",
        "填入横线部分最恰当的一项是",
        "填入横线中最合适的一项是",
        "填入括号部分最恰当的一项是",
        "文中括号处应引用的句子是",
        "阅读上述句子，括号处应该填入的是",
        "以下句子填入画横线处，最恰当的是",
        "填入画横线部分最恰当的一句是",
        "填入画横线处最恰当的一句是",
        "填入画横线部分最为合适的一句是",
    )
    for marker in markers:
        idx = cleaned.find(marker)
        if idx >= 0:
            cleaned = cleaned[:idx].rstrip()
            notes.append(f"strip_tail:{marker}")
            break
    return cleaned, notes


def _repair_fill_text(base_text: str, answer_text: str) -> tuple[str, list[str]]:
    text = base_text.strip()
    notes: list[str] = []
    answer = answer_text.strip()
    if not text:
        return text, notes
    if answer:
        replaced = re.sub(r"\s+([。；，：？！])", f"{answer}\\1", text, count=1)
        if replaced != text:
            text = replaced
            notes.append("fill_blank_before_punct")
        elif text[:1] in "。；，：？！":
            text = f"{answer}{text}"
            notes.append("prepend_answer_before_leading_punct")
        elif "“ ”" in text:
            text = text.replace("“ ”", f"“{answer}”", 1)
            notes.append("fill_empty_quote")
        elif "“”" in text:
            text = text.replace("“”", f"“{answer}”", 1)
            notes.append("fill_empty_quote")
        elif "________" in text or "___" in text:
            text = re.sub(r"_{2,}", answer, text, count=1)
            notes.append("fill_underscore_blank")
    text = re.sub(r'^(“[^”]+”)(?=[\u4e00-\u9fff])', r"\1。", text, count=1)
    if notes and not text.endswith(("。", "！", "？", "”")):
        text = text.rstrip("，；： ")
    return text, notes


def _maybe_build_text_override(
    batch_name: str,
    row: dict[str, Any],
    rebuilt_lookup: dict[str, dict[str, Any]],
) -> tuple[str | None, list[str]]:
    if not batch_name.startswith("sentence_fill_"):
        return None, []
    rebuilt = rebuilt_lookup.get(str(row.get("question_id") or ""))
    if not rebuilt:
        return None, []
    base_text = str(rebuilt.get("material_rebuilt") or row.get("material_text") or "").strip()
    if not base_text:
        return None, []
    stripped, notes = _strip_fill_prompt_tail(base_text)
    repaired, repair_notes = _repair_fill_text(stripped, str(rebuilt.get("correct_option_text") or ""))
    all_notes = notes + repair_notes
    if repaired and repaired != str(row.get("material_text") or "").strip():
        return repaired, all_notes
    return None, all_notes


def _negative_reasons(batch_dir: str, row: dict[str, Any]) -> list[str]:
    text = str(row.get("_material_text_override") or row.get("material_text") or "").strip()
    reasons: list[str] = []
    if not text:
        reasons.append("empty_text")
        return reasons
    prompt_markers = (
        "填入题中画横线部分最合适的一项是",
        "填入横线部分最恰当的一句是",
        "填入画横线部分最恰当的一项是",
        "最合适的一项是",
        "横线部分",
    )
    if any(marker in text for marker in prompt_markers):
        reasons.append("question_stem_residue")
    unresolved_markers = ("“ ”", "“”", "___", "____", "______", "( )")
    if any(marker in text for marker in unresolved_markers):
        reasons.append("unresolved_blank")
    if "sentence_fill_head_start" in batch_dir and text[:1] in {"。", "，", "；", "、", "："}:
        reasons.append("truncated_opening")
    return reasons

def _round_robin_pick(
    grouped_rows: dict[str, list[dict[str, Any]]],
    target_count: int,
    *,
    seen_sample_ids: set[str],
) -> list[dict[str, Any]]:
    picked: list[dict[str, Any]] = []
    group_names = sorted(grouped_rows)
    while len(picked) < target_count:
        progress = False
        for group_name in group_names:
            bucket = grouped_rows[group_name]
            while bucket and str(bucket[0].get("sample_id") or "") in seen_sample_ids:
                bucket.pop(0)
            if not bucket:
                continue
            row = bucket.pop(0)
            sample_id = str(row.get("sample_id") or "")
            if sample_id:
                seen_sample_ids.add(sample_id)
            picked.append(row)
            progress = True
            if len(picked) >= target_count:
                break
        if not progress:
            break
    return picked


def main() -> None:
    mapping = yaml.safe_load(MAPPING_CONFIG_PATH.read_text(encoding="utf-8"))
    manual_curation = _load_manual_curation()
    manifest: dict[str, Any] = {
        "schema_version": "truth_segment_sample_manifest.v1",
        "source_mapping": str(MAPPING_CONFIG_PATH),
        "selection_policy": {
            "train_count_per_batch": TRAIN_COUNT_PER_BATCH,
            "holdout_count_per_batch": HOLDOUT_COUNT_PER_BATCH,
            "group_key_priority": ["pattern_tag", "subfamily", "source_doc"],
            "distribution": "round_robin_by_group",
        },
        "batches": {},
    }

    for batch_dir, payload in sorted((mapping.get("batches") or {}).items()):
        batch_path = DISTILL_BATCHES_DIR / batch_dir
        rebuilt_lookup = _load_rebuilt_lookup(batch_path)
        curated_batch = dict((manual_curation.get("batches") or {}).get(batch_dir) or {})
        rows = _iter_truth_rows(batch_path)
        clean_rows: list[dict[str, Any]] = []
        negative_rows: list[dict[str, Any]] = []
        for row in rows:
            text_override, repair_notes = _maybe_build_text_override(batch_dir, row, rebuilt_lookup)
            manual_text_override = _manual_text_override(curated_batch, str(row.get("question_id") or ""))
            if manual_text_override:
                row = dict(row)
                row["_material_text_override"] = manual_text_override
                existing_notes = list(row.get("_repair_notes") or repair_notes or [])
                existing_notes.append("manual_text_override")
                row["_repair_notes"] = existing_notes
            elif text_override:
                row = dict(row)
                row["_material_text_override"] = text_override
                row["_repair_notes"] = repair_notes
            reasons = _negative_reasons(batch_dir, row)
            if reasons:
                row = dict(row)
                row["_negative_reasons"] = reasons
                negative_rows.append(row)
            else:
                clean_rows.append(row)
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in clean_rows:
            grouped.setdefault(_group_key(row), []).append(row)

        row_by_question_id = {str(row.get("question_id") or ""): row for row in clean_rows}
        if curated_batch:
            train_rows = [
                row_by_question_id[qid]
                for qid in (curated_batch.get("train_question_ids") or [])
                if qid in row_by_question_id
            ]
            holdout_rows = [
                row_by_question_id[qid]
                for qid in (curated_batch.get("holdout_question_ids") or [])
                if qid in row_by_question_id
            ]
        else:
            working_groups = {name: list(bucket) for name, bucket in grouped.items()}
            seen_sample_ids: set[str] = set()
            train_rows = _round_robin_pick(
                working_groups,
                TRAIN_COUNT_PER_BATCH,
                seen_sample_ids=seen_sample_ids,
            )
            holdout_rows = _round_robin_pick(
                working_groups,
                HOLDOUT_COUNT_PER_BATCH,
                seen_sample_ids=seen_sample_ids,
            )

        def _serialize(row: dict[str, Any], split: str) -> dict[str, Any]:
            expectation = _resolve_expectation(payload, row)
            return {
                "split": split,
                "row_key": _row_key(row),
                "sample_id": str(row.get("sample_id") or ""),
                "question_id": str(row.get("question_id") or ""),
                "source_doc": str(row.get("source_doc") or ""),
                "subfamily": str(row.get("subfamily") or ""),
                "pattern_tag": str(row.get("pattern_tag") or ""),
                "group_key": _group_key(row),
                "expected_material_card_id": str(expectation.get("material_card_id") or ""),
                "material_text_override": str(row.get("_material_text_override") or ""),
                "repair_notes": list(row.get("_repair_notes") or []),
            }

        manifest["batches"][batch_dir] = {
            "business_family_id": payload.get("business_family_id"),
            "question_card_id": payload.get("question_card_id"),
            "curation_mode": "manual" if curated_batch else "auto_round_robin",
            "stats": {
                "total_rows": len(rows),
                "clean_rows": len(clean_rows),
                "negative_rows": len(negative_rows),
                "group_count": len(grouped),
                "train_count": len(train_rows),
                "holdout_count": len(holdout_rows),
            },
            "train": [_serialize(row, "train") for row in train_rows],
            "holdout": [_serialize(row, "holdout") for row in holdout_rows],
            "negative_probe": [
                {
                    "row_key": _row_key(row),
                    "sample_id": str(row.get("sample_id") or ""),
                    "question_id": str(row.get("question_id") or ""),
                    "source_doc": str(row.get("source_doc") or ""),
                    "subfamily": str(row.get("subfamily") or ""),
                    "pattern_tag": str(row.get("pattern_tag") or ""),
                    "group_key": _group_key(row),
                    "negative_reasons": list(row.get("_negative_reasons") or []),
                    "material_text_override": str(row.get("_material_text_override") or ""),
                    "repair_notes": list(row.get("_repair_notes") or []),
                }
                for row in negative_rows[:5]
            ],
        }

    DEFAULT_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    DEFAULT_OUTPUT_PATH.write_text(
        yaml.safe_dump(manifest, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    print(DEFAULT_OUTPUT_PATH)


if __name__ == "__main__":
    main()
