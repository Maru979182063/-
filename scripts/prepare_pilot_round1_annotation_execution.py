from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
INPUT_CANDIDATES = ROOT / "reports" / "pilot_round1_annotation_candidate_pool_2026-04-12.csv"
INPUT_BLOCKED = ROOT / "reports" / "pilot_round1_blocked_pool_2026-04-12.csv"

OUTPUT_MASTER = ROOT / "reports" / "pilot_round1_annotation_execution_master_2026-04-12.csv"
OUTPUT_ERROR_POOL = ROOT / "reports" / "pilot_round1_error_pool_locked_2026-04-12.csv"
OUTPUT_BATCH_DIR = ROOT / "reports" / "pilot_round1_annotation_batches_2026-04-12"
OUTPUT_BATCH_MANIFEST = ROOT / "reports" / "pilot_round1_annotation_batch_manifest_2026-04-12.csv"


FAMILY_BATCH_SIZE = {
    "sentence_fill": 10,
    "center_understanding": 10,
    "sentence_order": 10,
}

FAMILY_BATCH_PREFIX = {
    "sentence_fill": "sf",
    "center_understanding": "cu",
    "sentence_order": "so",
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def chunked(rows: list[dict[str, str]], size: int) -> list[list[dict[str, str]]]:
    return [rows[idx : idx + size] for idx in range(0, len(rows), size)]


def annotation_fields_for_family(family: str) -> list[str]:
    if family == "sentence_fill":
        return [
            "annotation_blank_position",
            "annotation_function_type",
            "annotation_logic_relation",
        ]
    if family == "center_understanding":
        return [
            "annotation_main_axis_source",
            "annotation_argument_structure",
        ]
    if family == "sentence_order":
        return [
            "annotation_candidate_type",
            "annotation_opening_anchor_type",
            "annotation_closing_anchor_type",
        ]
    return []


def build_master_rows(candidate_rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows_by_family: dict[str, list[dict[str, str]]] = {
        family: [row for row in candidate_rows if row["business_family_id"] == family]
        for family in ("sentence_fill", "center_understanding", "sentence_order")
    }

    master_rows: list[dict[str, Any]] = []
    batch_manifest_rows: list[dict[str, Any]] = []

    for family in ("sentence_fill", "center_understanding", "sentence_order"):
        family_rows = rows_by_family[family]
        batches = chunked(family_rows, FAMILY_BATCH_SIZE[family])
        for batch_no, batch_rows in enumerate(batches, start=1):
            batch_id = f"pilot.r1.annot.{FAMILY_BATCH_PREFIX[family]}.b{batch_no:02d}"
            batch_manifest_rows.append(
                {
                    "batch_id": batch_id,
                    "business_family_id": family,
                    "batch_size": len(batch_rows),
                    "annotation_fields": ",".join(annotation_fields_for_family(family)),
                }
            )
            for row in batch_rows:
                payload: dict[str, Any] = dict(row)
                payload["annotation_batch_id"] = batch_id
                payload["annotation_batch_seq"] = str(batch_no)
                payload["annotation_status"] = "pending"
                payload["completeness_check_status"] = "not_started"
                payload["consistency_check_status"] = "not_started"
                payload["layer_after_annotation"] = ""
                payload["review_status"] = "pending_annotation"
                payload["annotator_notes"] = ""
                payload["consistency_notes"] = ""
                payload["export_eligibility_notes"] = ""

                if family == "sentence_fill":
                    payload["annotation_blank_position"] = row.get("blank_position", "")
                    payload["annotation_function_type"] = row.get("function_type", "")
                    payload["annotation_logic_relation"] = row.get("logic_relation", "")
                    payload["annotation_prefill_source"] = "gate_projection"
                elif family == "center_understanding":
                    payload["annotation_main_axis_source"] = ""
                    payload["annotation_argument_structure"] = ""
                    payload["annotation_prefill_source"] = "manual_required"
                elif family == "sentence_order":
                    payload["annotation_candidate_type"] = row.get("candidate_type", "")
                    payload["annotation_opening_anchor_type"] = row.get("opening_anchor_type", "")
                    payload["annotation_closing_anchor_type"] = row.get("closing_anchor_type", "")
                    payload["annotation_prefill_source"] = "strict_projection"
                else:
                    payload["annotation_prefill_source"] = ""

                master_rows.append(payload)

    return master_rows, batch_manifest_rows


def write_batch_files(master_rows: list[dict[str, Any]], batch_manifest_rows: list[dict[str, Any]]) -> None:
    OUTPUT_BATCH_DIR.mkdir(parents=True, exist_ok=True)

    common_fields = [
        "sample_id",
        "annotation_batch_id",
        "business_family_id",
        "business_subtype_id",
        "question_card_id",
        "source_name",
        "source_batch",
        "source_qid",
        "source_exam",
        "gate_status",
        "blocked_reason",
        "is_canonical_clean",
        "formal_export_eligible",
        "annotation_status",
        "completeness_check_status",
        "consistency_check_status",
        "layer_after_annotation",
        "review_status",
        "notes",
        "annotator_notes",
        "consistency_notes",
        "export_eligibility_notes",
        "annotation_prefill_source",
    ]
    family_specific = {
        "sentence_fill": [
            "blank_position",
            "function_type",
            "logic_relation",
            "annotation_blank_position",
            "annotation_function_type",
            "annotation_logic_relation",
        ],
        "center_understanding": [
            "main_axis_source",
            "argument_structure",
            "annotation_main_axis_source",
            "annotation_argument_structure",
        ],
        "sentence_order": [
            "candidate_type",
            "opening_anchor_type",
            "closing_anchor_type",
            "annotation_candidate_type",
            "annotation_opening_anchor_type",
            "annotation_closing_anchor_type",
        ],
    }

    for batch in batch_manifest_rows:
        batch_id = batch["batch_id"]
        family = batch["business_family_id"]
        rows = [row for row in master_rows if row["annotation_batch_id"] == batch_id]
        file_name = f"{batch_id.replace('.', '_')}.csv"
        write_csv(OUTPUT_BATCH_DIR / file_name, common_fields + family_specific[family], rows)


def main() -> None:
    candidate_rows = read_csv(INPUT_CANDIDATES)
    blocked_rows = read_csv(INPUT_BLOCKED)

    master_rows, batch_manifest_rows = build_master_rows(candidate_rows)

    master_fieldnames = list(master_rows[0].keys()) if master_rows else []
    write_csv(OUTPUT_MASTER, master_fieldnames, master_rows)

    error_pool_rows: list[dict[str, Any]] = []
    for row in blocked_rows:
        payload = dict(row)
        payload["annotation_status"] = "not_in_annotation_pool"
        payload["layer_after_annotation"] = "error-pool"
        payload["review_status"] = "blocked"
        payload["export_eligible"] = "false"
        error_pool_rows.append(payload)
    error_pool_fieldnames = list(error_pool_rows[0].keys()) if error_pool_rows else []
    write_csv(OUTPUT_ERROR_POOL, error_pool_fieldnames, error_pool_rows)

    batch_manifest_fieldnames = list(batch_manifest_rows[0].keys()) if batch_manifest_rows else []
    write_csv(OUTPUT_BATCH_MANIFEST, batch_manifest_fieldnames, batch_manifest_rows)
    write_batch_files(master_rows, batch_manifest_rows)


if __name__ == "__main__":
    main()
