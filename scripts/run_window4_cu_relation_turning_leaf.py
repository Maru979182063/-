from __future__ import annotations

import csv
import json
import os
import random
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
PASSAGE_ENV = ROOT / "passage_service" / ".env"
SOURCE_PATH = Path(
    str(
        os.getenv("WINDOW4_SOURCE_PATH")
        or (ROOT / "reports" / "distill_batches" / "center_understanding_relation_words_2026-04-14" / "material_samples.jsonl")
    )
)
PROMPT_SERVICE_ROOT = ROOT / "prompt_skeleton_service"

DATE_TAG = "2026-04-15"
RUN_TAG = str(os.getenv("WINDOW4_RUN_TAG") or "window4_cu_relation_turning_v1").strip()

LEAF_PATTERN_TAG = str(os.getenv("WINDOW4_LEAF_PATTERN_TAG") or "杞姌").strip()
LEAF_SUBFAMILY_ID = str(os.getenv("WINDOW4_LEAF_SUBFAMILY_ID") or "").strip()
QUESTION_CARD_ID = "question.center_understanding.standard_v1"
RANDOM_SEED = 20260415
SPLIT_SIZE = 5
TOTAL_SELECTED_ROWS = SPLIT_SIZE * 3
ACTIVE_SPLITS = {
    item.strip().lower()
    for item in str(os.getenv("WINDOW4_SPLITS") or "dev").split(",")
    if item.strip()
}
STAGE_TAG = "_".join(sorted(ACTIVE_SPLITS)) if ACTIVE_SPLITS else "dev"
RESULTS_PATH = REPORTS_DIR / f"{RUN_TAG}_{STAGE_TAG}_results_{DATE_TAG}.csv"
PACK_PATH = REPORTS_DIR / f"{RUN_TAG}_{STAGE_TAG}_pack_{DATE_TAG}.md"
MANIFEST_PATH = REPORTS_DIR / f"{RUN_TAG}_{STAGE_TAG}_manifest_{DATE_TAG}.json"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(PROMPT_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PROMPT_SERVICE_ROOT))

from scripts.round1_generation_smoke_rerun import build_service  # noqa: E402
from app.core.exceptions import DomainError  # noqa: E402
from app.schemas.question import QuestionGenerateRequest  # noqa: E402


def _load_llm_env() -> None:
    api_key = str(os.getenv("API_KEY") or "").strip()
    base_url = str(os.getenv("BASE_URL") or "").strip()
    if api_key:
        os.environ.setdefault("OPENAI_API_KEY", api_key)
        os.environ.setdefault("GENERATION_LLM_API_KEY", api_key)
        os.environ.setdefault("MATERIAL_LLM_API_KEY", api_key)
    if base_url:
        os.environ.setdefault("OPENAI_BASE_URL", base_url)
        os.environ.setdefault("GENERATION_LLM_BASE_URL", base_url)
        os.environ.setdefault("MATERIAL_LLM_BASE_URL", base_url)
    if not PASSAGE_ENV.exists():
        return
    for line in PASSAGE_ENV.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        if key == "PASSAGE_OPENAI_API_KEY":
            os.environ.setdefault("OPENAI_API_KEY", value)
            os.environ.setdefault("GENERATION_LLM_API_KEY", value)
            os.environ.setdefault("MATERIAL_LLM_API_KEY", value)
        elif key == "PASSAGE_OPENAI_BASE_URL":
            os.environ.setdefault("OPENAI_BASE_URL", value)
            os.environ.setdefault("GENERATION_LLM_BASE_URL", value)
            os.environ.setdefault("MATERIAL_LLM_BASE_URL", value)


def _load_leaf_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in SOURCE_PATH.read_text(encoding="utf-8-sig").splitlines():
        text = raw.strip()
        if not text:
            continue
        payload = json.loads(text)
        if LEAF_SUBFAMILY_ID:
            if str(payload.get("subfamily_id") or "").strip() != LEAF_SUBFAMILY_ID:
                continue
        elif str(payload.get("pattern_tag") or "").strip() != LEAF_PATTERN_TAG:
            continue
        rows.append(payload)
    rows.sort(key=lambda item: str(item.get("question_id") or ""))
    random.Random(RANDOM_SEED).shuffle(rows)
    return rows[: SPLIT_SIZE * 3]


def _split_label(index: int) -> str:
    dev_count, holdout_count, _ = _split_plan(TOTAL_SELECTED_ROWS)
    if index < dev_count:
        return "dev"
    if index < dev_count + holdout_count:
        return "holdout"
    return "retest"


def _split_plan(total_rows: int) -> tuple[int, int, int]:
    if total_rows >= SPLIT_SIZE * 3:
        return SPLIT_SIZE, SPLIT_SIZE, SPLIT_SIZE
    if total_rows > SPLIT_SIZE:
        return SPLIT_SIZE, total_rows - SPLIT_SIZE, 0
    return total_rows, 0, 0


def _build_request(row: dict[str, Any]) -> QuestionGenerateRequest:
    return QuestionGenerateRequest.model_validate(
        {
            "question_card_id": QUESTION_CARD_ID,
            "generation_mode": "forced_user_material",
            "question_focus": "main_idea",
            "difficulty_level": "medium",
            "count": 1,
            "use_fewshot": True,
            "fewshot_mode": "structure_only",
            "user_material": {
                "text": str(row.get("material") or ""),
                "title": str(row.get("paper") or row.get("question_id") or ""),
                "document_genre": "exam_reference",
                "source_label": str(row.get("source") or "window4_leaf_material"),
            },
        }
    )


def _configure_service():
    service = build_service()
    service.RACE_CANDIDATE_COUNT = 1
    service.MAX_ALIGNMENT_RETRIES = 0
    service.MAX_QUALITY_REPAIR_RETRIES = 0
    model_override = str(os.getenv("MODEL") or "").strip()
    if model_override:
        provider = service.runtime_config.llm.providers["generation_llm"]
        provider.models.question_generation = model_override
        provider.models.question_repair = model_override
        provider.models.judge_review = model_override
    return service


def _options_text(options: dict[str, str]) -> list[str]:
    lines: list[str] = []
    for key in ("A", "B", "C", "D"):
        lines.append(f"- {key}: {options.get(key, '')}")
    return lines


def _render_case_markdown(row: dict[str, str]) -> list[str]:
    generated_options = json.loads(row["generated_options_json"] or "{}")
    gold_options = json.loads(row["gold_options_json"] or "{}")
    lines = [
        f"## {row['split']} :: {row['question_id']}",
        "",
        f"- sample_index: `{row['sample_index']}`",
        f"- validation_passed: `{row['validation_passed']}`",
        f"- overall_score: `{row['overall_score']}`",
        f"- generation_exception: `{row['generation_exception'] or 'none'}`",
        f"- generation_error_details: `{row['generation_error_details_json'] or 'none'}`",
        "",
        "**Material**",
        "",
        row["material_text"] or "(empty)",
        "",
        "**Generated Question**",
        "",
        f"> {row['generated_stem'] or '(empty)'}",
        "",
    ]
    lines.extend(_options_text(generated_options))
    lines.extend(
        [
            "",
            f"answer: `{row['generated_answer'] or ''}`",
            "",
            "**Generated Analysis**",
            "",
            row["generated_analysis"] or "(empty)",
            "",
            "**Gold Question**",
            "",
            f"> {row['gold_stem'] or '(empty)'}",
            "",
        ]
    )
    lines.extend(_options_text(gold_options))
    lines.extend(
        [
            "",
            f"answer: `{row['gold_answer'] or ''}`",
            "",
            "**Gold Analysis**",
            "",
            row["gold_analysis"] or "(empty)",
            "",
            "**Aux Signals**",
            "",
            f"- validator_errors: {row['validator_errors'] or 'none'}",
            f"- validator_warnings: {row['validator_warnings'] or 'none'}",
            f"- notes: {row['notes'] or 'none'}",
            f"- warnings: {row['warnings'] or 'none'}",
            "",
        ]
    )
    return lines


def main() -> None:
    _load_llm_env()
    selected_rows = _load_leaf_rows()
    if not selected_rows:
        selector = f"subfamily_id={LEAF_SUBFAMILY_ID}" if LEAF_SUBFAMILY_ID else f"pattern_tag={LEAF_PATTERN_TAG}"
        raise RuntimeError(f"No samples found for {selector}")

    global TOTAL_SELECTED_ROWS
    TOTAL_SELECTED_ROWS = len(selected_rows)
    dev_count, holdout_count, retest_count = _split_plan(TOTAL_SELECTED_ROWS)

    service = _configure_service()
    manifest_rows: list[dict[str, Any]] = []
    result_rows: list[dict[str, str]] = []
    pack_sections = [
        f"# {RUN_TAG}",
        "",
        f"- leaf_selector: `subfamily_id={LEAF_SUBFAMILY_ID}`"
        if LEAF_SUBFAMILY_ID
        else f"- leaf_selector: `pattern_tag={LEAF_PATTERN_TAG}`",
        f"- question_card_id: `{QUESTION_CARD_ID}`",
        f"- random_seed: `{RANDOM_SEED}`",
        f"- split_policy: `{dev_count} dev + {holdout_count} holdout + {retest_count} retest`",
        f"- active_splits: `{', '.join(sorted(ACTIVE_SPLITS))}`",
        f"- shortest_chain: `question_card_id -> forced_user_material -> generate`",
        "",
    ]

    for index, row in enumerate(selected_rows):
        split = _split_label(index)
        if ACTIVE_SPLITS and split not in ACTIVE_SPLITS:
            continue
        manifest_rows.append(
            {
                "sample_index": index + 1,
                "split": split,
                "question_id": str(row.get("question_id") or ""),
                "pattern_tag": str(row.get("pattern_tag") or ""),
                "subfamily_id": str(row.get("subfamily_id") or ""),
                "paper": str(row.get("paper") or ""),
            }
        )
        request = _build_request(row)
        generation_exception = ""
        generation_error_details_json = ""
        response: dict[str, Any] = {}
        try:
            response = service.generate(request)
        except DomainError as exc:
            generation_exception = f"{exc.__class__.__name__}: {exc}"
            generation_error_details_json = json.dumps(exc.details, ensure_ascii=False)
        except Exception as exc:  # noqa: BLE001
            generation_exception = f"{exc.__class__.__name__}: {exc}"

        item = ((response.get("items") or [{}])[0]) if response else {}
        generated = dict(item.get("generated_question") or {})
        validation = dict(item.get("validation_result") or {})
        evaluation = dict(item.get("evaluation_result") or {})
        generated_options = generated.get("options") if isinstance(generated.get("options"), dict) else {}
        gold_options = {
            "A": str(row.get("option_a") or ""),
            "B": str(row.get("option_b") or ""),
            "C": str(row.get("option_c") or ""),
            "D": str(row.get("option_d") or ""),
        }

        result_row = {
            "sample_index": str(index + 1),
            "split": split,
            "question_id": str(row.get("question_id") or ""),
            "pattern_tag": str(row.get("pattern_tag") or ""),
            "subfamily_id": str(row.get("subfamily_id") or ""),
            "paper": str(row.get("paper") or ""),
            "material_text": str(row.get("material") or ""),
            "generated_stem": str(generated.get("stem") or ""),
            "generated_options_json": json.dumps(generated_options, ensure_ascii=False),
            "generated_answer": str(generated.get("answer") or ""),
            "generated_analysis": str(generated.get("analysis") or ""),
            "gold_stem": str(row.get("stem") or ""),
            "gold_options_json": json.dumps(gold_options, ensure_ascii=False),
            "gold_answer": str(row.get("answer") or ""),
            "gold_analysis": str(row.get("analysis") or ""),
            "validation_passed": "true" if validation.get("passed") else "false",
            "validator_errors": "; ".join(validation.get("errors") or []),
            "validator_warnings": "; ".join(validation.get("warnings") or []),
            "overall_score": str(evaluation.get("overall_score") or ""),
            "notes": "; ".join(item.get("notes") or []),
            "warnings": "; ".join(item.get("warnings") or []),
            "generation_exception": generation_exception,
            "generation_error_details_json": generation_error_details_json,
        }
        result_rows.append(result_row)
        pack_sections.extend(_render_case_markdown(result_row))

    MANIFEST_PATH.write_text(json.dumps(manifest_rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_csv(result_rows)
    PACK_PATH.write_text("\n".join(pack_sections) + "\n", encoding="utf-8")


def _write_csv(rows: list[dict[str, str]]) -> None:
    fieldnames = list(rows[0].keys())
    with RESULTS_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()


