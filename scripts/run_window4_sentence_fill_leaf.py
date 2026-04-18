from __future__ import annotations

import ast
import csv
import json
import os
import random
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
PASSAGE_ENV = ROOT / "passage_service" / ".env"
SOURCE_PATH = Path(
    str(
        os.getenv("WINDOW4_SOURCE_PATH")
        or (
            ROOT
            / "reports"
            / "distill_batches"
            / "sentence_fill_middle_2026-04-14"
            / "cleaned_truth_materials.jsonl"
        )
    )
)
GOLD_SOURCE_PATH = Path(
    str(
        os.getenv("WINDOW4_GOLD_SOURCE_PATH")
        or (
            ROOT
            / "reports"
            / "distill_batches"
            / "sentence_fill_middle_2026-04-14"
            / "material_samples_rebuilt.jsonl"
        )
    )
)
PROMPT_SERVICE_ROOT = ROOT / "prompt_skeleton_service"

DATE_TAG = "2026-04-15"
RUN_TAG = str(os.getenv("WINDOW4_RUN_TAG") or "window4_sentence_fill_middle_v1").strip()

LEAF_PATTERN_TAG = str(os.getenv("WINDOW4_LEAF_PATTERN_TAG") or "妯嚎鍦ㄤ腑闂?鎵夸笂").strip()
QUESTION_CARD_ID = "question.sentence_fill.standard_v1"
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


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        text = raw.strip()
        if not text:
            continue
        rows.append(json.loads(text))
    return rows


def _gold_map() -> dict[str, dict[str, Any]]:
    gold_rows = _load_jsonl(GOLD_SOURCE_PATH)
    return {str(row.get("question_id") or ""): row for row in gold_rows}


def _load_leaf_rows() -> list[dict[str, Any]]:
    rows = [row for row in _load_jsonl(SOURCE_PATH) if str(row.get("pattern_tag") or "").strip() == LEAF_PATTERN_TAG]
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


def _type_slots_for_leaf(pattern_tag: str) -> dict[str, str]:
    mapping = {
        "横线在中间-承上": {
            "blank_position": "middle",
            "function_type": "carry_previous",
            "logic_relation": "explanation",
        },
        "横线在中间-承上启下": {
            "blank_position": "middle",
            "function_type": "bridge",
            "logic_relation": "continuation",
        },
        "横线在中间-启下": {
            "blank_position": "middle",
            "function_type": "lead_next",
            "logic_relation": "focus_shift",
        },
        "横线在开头-概括后文": {
            "blank_position": "opening",
            "function_type": "summary",
            "logic_relation": "summary",
        },
        "横线在开头-话题引入": {
            "blank_position": "opening",
            "function_type": "topic_intro",
            "logic_relation": "continuation",
        },
        "横线在开头-横线为首句中的分句": {
            "blank_position": "opening",
            "function_type": "topic_intro",
            "logic_relation": "continuation",
        },
        "横线在结尾-总结前文（原为 结论）": {
            "blank_position": "ending",
            "function_type": "conclusion",
            "logic_relation": "summary",
        },
        "横线在结尾-提出对策（原为 对策）": {
            "blank_position": "ending",
            "function_type": "countermeasure",
            "logic_relation": "continuation",
        },
        "横线在结尾-横线为尾句中的分句": {
            "blank_position": "ending",
            "function_type": "conclusion",
            "logic_relation": "summary",
        },
        "妯嚎鍦ㄤ腑闂?鎵夸笂": {
            "blank_position": "middle",
            "function_type": "carry_previous",
            "logic_relation": "explanation",
        },
        "妯嚎鍦ㄤ腑闂?鎵夸笂鍚笅": {
            "blank_position": "middle",
            "function_type": "bridge",
            "logic_relation": "continuation",
        },
        "妯嚎鍦ㄤ腑闂?鍚笅": {
            "blank_position": "middle",
            "function_type": "lead_next",
            "logic_relation": "focus_shift",
        },
        "妯嚎鍦ㄥ紑澶?鎬昏捣": {
            "blank_position": "opening",
            "function_type": "summary",
            "logic_relation": "summary",
        },
        "妯嚎鍦ㄥ紑澶?姒傛嫭鍚庢枃": {
            "blank_position": "opening",
            "function_type": "summary",
            "logic_relation": "summary",
        },
        "妯嚎鍦ㄥ紑澶?寮曞嚭璇濋": {
            "blank_position": "opening",
            "function_type": "topic_intro",
            "logic_relation": "continuation",
        },
        "妯嚎鍦ㄥ紑澶?璇濋寮曞叆": {
            "blank_position": "opening",
            "function_type": "topic_intro",
            "logic_relation": "continuation",
        },
        "妯嚎鍦ㄥ紑澶?鍙ュ瓙寮€澶?": {
            "blank_position": "opening",
            "function_type": "topic_intro",
            "logic_relation": "continuation",
        },
        "妯嚎鍦ㄥ紑澶?妯嚎涓洪鍙ヤ腑鐨勫垎鍙?": {
            "blank_position": "opening",
            "function_type": "topic_intro",
            "logic_relation": "continuation",
        },
        "妯嚎鍦ㄧ粨灏?鎬荤粨鍓嶆枃": {
            "blank_position": "ending",
            "function_type": "conclusion",
            "logic_relation": "summary",
        },
        "妯嚎鍦ㄧ粨灏?鎬荤粨鍓嶆枃锛堝師涓?缁撹锛?": {
            "blank_position": "ending",
            "function_type": "conclusion",
            "logic_relation": "summary",
        },
        "妯嚎鍦ㄧ粨灏?鎬荤粨鍙?": {
            "blank_position": "ending",
            "function_type": "conclusion",
            "logic_relation": "summary",
        },
        "妯嚎鍦ㄧ粨灏?瀵圭瓥鍙?": {
            "blank_position": "ending",
            "function_type": "countermeasure",
            "logic_relation": "continuation",
        },
        "妯嚎鍦ㄧ粨灏?鎻愬嚭瀵圭瓥锛堝師涓?瀵圭瓥锛?": {
            "blank_position": "ending",
            "function_type": "countermeasure",
            "logic_relation": "continuation",
        },
        "妯嚎鍦ㄧ粨灏?妯嚎涓哄熬鍙ヤ腑鐨勫垎鍙?": {
            "blank_position": "ending",
            "function_type": "conclusion",
            "logic_relation": "summary",
        },
    }
    if pattern_tag not in mapping:
        raise RuntimeError(f"Unsupported sentence_fill pattern_tag: {pattern_tag}")
    return dict(mapping[pattern_tag])


def _has_blank_marker(text: str) -> bool:
    markers = ("_____________", "( )", "___", "____")
    return any(marker in text for marker in markers)


def _strip_fill_instruction(text: str) -> str:
    if not text:
        return text
    cleaned = re.sub(r"\s*填入.*?(最恰当的一项|最恰当的一句).*?$", "", text)
    return cleaned.strip() or text


def _make_blank_explicit(text: str) -> str:
    if not text:
        return text
    normalized = str(text)
    if "____" in normalized or "( )" in normalized or "（ ）" in normalized:
        return normalized
    normalized = re.sub(r"([，,。！？!?；;：:])\s*([，,。！？!?；;：:])", r"\1（ ）\2", normalized)
    return normalized


def _runtime_material_text(row: dict[str, Any], gold: dict[str, Any]) -> str:
    reconstructed_full_passage = _reconstruct_full_passage_from_gold(gold)
    if reconstructed_full_passage:
        return reconstructed_full_passage
    source_text = str(row.get("material_text") or "").strip()
    if _has_blank_marker(source_text):
        return source_text
    prompt_before_fill = str(gold.get("prompt_before_fill") or "").strip()
    if prompt_before_fill:
        candidate = _strip_fill_instruction(prompt_before_fill)
        candidate = _make_blank_explicit(candidate)
        if _has_blank_marker(candidate):
            return candidate
    return _make_blank_explicit(source_text)


def _normalize_generated_options(options: Any) -> dict[str, str]:
    normalized: dict[str, str] = {}
    if not isinstance(options, dict):
        return normalized
    for key in ("A", "B", "C", "D"):
        value = options.get(key)
        if isinstance(value, dict):
            normalized[key] = str(value.get("text") or "")
        elif isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("{") and stripped.endswith("}"):
                try:
                    parsed = ast.literal_eval(stripped)
                except (ValueError, SyntaxError):
                    parsed = None
                if isinstance(parsed, dict):
                    normalized[key] = str(parsed.get("text") or value)
                else:
                    normalized[key] = value
            else:
                normalized[key] = value
        else:
            normalized[key] = str(value or "")
    return normalized


def _build_request(row: dict[str, Any], gold: dict[str, Any]) -> QuestionGenerateRequest:
    material_text = _runtime_material_text(row, gold)
    return QuestionGenerateRequest.model_validate(
        {
            "question_card_id": QUESTION_CARD_ID,
            "generation_mode": "forced_user_material",
            "question_focus": "sentence_fill",
            "difficulty_level": "medium",
            "count": 1,
            "use_fewshot": True,
            "fewshot_mode": "structure_only",
            "type_slots": _type_slots_for_leaf(str(row.get("pattern_tag") or "")),
            "user_material": {
                "text": material_text,
                "title": str(row.get("paper") or row.get("question_id") or ""),
                "document_genre": "exam_reference",
                "source_label": str(row.get("source_doc") or "window4_sentence_fill_leaf"),
            },
        }
    )


def _configure_service():
    service = build_service()
    service.RACE_CANDIDATE_COUNT = 1
    service.MAX_ALIGNMENT_RETRIES = 0
    service.MAX_QUALITY_REPAIR_RETRIES = 0
    if str(os.getenv("WINDOW4_DISABLE_EVALUATOR") or "1").strip().lower() not in {"0", "false", "no"}:
        service.evaluator.evaluate = lambda **_: {}
        service._apply_evaluation_gate = lambda **_: []
    model_override = str(os.getenv("MODEL") or "").strip()
    if model_override:
        provider = service.runtime_config.llm.providers["generation_llm"]
        provider.models.question_generation = model_override
        provider.models.question_repair = model_override
        provider.models.judge_review = model_override
    return service


def _options_text(options: dict[str, str]) -> list[str]:
    return [f"- {key}: {options.get(key, '')}" for key in ("A", "B", "C", "D")]


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
        raise RuntimeError(f"No samples found for pattern_tag={LEAF_PATTERN_TAG}")

    gold_by_qid = _gold_map()

    global TOTAL_SELECTED_ROWS
    TOTAL_SELECTED_ROWS = len(selected_rows)
    dev_count, holdout_count, retest_count = _split_plan(TOTAL_SELECTED_ROWS)

    service = _configure_service()
    manifest_rows: list[dict[str, Any]] = []
    result_rows: list[dict[str, str]] = []
    pack_sections = [
        f"# {RUN_TAG}",
        "",
        f"- leaf_selector: `pattern_tag={LEAF_PATTERN_TAG}`",
        f"- question_card_id: `{QUESTION_CARD_ID}`",
        f"- random_seed: `{RANDOM_SEED}`",
        f"- split_policy: `{dev_count} dev + {holdout_count} holdout + {retest_count} retest`",
        f"- active_splits: `{', '.join(sorted(ACTIVE_SPLITS))}`",
        f"- shortest_chain: `question_card_id + type_slots + forced_user_material -> generate`",
        "",
    ]

    for index, row in enumerate(selected_rows):
        split = _split_label(index)
        if ACTIVE_SPLITS and split not in ACTIVE_SPLITS:
            continue

        qid = str(row.get("question_id") or "")
        gold = gold_by_qid.get(qid) or {}
        manifest_rows.append(
            {
                "sample_index": index + 1,
                "split": split,
                "question_id": qid,
                "pattern_tag": str(row.get("pattern_tag") or ""),
                "paper": str(row.get("paper") or ""),
            }
        )

        request = _build_request(row, gold)
        generation_exception = ""
        response: dict[str, Any] = {}
        try:
            raw_response = service.generate(request)
            if hasattr(raw_response, "model_dump"):
                response = raw_response.model_dump(mode="json")
            elif isinstance(raw_response, dict):
                response = raw_response
            else:
                response = {}
        except DomainError as exc:
            generation_exception = f"DomainError: {getattr(exc, 'message', str(exc))}"
        except Exception as exc:  # noqa: BLE001
            generation_exception = f"{exc.__class__.__name__}: {exc}"

        item = ((response.get("items") or [None])[0]) or {}
        generated_question = dict(item.get("generated_question") or {})
        generated_options = _normalize_generated_options(generated_question.get("options") or {})
        validation_result = dict(item.get("validation_result") or {})
        evaluation_result = dict(item.get("evaluation_result") or {})
        runtime_material_text = _runtime_material_text(row, gold)

        result_row = {
            "sample_index": str(index + 1),
            "split": split,
            "question_id": qid,
            "pattern_tag": str(row.get("pattern_tag") or ""),
            "paper": str(row.get("paper") or ""),
            "material_text": runtime_material_text,
            "generated_stem": str(generated_question.get("stem") or ""),
            "generated_options_json": json.dumps(generated_options, ensure_ascii=False),
            "generated_answer": str(generated_question.get("answer") or ""),
            "generated_analysis": str(generated_question.get("analysis") or ""),
            "gold_stem": str(gold.get("prompt_before_fill") or gold.get("stem") or ""),
            "gold_options_json": json.dumps(
                {
                    "A": str(gold.get("option_a") or ""),
                    "B": str(gold.get("option_b") or ""),
                    "C": str(gold.get("option_c") or ""),
                    "D": str(gold.get("option_d") or ""),
                },
                ensure_ascii=False,
            ),
            "gold_answer": str(gold.get("answer") or ""),
            "gold_analysis": str(gold.get("analysis") or ""),
            "validation_passed": str(validation_result.get("passed") or False).lower(),
            "validator_errors": "; ".join(validation_result.get("errors") or []),
            "validator_warnings": "; ".join(validation_result.get("warnings") or []),
            "overall_score": str(evaluation_result.get("overall_score") or ""),
            "notes": "; ".join(item.get("notes") or []),
            "warnings": "; ".join(item.get("warnings") or []),
            "generation_exception": generation_exception,
        }
        result_rows.append(result_row)
        pack_sections.extend(_render_case_markdown(result_row))

    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(result_rows[0].keys()))
        writer.writeheader()
        writer.writerows(result_rows)
    PACK_PATH.write_text("\n".join(pack_sections) + "\n", encoding="utf-8")
    MANIFEST_PATH.write_text(json.dumps(manifest_rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

def _gold_correct_option_text(gold: dict[str, Any]) -> str:
    answer = str(gold.get("answer") or "").strip().upper()
    field_map = {"A": "option_a", "B": "option_b", "C": "option_c", "D": "option_d"}
    field_name = field_map.get(answer)
    return str(gold.get(field_name) or "").strip() if field_name else ""


def _reconstruct_full_passage_from_gold(gold: dict[str, Any]) -> str:
    prompt_before_fill = str(gold.get("prompt_before_fill") or gold.get("stem") or "").strip()
    correct_option_text = _gold_correct_option_text(gold)
    if not prompt_before_fill or not correct_option_text:
        return ""

    restored = _strip_fill_instruction(prompt_before_fill)
    for pattern in (r"_{2,}", r"（\s*）", r"\(\s*\)", r"“\s*”", r"﹍+"):
        restored, count = re.subn(pattern, correct_option_text, restored, count=1)
        if count:
            return restored.strip()

    restored, count = re.subn(
        r"([，,：:；;]\s*)[。！？!?]",
        lambda match: f"{match.group(1)}{correct_option_text}。",
        restored,
        count=1,
    )
    if count:
        return restored.strip()

    restored, count = re.subn(
        r"([，,：:；;]\s*)$",
        lambda match: f"{match.group(1)}{correct_option_text}",
        restored,
        count=1,
    )
    if count:
        return restored.strip()

    return ""


if __name__ == "__main__":
    main()


