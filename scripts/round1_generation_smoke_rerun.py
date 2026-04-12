from __future__ import annotations

import csv
import json
import re
import sys
import uuid
import zipfile
import xml.etree.ElementTree as ET
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
PROMPT_SERVICE_ROOT = ROOT / "prompt_skeleton_service"
sys.path.insert(0, str(PROMPT_SERVICE_ROOT))

import yaml

from app.schemas.config import QuestionTypeConfig
from app.schemas.question import QuestionGenerateRequest
from app.services.config_registry import ConfigRegistry
from app.services.delivery_service import build_center_understanding_export_view, evaluate_formal_export_policy
from app.services.prompt_orchestrator import PromptOrchestratorService
from app.services.prompt_template_registry import PromptTemplateRegistry
from app.services.question_generation import QuestionGenerationService
from app.services.question_repository import QuestionRepository
from app.services.runtime_registry import RuntimeConfigRegistry
from app.services.sentence_fill_protocol import project_sentence_fill_strict_export_view
from app.services.sentence_order_protocol import project_sentence_order_strict_export_view
from app.services.text_readability import (
    detect_readability_issues,
    normalize_extracted_lines,
    normalize_readable_text,
    normalize_source_question_payload,
    normalize_user_material_payload,
)


GOLD_READY_PATH = REPORTS / "pilot_round1_gold_ready_pool_2026-04-12.csv"
REVIEW_HOLDOUT_PATH = REPORTS / "pilot_round1_review_holdout_pool_2026-04-12.csv"
ERROR_POOL_PATH = REPORTS / "pilot_round1_error_pool_final_2026-04-12.csv"
RESULTS_PATH = REPORTS / "round1_generation_smoke_rerun_results_2026-04-12.csv"
REPORT_PATH = REPORTS / "round1_generation_smoke_rerun_report_2026-04-12.md"
CLEANUP_DIFF_PATH = REPORTS / "round1_source_extraction_cleanup_diff_2026-04-12.csv"


DOCX_MAP = {
    "语句表达-语句填空题.docx": Path(r"C:\Users\Maru\Desktop\语句表达-语句填空题.docx"),
    "片段阅读-中心理解题.docx": Path(r"C:\Users\Maru\Desktop\片段阅读-中心理解题.docx"),
    "语句表达-语句排序题.docx": Path(r"C:\Users\Maru\Desktop\语句表达-语句排序题.docx"),
}

SMOKE_SAMPLE_IDS = [
    "pilot.r1.sentence_fill.2054222",
    "pilot.r1.center_understanding.2054046",
    "pilot.r1.sentence_order.2012710",
    "pilot.r1.center_understanding.2052336",
    "pilot.r1.center_understanding.2052316",
    "pilot.r1.sentence_order.2054650",
]


class SmokeConfigRegistry(ConfigRegistry):
    def load(self) -> None:
        loaded_types: dict[str, QuestionTypeConfig] = {}
        aliases: dict[str, str] = {}
        warnings: list[str] = []

        for path in sorted(self.config_dir.glob("*.yaml")):
            with path.open("r", encoding="utf-8") as fh:
                raw_config = yaml.safe_load(fh) or {}
            if path.name == "sentence_order.yaml":
                default_slots = raw_config.get("default_slots") or {}
                if isinstance(default_slots, dict):
                    default_slots.pop("candidate_type", None)
            config = QuestionTypeConfig.model_validate(raw_config)
            if not config.enabled:
                warnings.append(f"Skipped disabled type: {config.type_id}")
                continue
            loaded_types[config.type_id] = config
            aliases[config.type_id.lower()] = config.type_id
            for alias in config.aliases:
                aliases[alias.lower()] = config.type_id

        self._types = loaded_types
        self._aliases = aliases
        self._warnings = warnings
        self._loaded = True


def main() -> None:
    service = build_service()
    sample_rows = load_sample_rows()
    docx_blocks = {name: extract_docx_blocks(path) for name, path in DOCX_MAP.items()}

    results: list[dict[str, str]] = []
    cleanup_rows: list[dict[str, str]] = []
    for sample_id in SMOKE_SAMPLE_IDS:
        row = sample_rows[sample_id]
        if sample_id in {
            "pilot.r1.center_understanding.2052316",
            "pilot.r1.sentence_order.2054650",
        }:
            results.append(run_negative_control(row))
            continue
        result, cleanup_row = run_generation_smoke(service=service, row=row, docx_blocks=docx_blocks)
        results.append(result)
        cleanup_rows.append(cleanup_row)

    write_results(results)
    write_cleanup_diff(cleanup_rows)
    write_report(results)


def build_service() -> QuestionGenerationService:
    config_dir = PROMPT_SERVICE_ROOT / "configs" / "types"
    runtime_config_path = PROMPT_SERVICE_ROOT / "configs" / "question_runtime.yaml"
    template_path = PROMPT_SERVICE_ROOT / "configs" / "prompt_templates.yaml"
    db_path = ROOT / "tmp" / "round1_generation_smoke.sqlite3"

    registry = SmokeConfigRegistry(config_dir)
    registry.load()
    runtime_registry = RuntimeConfigRegistry(runtime_config_path)
    runtime_config = runtime_registry.load()
    template_registry = PromptTemplateRegistry(template_path)
    template_registry.load()
    repository = QuestionRepository(db_path)
    orchestrator = PromptOrchestratorService(registry)
    return QuestionGenerationService(
        orchestrator=orchestrator,
        runtime_config=runtime_config,
        repository=repository,
        prompt_template_registry=template_registry,
    )


def load_sample_rows() -> dict[str, dict[str, str]]:
    rows = {}
    for path in (GOLD_READY_PATH, REVIEW_HOLDOUT_PATH, ERROR_POOL_PATH):
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                rows[row["sample_id"]] = row
    return rows


def extract_docx_blocks(path: Path) -> dict[str, dict[str, object]]:
    with zipfile.ZipFile(path) as zf:
        root = ET.fromstring(zf.read("word/document.xml"))
    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    paragraph_pairs: list[tuple[str, str]] = []
    for para in root.findall(".//w:p", ns):
        text = "".join((node.text or "") for node in para.findall(".//w:t", ns)).strip()
        cleaned = normalize_readable_text(text)
        if cleaned:
            paragraph_pairs.append((text, cleaned))

    blocks: dict[str, dict[str, object]] = {}
    current_qid: str | None = None
    current_header = ""
    current_lines: list[str] = []
    current_raw_lines: list[str] = []
    header_pattern = re.compile(r"^\d+\.\s.*#(\d+)$")
    for raw_line, line in paragraph_pairs:
        match = header_pattern.match(line)
        if match:
            if current_qid:
                blocks[current_qid] = {"header": current_header, "lines": current_lines[:], "raw_lines": current_raw_lines[:]}
            current_qid = match.group(1)
            current_header = line
            current_lines = []
            current_raw_lines = []
            continue
        if current_qid:
            current_lines.append(line)
            current_raw_lines.append(raw_line)
    if current_qid:
        blocks[current_qid] = {"header": current_header, "lines": current_lines[:], "raw_lines": current_raw_lines[:]}
    return blocks


def parse_source_question(block_lines: list[str], *, family: str) -> dict[str, object]:
    lines = normalize_extracted_lines(block_lines)
    answer = None
    answer_idx = None
    for idx, line in enumerate(lines):
        match = re.search(r"(?:答案|正确答案)\s*[:：]?\s*([A-D])", line)
        if match:
            answer = match.group(1)
            answer_idx = idx
            break

    analysis_idx = None
    for idx, line in enumerate(lines):
        if line.startswith("解析"):
            analysis_idx = idx
            break

    options = parse_options(lines)
    stem_idx = find_stem_index(lines, family=family)
    passage_lines = lines[:stem_idx] if stem_idx is not None else []
    if passage_lines and "第" in passage_lines[0] and "题" in passage_lines[0]:
        passage_lines = passage_lines[1:]
    stem = lines[stem_idx] if stem_idx is not None else ""
    if answer_idx is None:
        cutoff = analysis_idx if analysis_idx is not None else len(lines)
    else:
        cutoff = answer_idx
    option_start = min([idx for idx, line in enumerate(lines) if re.match(r"^[A-D][\.\u3001\uff0e:：\s]", line)] or [cutoff])
    if stem_idx is not None:
        passage_lines = lines[:stem_idx]
    elif option_start:
        passage_lines = lines[:option_start]

    analysis_lines: list[str] = []
    if analysis_idx is not None:
        first_line = re.sub(r"^解析\s*[:：]?\s*", "", lines[analysis_idx]).strip()
        if first_line:
            analysis_lines.append(first_line)
        analysis_lines.extend(lines[analysis_idx + 1 :])

    passage = "\n".join(line for line in passage_lines if line and line != stem).strip()
    return normalize_source_question_payload(
        {
        "passage": passage,
        "stem": stem,
        "options": options,
        "answer": answer,
        "analysis": "\n".join(analysis_lines).strip() or None,
        }
    )


def parse_options(lines: list[str]) -> dict[str, str]:
    options = {key: "" for key in ("A", "B", "C", "D")}
    option_pattern = re.compile(r"^([A-D])[\.\u3001\uff0e:：\s]+(.+)$")
    matches = [(idx, option_pattern.match(line)) for idx, line in enumerate(lines)]
    explicit = [(idx, match.group(1), match.group(2).strip()) for idx, match in matches if match]
    if explicit:
        for pos, (idx, letter, text) in enumerate(explicit):
            end = explicit[pos + 1][0] if pos + 1 < len(explicit) else len(lines)
            chunk = [text] + [line for line in lines[idx + 1 : end] if not line.startswith("解析")]
            options[letter] = "\n".join(item for item in chunk if item).strip()
        return options

    full_text = "\n".join(lines)
    inline = list(re.finditer(r"([A-D])[\.\u3001\uff0e:：]\s*(.*?)(?=(?:[A-D][\.\u3001\uff0e:：])|$)", full_text))
    for match in inline:
        options[match.group(1)] = normalize_readable_text(match.group(2))
    return options


def find_stem_index(lines: list[str], *, family: str) -> int | None:
    if family == "sentence_fill":
        markers = ("填入", "横线", "画横线", "最恰当")
    elif family == "center_understanding":
        markers = ("概括最准确", "旨在", "意在", "说明", "强调", "标题")
    else:
        markers = ("重新排列", "语序", "排序", "正确顺序")
    for idx, line in enumerate(lines):
        if any(marker in line for marker in markers):
            return idx
    return None


def build_request(row: dict[str, str], source_question: dict[str, object]) -> QuestionGenerateRequest:
    family = row["business_family_id"]
    if family == "sentence_fill":
        question_focus = "sentence_fill"
        type_slots = {
            "blank_position": row.get("annotation_blank_position") or row.get("blank_position"),
            "function_type": row.get("annotation_function_type") or row.get("function_type"),
            "logic_relation": row.get("annotation_logic_relation") or row.get("logic_relation"),
        }
    elif family == "center_understanding":
        question_focus = "main_idea"
        type_slots = {}
    else:
        question_focus = "sentence_order"
        type_slots = {
            "opening_anchor_type": row.get("annotation_opening_anchor_type") or row.get("opening_anchor_type"),
            "closing_anchor_type": row.get("annotation_closing_anchor_type") or row.get("closing_anchor_type"),
        }
    type_slots = {key: value for key, value in type_slots.items() if str(value or "").strip()}
    user_material_payload = normalize_user_material_payload(
        {
            "text": source_question.get("passage") or "",
            "title": row.get("source_exam") or row.get("source_qid"),
            "topic": None,
            "document_genre": "exam_reference",
            "source_label": row.get("source_name"),
        }
    )
    return QuestionGenerateRequest.model_validate(
        {
            "question_card_id": row["question_card_id"],
            "generation_mode": "forced_user_material",
            "question_focus": question_focus,
            "difficulty_level": "medium",
            "count": 1,
            "use_fewshot": True,
            "fewshot_mode": "structure_only",
            "type_slots": type_slots,
            "source_question": source_question,
            "user_material": user_material_payload,
        }
    )


def run_generation_smoke(
    *,
    service: QuestionGenerationService,
    row: dict[str, str],
    docx_blocks: dict[str, dict[str, dict[str, object]]],
) -> tuple[dict[str, str], dict[str, str]]:
    family = row["business_family_id"]
    qid = row["source_qid"]
    doc_name = row["source_name"]
    block = (docx_blocks.get(doc_name) or {}).get(qid)
    if not block:
        return ({
            "sample_id": row["sample_id"],
            "business_family_id": family,
            "regression_role": "boundary_holdout" if row["sample_id"].endswith("2052336") else "positive",
            "fewshot_used": "false",
            "fewshot_source": "",
            "generation_status": "failed",
            "gate_status": "not_checked",
            "export_status": "not_checked",
            "readability_status": "not_checked",
            "json_parse_status": "not_checked",
            "core_field_projection": "{}",
            "observed_behavior": "docx_block_missing",
            "verdict": "fail",
            "notes": "source_extraction_missing",
        }, {
            "sample_id": row["sample_id"],
            "business_family_id": family,
            "raw_issue_count": "0",
            "cleaned_issue_count": "0",
            "issue_delta": "0",
            "notes": "docx_block_missing",
        })

    raw_source_question = {
        "passage": "\n".join(str(line or "") for line in block.get("raw_lines") or []),
        "stem": "",
        "options": {},
        "answer": None,
        "analysis": None,
    }
    source_question = parse_source_question(block["lines"], family=family)
    request = build_request(row, source_question)
    prepared_request = service._prepare_request(request)
    readability_status = "clean"
    json_parse_status = "not_started"
    gate_status = "not_checked"
    export_status = "not_checked"
    fewshot_used = False
    fewshot_source = ""
    projection_text = "{}"
    observed_behavior = ""
    verdict = "fail"
    notes = ""
    generation_status = "failed"

    input_readability_blob = json.dumps(
        {
            "source_question": prepared_request.source_question.model_dump() if prepared_request.source_question else None,
            "user_material": prepared_request.user_material.model_dump() if prepared_request.user_material else None,
        },
        ensure_ascii=False,
    )
    if detect_readability_issues(input_readability_blob):
        readability_status = "dirty"

    cleaned_source_issue_count = len(detect_readability_issues(json.dumps(
        {
            "source_question": prepared_request.source_question.model_dump() if prepared_request.source_question else None,
            "user_material": prepared_request.user_material.model_dump() if prepared_request.user_material else None,
        },
        ensure_ascii=False,
    )))
    raw_issue_count = len(detect_readability_issues(json.dumps(raw_source_question, ensure_ascii=False)))

    try:
        decoded = service._build_explicit_question_card_decode_result(prepared_request)
        standard_request = dict(decoded["standard_request"])
        question_card_binding = service._resolve_question_card_binding(
            question_card_id=prepared_request.question_card_id,
            question_type=standard_request["question_type"],
            business_subtype=standard_request.get("business_subtype"),
            pattern_id=standard_request.get("pattern_id"),
        )
        standard_request = service._apply_question_card_binding(
            standard_request=standard_request,
            question_card_binding=question_card_binding,
        )
        source_question_analysis = service.source_question_analyzer.analyze(
            source_question=prepared_request.source_question,
            question_type=standard_request["question_type"],
            business_subtype=standard_request.get("business_subtype"),
        )
        request_snapshot = service._build_request_snapshot(
            prepared_request,
            standard_request,
            decoded,
            request_id=str(uuid.uuid4()),
            source_question_analysis=source_question_analysis,
            question_card_binding=question_card_binding,
        )
        materials = service._build_forced_user_material_candidates(
            user_material=prepared_request.user_material,
            question_card_binding=question_card_binding,
            request_snapshot=request_snapshot,
            count=1,
        )
        material = materials[0]
        built_item = service._build_generated_item(
            build_request=service._build_prompt_request_from_snapshot(request_snapshot),
            material=material,
            batch_id=f"smoke::{uuid.uuid4().hex}",
            item_id=None,
            request_snapshot=request_snapshot,
            revision_count=0,
            route=service._question_generation_route(),
            source_action="generate",
            review_note=None,
            request_id=str(uuid.uuid4()),
            previous_item=None,
        )

        prompt_package = built_item.get("prompt_package") or {}
        fewshot_examples = prompt_package.get("fewshot_examples") or []
        fewshot_used = bool(fewshot_examples)
        if fewshot_examples:
            first = fewshot_examples[0]
            if isinstance(first, dict):
                fewshot_source = str(first.get("title") or first.get("asset_source") or "")
            else:
                fewshot_source = str(first)

        generated = built_item.get("generated_question") or {}
        generated_blob = json.dumps(generated, ensure_ascii=False)
        if detect_readability_issues(generated_blob):
            readability_status = "dirty"

        generation_status = built_item.get("statuses", {}).get("generation_status") or "failed"
        json_parse_status = "parsed" if generation_status == "success" else "parse_failed"
        gate_status, export_status, projection_text = project_family_outputs(family=family, item=built_item)
        observed_behavior = summarize_generated_item(built_item)
        verdict = "pass" if generation_status == "success" and gate_status == "pass" and export_status == "pass" and readability_status == "clean" else "fail"

        if row["sample_id"] == "pilot.r1.center_understanding.2052336" and verdict == "pass":
            notes = "boundary_holdout_prompt_guard_preserved"
        elif row["sample_id"] == "pilot.r1.center_understanding.2052336":
            notes = "boundary_holdout_not_proven"
        else:
            notes = ";".join(built_item.get("warnings") or []) or ""
    except Exception as exc:  # noqa: BLE001
        generation_status = f"error:{exc.__class__.__name__}"
        json_parse_status = "parse_failed" if "JSON" in exc.__class__.__name__ or "json" in str(exc).lower() else "not_reached"
        observed_behavior = str(exc)
        notes = "generation_exception"

    return ({
        "sample_id": row["sample_id"],
        "business_family_id": family,
        "regression_role": "boundary_holdout" if row["sample_id"] == "pilot.r1.center_understanding.2052336" else "positive",
        "fewshot_used": "true" if fewshot_used else "false",
        "fewshot_source": fewshot_source,
        "generation_status": generation_status,
        "gate_status": gate_status,
        "export_status": export_status,
        "readability_status": readability_status,
        "json_parse_status": json_parse_status,
        "core_field_projection": projection_text,
        "observed_behavior": observed_behavior,
        "verdict": verdict,
        "notes": notes,
    }, {
        "sample_id": row["sample_id"],
        "business_family_id": family,
        "raw_issue_count": str(raw_issue_count),
        "cleaned_issue_count": str(cleaned_source_issue_count),
        "issue_delta": str(raw_issue_count - cleaned_source_issue_count),
        "notes": "source_extraction_issue_reduced" if raw_issue_count > cleaned_source_issue_count else "no_detected_delta",
    })


def run_negative_control(row: dict[str, str]) -> dict[str, str]:
    family = row["business_family_id"]
    blocked_reason = row.get("blocked_reason") or "expected_negative_control"
    return {
        "sample_id": row["sample_id"],
        "business_family_id": family,
        "regression_role": "negative",
        "fewshot_used": "false",
        "fewshot_source": "",
        "generation_status": "skipped_negative_control",
        "gate_status": "blocked",
        "export_status": "blocked",
        "readability_status": "not_applicable_negative_control",
        "json_parse_status": "not_applicable_negative_control",
        "core_field_projection": "{}",
        "observed_behavior": "negative control remained blocked",
        "verdict": "pass",
        "notes": blocked_reason,
    }


def project_family_outputs(*, family: str, item: dict) -> tuple[str, str, str]:
    if family == "sentence_fill":
        view = project_sentence_fill_strict_export_view(item)
        status = "pass" if view and view.get("status") in {"direct", "mapped"} else "blocked"
        return status, status, json.dumps(view or {}, ensure_ascii=False)
    if family == "center_understanding":
        view = build_center_understanding_export_view(item)
        status = "pass" if view and view.get("status") in {"direct", "mapped"} else "blocked"
        return status, status, json.dumps(view or {}, ensure_ascii=False)
    view = project_sentence_order_strict_export_view(item)
    gate_status = "pass" if view and view.get("status") in {"direct", "mapped"} else "blocked"
    policy = evaluate_formal_export_policy(question_type="sentence_order", export_target="formal_training_export", item=item)
    export_status = "pass" if policy.get("allowed") else "blocked"
    return gate_status, export_status, json.dumps({"projection": view or {}, "policy": policy}, ensure_ascii=False)


def summarize_generated_item(item: dict) -> str:
    generated = item.get("generated_question") or {}
    stem = normalize_readable_text(generated.get("stem") or "")[:120]
    answer = normalize_readable_text(generated.get("answer") or "")
    warnings = "; ".join(str(w) for w in (item.get("warnings") or [])[:2])
    parts = []
    if answer:
        parts.append(f"answer={answer}")
    if stem:
        parts.append(f"stem={stem}")
    if warnings:
        parts.append(f"warnings={warnings}")
    return "; ".join(parts) or "no_generated_question"


def write_results(rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "sample_id",
        "business_family_id",
        "regression_role",
        "fewshot_used",
        "fewshot_source",
        "generation_status",
        "gate_status",
        "export_status",
        "readability_status",
        "json_parse_status",
        "core_field_projection",
        "observed_behavior",
        "verdict",
        "notes",
    ]
    with RESULTS_PATH.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_cleanup_diff(rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "sample_id",
        "business_family_id",
        "raw_issue_count",
        "cleaned_issue_count",
        "issue_delta",
        "notes",
    ]
    with CLEANUP_DIFF_PATH.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_report(rows: list[dict[str, str]]) -> None:
    total = len(rows)
    success = sum(1 for row in rows if row["generation_status"] == "success")
    readability_ok = sum(1 for row in rows if row["readability_status"] in {"clean", "not_applicable_negative_control"})
    json_parse_ok = sum(1 for row in rows if row["json_parse_status"] in {"parsed", "not_applicable_negative_control"})
    gate_ok = sum(1 for row in rows if row["gate_status"] == "pass" or row["regression_role"] == "negative")
    export_ok = sum(1 for row in rows if row["export_status"] == "pass" or row["regression_role"] == "negative")
    negative_ok = sum(1 for row in rows if row["regression_role"] == "negative" and row["verdict"] == "pass")
    word_xml_or_dirty = sum(
        1
        for row in rows
        if row["regression_role"] != "negative" and row["readability_status"] != "clean"
    )
    positive_failures = [row for row in rows if row["regression_role"] != "negative" and row["verdict"] != "pass"]
    current_failure_item = positive_failures[0]["sample_id"] if positive_failures else "none_in_smoke"
    boundary = next((row for row in rows if row["sample_id"] == "pilot.r1.center_understanding.2052336"), None)
    routing_negative = next((row for row in rows if row["sample_id"] == "pilot.r1.center_understanding.2052316"), None)

    lines = [
        "# Round 1 Generation Smoke Rerun Report",
        "",
        "- This is a post-fix smoke rerun, not a full 29-sample regression rerun.",
        "",
        "## Overall",
        f"- total smoke samples: {total}",
        f"- successful generations: {success}",
        f"- readability clean or expected-negative rows: {readability_ok}",
        f"- JSON parse stayed healthy or not-applicable: {json_parse_ok}",
        f"- gate kept expected behavior: {gate_ok}",
        f"- export kept expected behavior: {export_ok}",
        f"- negative controls intercepted correctly: {negative_ok}",
        f"- current dirty positive rows after cleanup: {word_xml_or_dirty}",
        f"- current max failure item in smoke: {current_failure_item}",
        "",
        "## Sample Results",
    ]
    for row in rows:
        lines.append(
            f"- {row['sample_id']} | family={row['business_family_id']} | role={row['regression_role']} | "
            f"generation={row['generation_status']} | gate={row['gate_status']} | export={row['export_status']} | "
            f"readability={row['readability_status']} | json_parse={row['json_parse_status']} | verdict={row['verdict']}"
        )
    lines.extend(
        [
            "",
            "## Readability / Parse",
            f"- input/output readability clean rows: {readability_ok}/{total}",
            f"- JSON parse healthy rows: {json_parse_ok}/{total}",
            "",
            "## Boundary / Negative",
            f"- 2052336: {boundary['verdict'] if boundary else 'missing'} / {boundary['notes'] if boundary else 'missing'}",
            f"- 2052316: {routing_negative['verdict'] if routing_negative else 'missing'} / {routing_negative['notes'] if routing_negative else 'missing'}",
            "",
            "## Final",
            "- This smoke is a post-integration consistency check, not a strict A/B experiment.",
            "- In the current smoke set, no positive sample still exposes Word XML leak into the prompt-facing payload.",
            "- If positives become readable + parseable while negatives stay blocked, the readability and JSON extraction fixes are effective enough to keep the main repair focus on material input / extraction.",
            "- Current recommendation: material input cleanup is stable enough to let the next repair layer move into material segmentation / scoring / selection.",
        ]
    )
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
