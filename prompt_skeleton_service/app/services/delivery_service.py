from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any

from fastapi.responses import PlainTextResponse

from app.core.exceptions import DomainError
from app.services.question_repository import QuestionRepository
from app.services.sentence_order_protocol import project_sentence_order_strict_export_view
from app.services.sentence_fill_protocol import project_sentence_fill_strict_export_view


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump()
        if isinstance(dumped, dict):
            return dumped
    return {}


def build_sentence_fill_public_export_view(item: dict | None) -> dict | None:
    view = project_sentence_fill_strict_export_view(item)
    if not view:
        return None
    return {
        "status": view.get("status"),
        "blank_position": view.get("blank_position"),
        "function_type": view.get("function_type"),
        "logic_relation": view.get("logic_relation"),
        "blocked_reason": view.get("blocked_reason"),
    }


def build_center_understanding_export_view(item: dict | None) -> dict | None:
    payload = _as_mapping(item)
    if str(payload.get("question_type") or "").strip() != "main_idea":
        return None

    request_snapshot = _as_mapping(payload.get("request_snapshot"))
    generated_question = _as_mapping(payload.get("generated_question"))
    question_card_id = str(request_snapshot.get("question_card_id") or "").strip()
    sources = [
        ("item.business_subtype", str(payload.get("business_subtype") or "").strip()),
        ("generated_question.business_subtype", str(generated_question.get("business_subtype") or "").strip()),
        ("request_snapshot.business_subtype", str(request_snapshot.get("business_subtype") or "").strip()),
    ]
    if question_card_id:
        if "center_understanding" in question_card_id:
            sources.append(("request_snapshot.question_card_id", "center_understanding"))
        elif "title_selection" in question_card_id:
            sources.append(("request_snapshot.question_card_id", "title_selection"))

    center_targeted = any(value == "center_understanding" for _, value in sources)
    if not center_targeted:
        return None

    leak_source = next((source_name for source_name, value in sources if value == "title_selection"), None)
    if leak_source:
        return {
            "status": "blocked",
            "business_family_id": "center_understanding",
            "business_subtype": None,
            "blocked_reason": f"title_selection_leaked_to_center_understanding_export:{leak_source}",
        }

    return {
        "status": "direct",
        "business_family_id": "center_understanding",
        "business_subtype": "center_understanding",
        "blocked_reason": None,
    }


def build_sentence_order_public_export_view(item: dict | None) -> dict | None:
    view = project_sentence_order_strict_export_view(item)
    if not view:
        return None
    return {
        "status": view.get("status"),
        "candidate_type": view.get("candidate_type"),
        "opening_anchor_type": view.get("opening_anchor_type"),
        "closing_anchor_type": view.get("closing_anchor_type"),
        "blocked_reason": view.get("blocked_reason"),
    }


def evaluate_formal_export_policy(*, question_type: str, export_target: str, item: dict | None = None) -> dict[str, Any]:
    if question_type == "sentence_order" and export_target in {
        "formal_training_export",
        "formal_fewshot_asset",
        "external_baseline_pack",
    }:
        view = project_sentence_order_strict_export_view(item)
        if not view:
            return {
                "allowed": False,
                "status": "blocked",
                "blocked_reason": f"missing_sentence_order_projection_for_export:{export_target}",
            }
        if (
            view.get("status") in {"direct", "mapped"}
            and view.get("blocked_reason") is None
            and view.get("candidate_type") == "sentence_block_group"
            and view.get("opening_anchor_type")
            and view.get("closing_anchor_type")
        ):
            return {
                "allowed": True,
                "status": "allowed",
                "blocked_reason": None,
            }
        return {
            "allowed": False,
            "status": "blocked",
            "blocked_reason": view.get("blocked_reason")
            or f"sentence_order_formal_export_projection_incomplete:{export_target}",
        }
    return {
        "allowed": True,
        "status": "allowed",
        "blocked_reason": None,
    }


class DeliveryService:
    def __init__(self, repository: QuestionRepository) -> None:
        self.repository = repository

    def get_batch_delivery(self, batch_id: str) -> dict:
        batch = self.repository.get_batch(batch_id)
        if batch is None:
            raise DomainError("Question batch not found.", status_code=404, details={"batch_id": batch_id})

        approved_items = [item for item in batch.get("items", []) if item.get("current_status") == "approved"]
        delivery_items = []
        for item_summary in approved_items:
            history = self.repository.get_item_history(item_summary["item_id"])
            current = history["current_version"] if history else None
            item = self.repository.get_item(item_summary["item_id"])
            generated_question = (item or {}).get("generated_question") or {}
            material = (item or {}).get("material_selection") or {}
            material_text = self._clean_material_text((item or {}).get("material_text") or material.get("text") or "")
            sentence_fill_export_view = self._sentence_fill_delivery_view(item)
            center_understanding_export_view = self._center_understanding_delivery_view(item)
            sentence_order_export_view = self._sentence_order_delivery_view(item)
            delivery_items.append(
                {
                    "item_id": item_summary["item_id"],
                    "version_no": current["version_no"] if current else item_summary.get("current_version_no", 1),
                    "question_type": item_summary["question_type"],
                    "business_subtype": (
                        center_understanding_export_view.get("business_subtype")
                        if center_understanding_export_view is not None
                        else (item or {}).get("business_subtype")
                    ),
                    "difficulty_target": (item or {}).get("difficulty_target"),
                    "stem": generated_question.get("stem"),
                    "options": generated_question.get("options", {}),
                    "answer": generated_question.get("answer"),
                    "analysis": generated_question.get("analysis"),
                    "material_id": material.get("material_id"),
                    "document_genre": material.get("document_genre"),
                    "material_text": material_text,
                    "material_source": (item or {}).get("material_source") or material.get("source") or {},
                    "prompt_template_name": current.get("prompt_template_name") if current else None,
                    "prompt_template_version": current.get("prompt_template_version") if current else None,
                    "sentence_fill_export_view": sentence_fill_export_view,
                    "sentence_order_export_view": sentence_order_export_view,
                }
            )
        return {
            "batch_id": batch_id,
            "batch_status": batch["batch_status"],
            "total_count": batch["total_count"],
            "approved_count": len(delivery_items),
            "exported_count": len(delivery_items),
            "items": delivery_items,
        }

    def export_markdown(self, batch_id: str) -> PlainTextResponse:
        delivery = self.get_batch_delivery(batch_id)
        lines = [f"# Delivery Batch {batch_id}", ""]
        for index, item in enumerate(delivery["items"], start=1):
            lines.extend(
                [
                    f"## Question {index}",
                    f"- Item ID: {item['item_id']}",
                    f"- Question Type: {item['question_type']}",
                    f"- Difficulty: {item.get('difficulty_target') or 'unknown'}",
                    f"- Material ID: {item.get('material_id') or 'none'}",
                    f"- Material Source: {(item.get('material_source') or {}).get('source_name') or '-'}",
                    f"- Material Article: {(item.get('material_source') or {}).get('article_title') or '-'}",
                    f"- Material URL: {(item.get('material_source') or {}).get('source_url') or '-'}",
                    *self._sentence_fill_markdown_lines(item),
                    *self._sentence_order_markdown_lines(item),
                    "",
                    "### Material Text",
                    item.get("material_text") or "",
                    "",
                    "### Question",
                    "",
                    item.get("stem") or "",
                    "",
                    *(f"- {key}. {value}" for key, value in (item.get("options") or {}).items()),
                    "",
                    f"Answer: {item.get('answer') or ''}",
                    f"Analysis: {item.get('analysis') or ''}",
                    "",
                ]
            )
        return PlainTextResponse("\n".join(lines), media_type="text/markdown; charset=utf-8")

    def _sentence_fill_delivery_view(self, item: dict | None) -> dict | None:
        view = build_sentence_fill_public_export_view(item)
        if view and view.get("status") == "blocked":
            raise DomainError(
                "Sentence fill export blocked by non-canonical export fields.",
                status_code=409,
                details={
                    "item_id": (item or {}).get("item_id"),
                    "blocked_reason": view.get("blocked_reason"),
                    "sentence_fill_export_view": view,
                },
            )
        return view

    def _center_understanding_delivery_view(self, item: dict | None) -> dict | None:
        view = build_center_understanding_export_view(item)
        if view and view.get("status") == "blocked":
            raise DomainError(
                "Center understanding export blocked by title_selection leakage.",
                status_code=409,
                details={
                    "item_id": (item or {}).get("item_id"),
                    "blocked_reason": view.get("blocked_reason"),
                    "center_understanding_export_view": view,
                },
            )
        return view

    def _sentence_order_delivery_view(self, item: dict | None) -> dict | None:
        view = build_sentence_order_public_export_view(item)
        if view and view.get("status") == "blocked":
            raise DomainError(
                "Sentence order export blocked by non-canonical export fields.",
                status_code=409,
                details={
                    "item_id": (item or {}).get("item_id"),
                    "blocked_reason": view.get("blocked_reason"),
                    "sentence_order_export_view": view,
                },
            )
        return view

    def _sentence_fill_markdown_lines(self, item: dict) -> list[str]:
        view = item.get("sentence_fill_export_view") or self._sentence_fill_delivery_view(item)
        if not view:
            return []
        return [
            f"- Blank Position: {view.get('blank_position') or '-'}",
            f"- Function Type: {view.get('function_type') or '-'}",
            f"- Logic Relation: {view.get('logic_relation') or '-'}",
        ]

    def _sentence_order_markdown_lines(self, item: dict) -> list[str]:
        view = item.get("sentence_order_export_view") or self._sentence_order_delivery_view(item)
        if not view:
            return []
        return [
            f"- Candidate Type: {view.get('candidate_type') or '-'}",
            f"- Opening Anchor Type: {view.get('opening_anchor_type') or '-'}",
            f"- Closing Anchor Type: {view.get('closing_anchor_type') or '-'}",
        ]

    def _clean_material_text(self, text: str) -> str:
        normalized = (text or "").replace("\r\n", "\n").replace("\r", "\n")
        normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()
        if not normalized:
            return ""
        if normalized.count("【关键词】") > 1:
            blocks = [part.strip() for part in re.split(r"(?=【关键词】)", normalized) if part.strip()]
            unique_blocks: list[str] = []
            block_signatures: list[str] = []
            for block in blocks:
                signature = re.sub(r"\s+", "", block)
                if not signature:
                    continue
                if any(
                    signature in existing
                    or existing in signature
                    or SequenceMatcher(None, signature, existing).ratio() >= 0.88
                    for existing in block_signatures
                ):
                    continue
                block_signatures.append(signature)
                unique_blocks.append(block)
            normalized = "\n\n".join(unique_blocks).strip()

        paragraphs = [part.strip() for part in re.split(r"\n\s*\n", normalized) if part.strip()]
        deduped: list[str] = []
        seen_signatures: list[str] = []
        for paragraph in paragraphs:
            signature = re.sub(r"\s+", "", paragraph)
            if not signature:
                continue
            if any(
                signature == existing
                or signature in existing
                or existing in signature
                or SequenceMatcher(None, signature, existing).ratio() >= 0.94
                for existing in seen_signatures
            ):
                continue
            seen_signatures.append(signature)
            deduped.append(paragraph)
        return "\n\n".join(deduped).strip() or normalized
