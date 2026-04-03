from __future__ import annotations

import re
from difflib import SequenceMatcher

from fastapi.responses import PlainTextResponse

from app.core.exceptions import DomainError
from app.services.question_repository import QuestionRepository


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
            delivery_items.append(
                {
                    "item_id": item_summary["item_id"],
                    "version_no": current["version_no"] if current else item_summary.get("current_version_no", 1),
                    "question_type": item_summary["question_type"],
                    "business_subtype": (item or {}).get("business_subtype"),
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
