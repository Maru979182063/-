from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from app.core.exceptions import DomainError


def _question_card_root() -> Path:
    root = Path(__file__).resolve().parents[3] / "card_specs" / "normalized" / "question_cards"
    if not root.exists():
        raise FileNotFoundError(f"Question card specs not found: {root}")
    return root


def _read_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


@lru_cache
def load_question_card_registry() -> dict[str, Any]:
    cards_by_id: dict[str, dict[str, Any]] = {}
    cards_by_runtime_binding: dict[tuple[str, str | None], list[dict[str, Any]]] = {}

    for path in sorted(_question_card_root().glob("*.yaml")):
        payload = _read_yaml(path)
        card_id = str(payload.get("card_id") or "").strip()
        if not card_id:
            continue
        runtime_binding = payload.get("runtime_binding") or {}
        question_type = str(runtime_binding.get("question_type") or "").strip()
        business_subtype_raw = runtime_binding.get("business_subtype")
        business_subtype = str(business_subtype_raw).strip() if business_subtype_raw not in {None, ""} else None
        cards_by_id[card_id] = payload
        cards_by_runtime_binding.setdefault((question_type, business_subtype), []).append(payload)

    return {
        "cards_by_id": cards_by_id,
        "cards_by_runtime_binding": cards_by_runtime_binding,
    }


class QuestionCardBindingService:
    def __init__(self) -> None:
        self.registry = load_question_card_registry()

    def resolve(
        self,
        *,
        question_card_id: str | None = None,
        question_type: str | None = None,
        business_subtype: str | None = None,
        require_match: bool = False,
    ) -> dict[str, Any]:
        explicit_card_id = str(question_card_id or "").strip() or None
        normalized_question_type = str(question_type or "").strip() or None
        normalized_business_subtype = str(business_subtype).strip() if business_subtype not in {None, ""} else None

        if explicit_card_id:
            card = self.registry["cards_by_id"].get(explicit_card_id)
            if card is None:
                raise DomainError(
                    "Unknown question_card_id.",
                    status_code=422,
                    details={"question_card_id": explicit_card_id},
                )
            runtime_binding = self._runtime_binding_from_card(card)
            mismatch = self._runtime_binding_mismatch(
                runtime_binding=runtime_binding,
                question_type=normalized_question_type,
                business_subtype=normalized_business_subtype,
            )
            return {
                "question_card_id": explicit_card_id,
                "question_card": card,
                "runtime_binding": runtime_binding,
                "binding_source": "explicit_question_card_id",
                "binding_reason": "explicit_question_card_id",
                "warning": mismatch,
            }

        if not normalized_question_type:
            if require_match:
                raise DomainError(
                    "question_card binding requires either question_card_id or question_type.",
                    status_code=422,
                    details={"question_type": question_type, "business_subtype": business_subtype},
                )
            return self._unresolved_binding(
                question_type=normalized_question_type,
                business_subtype=normalized_business_subtype,
                reason="missing_runtime_binding_inputs",
            )

        matches = self.registry["cards_by_runtime_binding"].get((normalized_question_type, normalized_business_subtype), [])
        if len(matches) == 1:
            card = matches[0]
            return {
                "question_card_id": card["card_id"],
                "question_card": card,
                "runtime_binding": self._runtime_binding_from_card(card),
                "binding_source": "runtime_binding_lookup",
                "binding_reason": "question_card.runtime_binding",
                "warning": None,
            }
        if len(matches) > 1:
            raise DomainError(
                "Multiple question cards match the same runtime binding; explicit question_card_id is required.",
                status_code=422,
                details={
                    "question_type": normalized_question_type,
                    "business_subtype": normalized_business_subtype,
                    "matching_question_card_ids": [str(card.get("card_id") or "") for card in matches],
                },
            )

        if require_match:
            raise DomainError(
                "No normalized question card matches the requested runtime binding.",
                status_code=422,
                details={
                    "question_type": normalized_question_type,
                    "business_subtype": normalized_business_subtype,
                },
            )
        return self._unresolved_binding(
            question_type=normalized_question_type,
            business_subtype=normalized_business_subtype,
            reason="no_matching_question_card",
        )

    @staticmethod
    def _runtime_binding_from_card(card: dict[str, Any]) -> dict[str, Any]:
        runtime_binding = card.get("runtime_binding") or {}
        return {
            "question_type": str(runtime_binding.get("question_type") or "").strip(),
            "business_subtype": (
                str(runtime_binding.get("business_subtype")).strip()
                if runtime_binding.get("business_subtype") not in {None, ""}
                else None
            ),
        }

    @staticmethod
    def _runtime_binding_mismatch(
        *,
        runtime_binding: dict[str, Any],
        question_type: str | None,
        business_subtype: str | None,
    ) -> str | None:
        if not question_type and business_subtype is None:
            return None
        runtime_question_type = runtime_binding.get("question_type")
        runtime_business_subtype = runtime_binding.get("business_subtype")
        if runtime_question_type == question_type and runtime_business_subtype == business_subtype:
            return None
        return (
            "explicit_question_card_id_overrode_requested_runtime_binding: "
            f"requested question_type={question_type!r}, business_subtype={business_subtype!r}; "
            f"effective question_type={runtime_question_type!r}, business_subtype={runtime_business_subtype!r}."
        )

    @staticmethod
    def _unresolved_binding(
        *,
        question_type: str | None,
        business_subtype: str | None,
        reason: str,
    ) -> dict[str, Any]:
        return {
            "question_card_id": None,
            "question_card": None,
            "runtime_binding": {
                "question_type": question_type,
                "business_subtype": business_subtype,
            },
            "binding_source": "unresolved",
            "binding_reason": reason,
            "warning": (
                "question_card_binding_unresolved: no normalized question card was closed for "
                f"question_type={question_type!r}, business_subtype={business_subtype!r}."
            ),
        }
