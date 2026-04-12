from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from app.core.exceptions import DomainError
from app.schemas.config import FewshotExampleConfig


_CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "question_generation_prompt_assets.yaml"


@lru_cache(maxsize=1)
def load_question_generation_prompt_assets() -> dict[str, Any]:
    if not _CONFIG_PATH.exists():
        raise DomainError(
            "Question generation prompt assets config does not exist.",
            status_code=500,
            details={"config_path": str(_CONFIG_PATH)},
        )

    raw = yaml.safe_load(_CONFIG_PATH.read_text(encoding="utf-8")) or {}
    assets = raw.get("question_generation")
    if not isinstance(assets, dict):
        raise DomainError(
            "Question generation prompt assets config is invalid.",
            status_code=500,
            details={"config_path": str(_CONFIG_PATH)},
        )
    return assets


def resolve_round1_fewshot_family(*, question_type: str, business_subtype: str | None = None) -> str | None:
    if question_type == "sentence_fill":
        return "sentence_fill"
    if question_type == "sentence_order":
        return "sentence_order"
    if question_type == "main_idea" and business_subtype == "center_understanding":
        return "center_understanding"
    return None


@lru_cache(maxsize=1)
def load_round1_fewshot_assets() -> dict[str, list[FewshotExampleConfig]]:
    assets = load_question_generation_prompt_assets()
    config = assets.get("round1_fewshot_assets") or {}
    if not isinstance(config, dict) or not config.get("enabled", False):
        return {}

    packs = config.get("packs") or {}
    if not isinstance(packs, dict):
        raise DomainError(
            "Round 1 few-shot assets config is invalid.",
            status_code=500,
            details={"config_path": str(_CONFIG_PATH), "section": "round1_fewshot_assets.packs"},
        )

    loaded: dict[str, list[FewshotExampleConfig]] = {}
    for family, pack in packs.items():
        if not isinstance(pack, dict):
            raise DomainError(
                "Round 1 few-shot pack config is invalid.",
                status_code=500,
                details={"config_path": str(_CONFIG_PATH), "family": family},
            )
        csv_path_value = str(pack.get("csv_path") or "").strip()
        if not csv_path_value:
            raise DomainError(
                "Round 1 few-shot pack csv_path is required.",
                status_code=500,
                details={"config_path": str(_CONFIG_PATH), "family": family},
            )
        csv_path = (_CONFIG_PATH.parent / csv_path_value).resolve()
        if not csv_path.exists():
            raise DomainError(
                "Round 1 few-shot pack file does not exist.",
                status_code=500,
                details={"config_path": str(_CONFIG_PATH), "family": family, "csv_path": str(csv_path)},
            )
        loaded[family] = _load_round1_pack_examples(csv_path, family=family)
    return loaded


def get_round1_fewshot_examples(*, question_type: str, business_subtype: str | None = None) -> list[FewshotExampleConfig]:
    family = resolve_round1_fewshot_family(question_type=question_type, business_subtype=business_subtype)
    if not family:
        return []
    return list(load_round1_fewshot_assets().get(family, []))


def get_round1_family_prompt_guards(*, question_type: str, business_subtype: str | None = None) -> list[str]:
    family = resolve_round1_fewshot_family(question_type=question_type, business_subtype=business_subtype)
    if not family:
        return []
    assets = load_question_generation_prompt_assets()
    guards = (assets.get("fewshot_prompt_guards") or {}).get(family) or []
    if not isinstance(guards, list):
        raise DomainError(
            "Round 1 few-shot prompt guards config is invalid.",
            status_code=500,
            details={"config_path": str(_CONFIG_PATH), "family": family},
        )
    return [str(item).strip() for item in guards if str(item).strip()]


def _load_round1_pack_examples(csv_path: Path, *, family: str) -> list[FewshotExampleConfig]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    return [_row_to_fewshot_example(row, family=family) for row in rows]


def _row_to_fewshot_example(row: dict[str, str], *, family: str) -> FewshotExampleConfig:
    question_card_id = str(row.get("question_card_id") or "").strip()
    coverage_tag = str(row.get("coverage_tag") or "").strip()
    fewshot_use_reason = str(row.get("fewshot_use_reason") or "").strip()
    canonical_summary_parts: list[str] = []
    fit_slots: dict[str, Any] = {}

    if family == "sentence_fill":
        for field in ("blank_position", "function_type", "logic_relation"):
            value = str(row.get(field) or "").strip()
            if value:
                fit_slots[field] = value
                canonical_summary_parts.append(f"{field}={value}")
    elif family == "center_understanding":
        for field in ("main_axis_source", "argument_structure"):
            value = str(row.get(field) or "").strip()
            if value:
                fit_slots[field] = value
                canonical_summary_parts.append(f"{field}={value}")
    elif family == "sentence_order":
        for field in ("candidate_type", "opening_anchor_type", "closing_anchor_type"):
            value = str(row.get(field) or "").strip()
            if value:
                fit_slots[field] = value
                canonical_summary_parts.append(f"{field}={value}")

    content = "\n".join(
        [
            "Round 1 structure-only few-shot asset.",
            f"sample_id={str(row.get('sample_id') or '').strip()}",
            f"question_card_id={question_card_id or 'unknown'}",
            f"canonical_view={'; '.join(canonical_summary_parts) or 'not_specified'}",
            f"coverage_tag={coverage_tag or 'not_specified'}",
            f"use_reason={fewshot_use_reason or 'not_specified'}",
            "Do not copy topic wording. Reuse only the canonical structure and reasoning shape.",
        ]
    )
    return FewshotExampleConfig.model_validate(
        {
            "title": str(row.get("sample_id") or "round1_example").strip(),
            "content": content,
            "fit_slots": fit_slots,
            "note": coverage_tag or None,
            "asset_family": family,
            "asset_source": "round1_candidate_pack",
        }
    )
