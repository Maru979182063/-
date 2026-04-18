from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
REPORTS_ROOT = ROOT / "reports"

os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.core.config import get_config_bundle  # noqa: E402
from app.core.enums import MaterialStatus, ReleaseChannel  # noqa: E402
from app.domain.services.material_pipeline_v2_service import MaterialPipelineV2Service  # noqa: E402
from app.infra.db.repositories.material_span_repo_sqlalchemy import SQLAlchemyMaterialSpanRepository  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402
from app.services.card_registry_v2 import CardRegistryV2  # noqa: E402
from app.services.llm_runtime import get_llm_provider  # noqa: E402


FAMILIES = ("sentence_fill", "center_understanding", "sentence_order")
SOURCE_FAMILY_FOR_REPLAY = {
    "sentence_fill": "sentence_fill",
    "center_understanding": "title_selection",
    "sentence_order": "sentence_order",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run LLM group-wise evaluation against current material ranking.")
    parser.add_argument("--groups-per-family", type=int, default=8)
    parser.add_argument("--holdout-groups-per-family", type=int, default=4)
    parser.add_argument("--group-size", type=int, default=3)
    parser.add_argument("--cache-limit-per-family", type=int, default=180)
    parser.add_argument("--review-gate-mode", type=str, default="stable_relaxed")
    parser.add_argument("--output-dir", type=str, default=str(REPORTS_ROOT))
    parser.add_argument("--tag", type=str, default="baseline")
    return parser.parse_args()


def _json_dump(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _safe_float(value: Any) -> float:
    try:
        return round(float(value or 0.0), 4)
    except (TypeError, ValueError):
        return 0.0


def _top_score(items: list[dict[str, Any]], key: str) -> float:
    if items and isinstance(items[0], dict):
        return _safe_float(items[0].get(key))
    return 0.0


def _load_family_config(family: str) -> dict[str, Any]:
    llm_cfg = get_config_bundle().llm
    family_cfg = dict((((llm_cfg.get("main_card_dual_judge") or {}).get("families") or {}).get(family)) or {})
    return family_cfg


def _family_signal_summary(item: dict[str, Any], family: str) -> dict[str, Any]:
    neutral = dict(item.get("neutral_signal_profile") or {})
    business = dict(item.get("business_feature_profile") or {})
    if family == "sentence_fill":
        profile = dict(business.get("sentence_fill_profile") or {})
        return {
            "blank_position": profile.get("blank_position"),
            "function_type": profile.get("function_type"),
            "logic_relation": profile.get("logic_relation"),
            "bidirectional_validation": _safe_float(profile.get("bidirectional_validation")),
            "reference_dependency": _safe_float(profile.get("reference_dependency")),
            "backward_link_strength": _safe_float(profile.get("backward_link_strength")),
            "forward_link_strength": _safe_float(profile.get("forward_link_strength")),
            "closure_score": _safe_float(neutral.get("closure_score")),
            "titleability": _safe_float(neutral.get("titleability")),
            "material_structure_label": business.get("material_structure_label") or neutral.get("material_structure_label"),
        }
    if family == "center_understanding":
        return {
            "single_center_strength": _safe_float(neutral.get("single_center_strength")),
            "summary_strength": _safe_float(neutral.get("summary_strength")),
            "analysis_to_conclusion_strength": _safe_float(neutral.get("analysis_to_conclusion_strength")),
            "example_to_theme_strength": _safe_float(neutral.get("example_to_theme_strength")),
            "branch_focus_strength": _safe_float(neutral.get("branch_focus_strength")),
            "theme_raise_strength": _safe_float(neutral.get("theme_raise_strength")),
            "document_genre": neutral.get("document_genre"),
            "material_structure_label": business.get("material_structure_label") or neutral.get("material_structure_label"),
        }
    profile = dict(business.get("sentence_order_profile") or {})
    return {
        "opening_signal_strength": _safe_float(profile.get("opening_signal_strength") or neutral.get("opening_signal_strength")),
        "closing_signal_strength": _safe_float(profile.get("closing_signal_strength") or neutral.get("closing_signal_strength")),
        "sequence_integrity": _safe_float(profile.get("sequence_integrity") or neutral.get("sequence_integrity")),
        "multi_path_risk": _safe_float(profile.get("multi_path_risk") or neutral.get("multi_path_risk")),
        "local_binding_strength": _safe_float(profile.get("local_binding_strength") or neutral.get("local_binding_strength")),
        "candidate_type": item.get("candidate_type"),
        "material_structure_label": business.get("material_structure_label") or neutral.get("material_structure_label"),
    }


def _build_candidate_payload(item: dict[str, Any], family: str) -> dict[str, Any]:
    qrc = dict(item.get("question_ready_context") or {})
    scoring = dict(item.get("selected_task_scoring") or {})
    return {
        "candidate_id": str(item.get("candidate_id") or ""),
        "article_id": str(item.get("article_id") or ""),
        "selected_material_card": str(qrc.get("selected_material_card") or item.get("material_card_id") or ""),
        "selected_business_card": str(qrc.get("selected_business_card") or item.get("selected_business_card") or ""),
        "generation_archetype": str(qrc.get("generation_archetype") or ""),
        "material_card_score": _top_score(list(item.get("eligible_material_cards") or []), "score"),
        "business_card_score": _top_score(list(item.get("eligible_business_cards") or []), "score"),
        "llm_selection_score": _safe_float(item.get("llm_selection_score")),
        "llm_generation_readiness_score": _safe_float(((item.get("llm_generation_readiness") or {}).get("score"))),
        "llm_family_match_score": _safe_float(((item.get("llm_family_match_hint") or {}).get("score"))),
        "quality_score": _safe_float(item.get("quality_score")),
        "final_candidate_score": _safe_float(scoring.get("final_candidate_score")),
        "recommended": bool(scoring.get("recommended")),
        "signal_summary": _family_signal_summary(item, family),
        "text": str(item.get("text") or ""),
    }


def _build_eval_prompt(*, family: str, group_id: str, family_config: dict[str, Any], candidates: list[dict[str, Any]]) -> str:
    lines = [
        f"family={family}",
        f"group_id={group_id}",
        "任务：在同组候选中选择最应该排第一的材料。如果三条都不够格，返回 null。",
        "评判重点：不要追随机械分数；优先看哪条最像该 family 的正式承载单元，且更稳定、更可消费。",
        f"family_goal={str(family_config.get('goal') or '').strip()}",
        f"formal_unit_definition={str(family_config.get('formal_unit_definition') or '').strip()}",
        f"strong_accept_definition={str(family_config.get('strong_accept_definition') or '').strip()}",
        f"weak_accept_definition={str(family_config.get('weak_accept_definition') or '').strip()}",
        f"reject_definition={str(family_config.get('reject_definition') or '').strip()}",
        "",
        "候选列表：",
    ]
    for idx, candidate in enumerate(candidates, start=1):
        lines.extend(
            [
                f"[Candidate {idx}]",
                _json_dump(
                    {
                        "candidate_id": candidate["candidate_id"],
                        "selected_material_card": candidate["selected_material_card"],
                        "selected_business_card": candidate["selected_business_card"],
                        "generation_archetype": candidate["generation_archetype"],
                        "material_card_score": candidate["material_card_score"],
                        "business_card_score": candidate["business_card_score"],
                        "llm_selection_score": candidate["llm_selection_score"],
                        "llm_generation_readiness_score": candidate["llm_generation_readiness_score"],
                        "llm_family_match_score": candidate["llm_family_match_score"],
                        "quality_score": candidate["quality_score"],
                        "final_candidate_score": candidate["final_candidate_score"],
                        "recommended": candidate["recommended"],
                        "signal_summary": candidate["signal_summary"],
                    }
                ),
                candidate["text"],
                "",
            ]
        )
    lines.append("请只输出 JSON。")
    return "\n".join(lines)


def _response_schema(candidate_ids: list[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "winning_candidate_id": {
                "type": ["string", "null"],
                "enum": candidate_ids + [None],
            },
            "current_top_is_correct": {"type": "boolean"},
            "confidence": {"type": "number"},
            "reason": {"type": "string"},
            "risk_note": {"type": "string"},
        },
        "required": ["winning_candidate_id", "current_top_is_correct", "confidence", "reason", "risk_note"],
        "additionalProperties": False,
    }


def _collect_candidates(
    *,
    service: MaterialPipelineV2Service,
    repo: SQLAlchemyMaterialSpanRepository,
    family: str,
    limit: int,
    review_gate_mode: str,
) -> list[dict[str, Any]]:
    source_family = SOURCE_FAMILY_FOR_REPLAY[family]
    question_card = service._resolve_search_question_card(
        business_family_id=source_family,
        question_card_id=None,
    )
    materials = repo.list_v2_cached(
        business_family_id=source_family,
        status=MaterialStatus.PROMOTED.value,
        release_channel=ReleaseChannel.STABLE.value,
        limit=limit,
    )
    review_status_map = service._load_review_status_map([material.id for material in materials])
    materials, _ = service._apply_review_gate(
        materials=materials,
        review_status_map=review_status_map,
        mode=review_gate_mode,
    )
    candidates: list[dict[str, Any]] = []
    for material in materials:
        cached_payload = dict(material.v2_index_payload or {})
        cached_item = dict(cached_payload.get(source_family) or {})
        if not cached_item:
            continue
        cached_item["_business_family_id"] = family
        cached_item["_cached_business_family_id"] = family
        qrc = dict(cached_item.get("question_ready_context") or {})
        runtime_binding = dict(qrc.get("runtime_binding") or {})
        runtime_binding["question_type"] = family
        qrc["runtime_binding"] = runtime_binding
        cached_item["question_ready_context"] = qrc
        refreshed = service.pipeline.refresh_cached_item(
            cached_item=cached_item,
            query_terms=[],
            target_length=None,
            length_tolerance=120,
            enable_anchor_adaptation=True,
            preserve_anchor=True,
        )
        refreshed["quality_score"] = float(refreshed.get("quality_score") or getattr(material, "quality_score", 0.0) or 0.0)
        refreshed["review_status"] = review_status_map.get(material.id)
        candidates.append(refreshed)
    return service.pipeline._select_diverse_items(candidates, len(candidates))


def _group_rows(items: list[dict[str, Any]], *, family: str, start_group_index: int, group_count: int, group_size: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    start = start_group_index * group_size
    end = start + group_count * group_size
    capped = items[start:end]
    for group_offset in range(group_count):
        group = capped[group_offset * group_size : (group_offset + 1) * group_size]
        if len(group) < 2:
            continue
        rows.append(
            {
                "family": family,
                "group_id": f"{family}.g{start_group_index + group_offset + 1:02d}",
                "current_top_candidate_id": str(group[0].get("candidate_id") or ""),
                "candidates": [_build_candidate_payload(item, family) for item in group],
            }
        )
    return rows


def _run_group_eval(
    *,
    provider,
    model: str,
    family: str,
    group_id: str,
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    family_config = _load_family_config(family)
    prompt = _build_eval_prompt(
        family=family,
        group_id=group_id,
        family_config=family_config,
        candidates=candidates,
    )
    schema = _response_schema([candidate["candidate_id"] for candidate in candidates])
    instructions = (
        "你是材料评分总裁决，只负责在同组候选里选出最适合当前 family 排第一的材料。"
        "不要机械追随现有 material/business card 或已有分数。"
        "如果现有 top1 只是表面像题感、但稳定性和正式承载性不如别的候选，必须改判。"
        "如果三条都不是真正可消费的正式单元，可以返回 null。"
    )
    return provider.generate_json(
        model=model,
        instructions=instructions,
        input_payload={
            "prompt": prompt,
            "schema_name": f"{family}_{group_id.replace('.', '_')}_winner",
            "schema": schema,
        },
    )


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _build_markdown(summary: dict[str, Any], train_rows: list[dict[str, Any]], holdout_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# LLM Material Group Eval",
        "",
        f"- generated_at: `{summary['generated_at']}`",
        f"- model: `{summary['model']}`",
        f"- train_group_count: `{summary['train_group_count']}`",
        f"- holdout_group_count: `{summary['holdout_group_count']}`",
        "",
        "## Train",
        "",
    ]
    for family, stats in summary["train_by_family"].items():
        lines.extend(
            [
                f"### {family}",
                f"- agreement_rate: `{stats['agreement_rate']}`",
                f"- mismatch_count: `{stats['mismatch_count']}`",
                f"- null_winner_count: `{stats['null_winner_count']}`",
                "",
            ]
        )
    lines.extend(["## Holdout", ""])
    for family, stats in summary["holdout_by_family"].items():
        lines.extend(
            [
                f"### {family}",
                f"- agreement_rate: `{stats['agreement_rate']}`",
                f"- mismatch_count: `{stats['mismatch_count']}`",
                f"- null_winner_count: `{stats['null_winner_count']}`",
                "",
            ]
        )
    mismatches = [row for row in train_rows + holdout_rows if row["winner_matches_current_top"] == "false"]
    lines.extend(["## Mismatches", ""])
    if not mismatches:
        lines.append("- none")
    else:
        for row in mismatches[:18]:
            lines.append(
                f"- `{row['split']}` / `{row['family']}` / `{row['group_id']}`: current_top=`{row['current_top_candidate_id']}` vs llm=`{row['winning_candidate_id']}` | {row['reason']}"
            )
    return "\n".join(lines)


def _family_stats(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {family: [row for row in rows if row["family"] == family] for family in FAMILIES}
    stats: dict[str, dict[str, Any]] = {}
    for family, family_rows in grouped.items():
        total = len(family_rows)
        matches = sum(1 for row in family_rows if row["winner_matches_current_top"] == "true")
        nulls = sum(1 for row in family_rows if not row["winning_candidate_id"])
        stats[family] = {
            "group_count": total,
            "agreement_rate": round(matches / total, 4) if total else 0.0,
            "mismatch_count": total - matches,
            "null_winner_count": nulls,
        }
    return stats


def main() -> int:
    args = parse_args()
    init_db()
    load_plugins()
    provider = get_llm_provider()
    if not provider.is_enabled():
        raise RuntimeError("LLM provider is not enabled.")
    model = str(((get_config_bundle().llm.get("main_card_dual_judge") or {}).get("models") or {}).get("judge_a") or "gpt-4o-mini")
    registry = CardRegistryV2()
    _ = registry
    session = get_session()
    try:
        service = MaterialPipelineV2Service(session)
        repo = SQLAlchemyMaterialSpanRepository(session)
        train_rows: list[dict[str, Any]] = []
        holdout_rows: list[dict[str, Any]] = []
        total_needed = (args.groups_per_family + args.holdout_groups_per_family) * args.group_size
        cache_limit = max(args.cache_limit_per_family, total_needed * 4)
        for family in FAMILIES:
            print(f"[llm-eval] collecting family={family}")
            items = _collect_candidates(
                service=service,
                repo=repo,
                family=family,
                limit=cache_limit,
                review_gate_mode=args.review_gate_mode,
            )
            train_groups = _group_rows(
                items,
                family=family,
                start_group_index=0,
                group_count=args.groups_per_family,
                group_size=args.group_size,
            )
            holdout_groups = _group_rows(
                items,
                family=family,
                start_group_index=args.groups_per_family,
                group_count=args.holdout_groups_per_family,
                group_size=args.group_size,
            )
            for split_name, groups, bucket in (
                ("train", train_groups, train_rows),
                ("holdout", holdout_groups, holdout_rows),
            ):
                for idx, group in enumerate(groups, start=1):
                    result = _run_group_eval(
                        provider=provider,
                        model=model,
                        family=family,
                        group_id=group["group_id"],
                        candidates=group["candidates"],
                    )
                    winner_id = result.get("winning_candidate_id")
                    bucket.append(
                        {
                            "split": split_name,
                            "family": family,
                            "group_id": group["group_id"],
                            "current_top_candidate_id": group["current_top_candidate_id"],
                            "winning_candidate_id": str(winner_id or ""),
                            "winner_matches_current_top": str(winner_id == group["current_top_candidate_id"]).lower(),
                            "current_top_is_correct": str(bool(result.get("current_top_is_correct"))).lower(),
                            "confidence": _safe_float(result.get("confidence")),
                            "reason": str(result.get("reason") or ""),
                            "risk_note": str(result.get("risk_note") or ""),
                            "candidates_json": _json_dump(group["candidates"]),
                        }
                    )
                    if idx == 1 or idx == len(groups):
                        print(f"[llm-eval] family={family} split={split_name} progress={idx}/{len(groups)}")
        summary = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "model": model,
            "tag": args.tag,
            "train_group_count": len(train_rows),
            "holdout_group_count": len(holdout_rows),
            "train_by_family": _family_stats(train_rows),
            "holdout_by_family": _family_stats(holdout_rows),
            "mismatch_counter": dict(Counter(row["family"] for row in train_rows + holdout_rows if row["winner_matches_current_top"] == "false")),
        }
        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = out_dir / f"llm_material_group_eval_{args.tag}_{ts}.json"
        train_csv = out_dir / f"llm_material_group_eval_{args.tag}_{ts}_train.csv"
        holdout_csv = out_dir / f"llm_material_group_eval_{args.tag}_{ts}_holdout.csv"
        md_path = out_dir / f"llm_material_group_eval_{args.tag}_{ts}.md"
        json_path.write_text(
            json.dumps(
                {
                    "summary": summary,
                    "train_rows": train_rows,
                    "holdout_rows": holdout_rows,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        _write_csv(train_csv, train_rows)
        _write_csv(holdout_csv, holdout_rows)
        md_path.write_text(_build_markdown(summary, train_rows, holdout_rows), encoding="utf-8")
        print(json_path)
        print(train_csv)
        print(holdout_csv)
        print(md_path)
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
