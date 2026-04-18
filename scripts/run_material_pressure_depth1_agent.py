from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
REPORTS_ROOT = ROOT / "reports" / "pressure_tests" / "runs"

os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.core.enums import MaterialStatus, ReleaseChannel  # noqa: E402
from app.domain.services.material_pipeline_v2_service import MaterialPipelineV2Service  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Depth1 pressure test agent (cache chain only).")
    parser.add_argument("--manifest-yaml", type=str, default="")
    parser.add_argument("--batch-id", type=str, default="")
    parser.add_argument("--batch-prefix", type=str, default="")
    parser.add_argument(
        "--input-jsonl",
        type=str,
        default=str(ROOT / "reports" / "pressure_tests" / "depth1" / "depth1_expanded_all_2_per_group.jsonl"),
    )
    parser.add_argument("--candidate-limit", type=int, default=20)
    parser.add_argument("--min-card-score", type=float, default=0.55)
    parser.add_argument("--min-business-card-score", type=float, default=0.45)
    parser.add_argument("--review-gate-mode", type=str, default="stable_relaxed")
    parser.add_argument("--use-label-filters", action="store_true", help="When set, map subfamily/pattern_tag into topic/text_direction.")
    parser.add_argument("--output-dir", type=str, default=str(REPORTS_ROOT))
    parser.add_argument("--max-samples", type=int, default=0, help="0 means all.")
    return parser.parse_args()


def _load_rows(path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            if not line.strip():
                continue
            rows.append(json.loads(line))
    return rows


def _load_rows_from_manifest(
    *,
    manifest_yaml: str,
    batch_id: str,
    batch_prefix: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    payload = yaml.safe_load(Path(manifest_yaml).read_text(encoding="utf-8"))
    execution_batches = dict(payload.get("execution_batches") or {})
    selected_batch_ids: list[str] = []
    for current_batch_id in execution_batches:
        if batch_id and current_batch_id != batch_id:
            continue
        if batch_prefix and not current_batch_id.startswith(batch_prefix):
            continue
        selected_batch_ids.append(current_batch_id)
    if not selected_batch_ids and batch_id:
        raise ValueError(f"batch_id not found: {batch_id}")
    if not selected_batch_ids and batch_prefix:
        raise ValueError(f"batch_prefix matched no batches: {batch_prefix}")

    rows: list[dict[str, Any]] = []
    for current_batch_id in selected_batch_ids:
        batch_payload = dict(execution_batches.get(current_batch_id) or {})
        for row in (batch_payload.get("rows") or []):
            merged = dict(row)
            merged.setdefault("execution_batch_id", current_batch_id)
            merged.setdefault("execution_batch_kind", batch_payload.get("batch_kind"))
            merged.setdefault("execution_split", batch_payload.get("split"))
            rows.append(merged)
    return rows, selected_batch_ids


def _extract_query_terms(text: str, *, keep: int = 8) -> list[str]:
    hits = re.findall(r"[\u4e00-\u9fff]{2,8}", text or "")
    ranked: list[str] = []
    seen: set[str] = set()
    for token in hits:
        if token in seen:
            continue
        seen.add(token)
        ranked.append(token)
        if len(ranked) >= keep:
            break
    return ranked


def _group_key(row: dict[str, Any]) -> str:
    return f"{row.get('business_family_id')}||{row.get('subfamily')}||{row.get('pattern_tag')}"


def main() -> int:
    args = parse_args()
    selected_batch_ids: list[str] = []
    if args.manifest_yaml:
        input_rows, selected_batch_ids = _load_rows_from_manifest(
            manifest_yaml=args.manifest_yaml,
            batch_id=args.batch_id,
            batch_prefix=args.batch_prefix,
        )
    else:
        input_rows = _load_rows(args.input_jsonl)
    if args.max_samples > 0:
        input_rows = input_rows[: args.max_samples]
    if not input_rows:
        print("[depth1] no input rows")
        return 1

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    init_db()
    load_plugins()
    session = get_session()
    try:
        service = MaterialPipelineV2Service(session)
        result_rows: list[dict[str, Any]] = []
        for idx, row in enumerate(input_rows, start=1):
            text = str(
                row.get("article_text")
                or row.get("expanded_text")
                or row.get("original_text")
                or row.get("material_text")
                or ""
            )
            business_family_id = str(row.get("business_family_id") or "").strip()
            subfamily = str(row.get("subfamily") or "").strip()
            pattern_tag = str(row.get("pattern_tag") or "").strip()
            query_terms = _extract_query_terms(text, keep=8)
            topic_value = subfamily if args.use_label_filters and subfamily else None
            direction_value = pattern_tag if args.use_label_filters and pattern_tag else None
            payload = {
                "business_family_id": business_family_id,
                "query_terms": query_terms,
                "topic": topic_value,
                "text_direction": direction_value,
                "candidate_limit": max(1, int(args.candidate_limit)),
                "min_card_score": float(args.min_card_score),
                "min_business_card_score": float(args.min_business_card_score),
                "review_gate_mode": args.review_gate_mode,
                "status": MaterialStatus.PROMOTED.value,
                "release_channel": ReleaseChannel.STABLE.value,
            }
            # Depth1 must stay on cache chain only.
            response = service._search_cached(payload) or {}
            items = response.get("items") or []
            top = items[0] if items else {}
            qrc = top.get("question_ready_context") or {}
            result_rows.append(
                {
                    "sample_id": row.get("sample_id"),
                    "question_id": row.get("question_id"),
                    "execution_batch_id": row.get("execution_batch_id"),
                    "execution_batch_kind": row.get("execution_batch_kind"),
                    "execution_split": row.get("execution_split"),
                    "group_key": _group_key(row),
                    "business_family_id": business_family_id,
                    "mother_family_id": row.get("mother_family_id"),
                    "child_family_id": row.get("child_family_id"),
                    "subfamily": subfamily,
                    "pattern_tag": pattern_tag,
                    "question_card_id": row.get("question_card_id"),
                    "expected_material_card_id": row.get("expected_material_card_id"),
                    "article_text_source": row.get("article_text_source"),
                    "article_ready_state": row.get("article_ready_state"),
                    "question_card_features": row.get("question_card_features"),
                    "cache_hit": bool(response.get("cache_hit", False)),
                    "candidate_count": len(items),
                    "top_selected_material_card": qrc.get("selected_material_card"),
                    "top_selected_business_card": qrc.get("selected_business_card"),
                    "top_final_candidate_score": (top.get("selected_task_scoring") or {}).get("final_candidate_score"),
                    "top_recommended": (top.get("selected_task_scoring") or {}).get("recommended"),
                    "top_matches_expected_material_card": qrc.get("selected_material_card") == row.get("expected_material_card_id"),
                    "warnings": response.get("warnings") or [],
                }
            )
            if idx == 1 or idx % 10 == 0 or idx == len(input_rows):
                print(f"[depth1] progress {idx}/{len(input_rows)}")

        # summary
        total = len(result_rows)
        cache_hit_count = sum(1 for x in result_rows if x.get("cache_hit"))
        has_top_count = sum(1 for x in result_rows if x.get("top_selected_material_card") and x.get("top_selected_business_card"))
        expected_leaf_match_count = sum(1 for x in result_rows if x.get("top_matches_expected_material_card"))
        group_card_diversity: dict[str, dict[str, int]] = {}
        per_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
        per_batch: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in result_rows:
            per_group[row["group_key"]].append(row)
            per_batch[str(row.get("execution_batch_id") or "__ungrouped__")].append(row)
        unstable_groups: list[str] = []
        for gk, rows in per_group.items():
            cards = [str(r.get("top_selected_business_card") or "") for r in rows if r.get("top_selected_business_card")]
            card_counter = Counter(cards)
            group_card_diversity[gk] = dict(card_counter)
            if len(card_counter) > 1:
                unstable_groups.append(gk)

        batch_summaries: dict[str, dict[str, Any]] = {}
        for batch_key, rows in sorted(per_batch.items()):
            total_rows = len(rows)
            batch_summaries[batch_key] = {
                "total_rows": total_rows,
                "cache_hit_count": sum(1 for row in rows if row.get("cache_hit")),
                "has_top_card_count": sum(1 for row in rows if row.get("top_selected_material_card")),
                "expected_leaf_match_count": sum(1 for row in rows if row.get("top_matches_expected_material_card")),
                "ready_article_count": sum(1 for row in rows if row.get("article_ready_state") == "ready_previous_expanded"),
                "fallback_article_count": sum(1 for row in rows if row.get("article_ready_state") != "ready_previous_expanded"),
                "child_family_id": rows[0].get("child_family_id"),
                "split": rows[0].get("execution_split"),
                "batch_kind": rows[0].get("execution_batch_kind"),
            }

        summary = {
            "total_samples": total,
            "cache_hit_count": cache_hit_count,
            "cache_hit_rate": round(cache_hit_count / max(1, total), 4),
            "has_top_card_count": has_top_count,
            "has_top_card_rate": round(has_top_count / max(1, total), 4),
            "expected_leaf_match_count": expected_leaf_match_count,
            "expected_leaf_match_rate": round(expected_leaf_match_count / max(1, total), 4),
            "group_count": len(per_group),
            "execution_batch_count": len(per_batch),
            "selected_batch_ids": selected_batch_ids,
            "unstable_group_count": len(unstable_groups),
            "unstable_groups": unstable_groups,
        }

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_json = out_dir / f"depth1_cache_agent_{ts}.json"
        out_md = out_dir / f"depth1_cache_agent_{ts}.md"
        out_payload = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "args": vars(args),
            "summary": summary,
            "group_card_diversity": group_card_diversity,
            "batch_summaries": batch_summaries,
            "rows": result_rows,
        }
        out_json.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        md_lines = [
            "# Depth1 Cache Agent Report",
            "",
            f"- total_samples: `{summary['total_samples']}`",
            f"- cache_hit_rate: `{summary['cache_hit_rate']}`",
            f"- has_top_card_rate: `{summary['has_top_card_rate']}`",
            f"- expected_leaf_match_rate: `{summary['expected_leaf_match_rate']}`",
            f"- execution_batch_count: `{summary['execution_batch_count']}`",
            f"- unstable_group_count: `{summary['unstable_group_count']}`",
            "",
            "## Batch Summaries",
        ]
        if not batch_summaries:
            md_lines.append("- none")
        else:
            for batch_key, batch_summary in batch_summaries.items():
                md_lines.append(
                    "- "
                    f"`{batch_key}` "
                    f"rows=`{batch_summary['total_rows']}` "
                    f"cache_hits=`{batch_summary['cache_hit_count']}` "
                    f"top_cards=`{batch_summary['has_top_card_count']}` "
                    f"expected_leaf_hits=`{batch_summary['expected_leaf_match_count']}` "
                    f"child=`{batch_summary.get('child_family_id')}` "
                    f"split=`{batch_summary.get('split')}`"
                )
        md_lines.extend(
            [
                "",
            "## Unstable Groups",
            ]
        )
        if not unstable_groups:
            md_lines.append("- none")
        else:
            md_lines.extend([f"- {g}" for g in unstable_groups[:100]])
        out_md.write_text("\n".join(md_lines), encoding="utf-8")
        print(f"[depth1] report_json={out_json}")
        print(f"[depth1] report_md={out_md}")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
