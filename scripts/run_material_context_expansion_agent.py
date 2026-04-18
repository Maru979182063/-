from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from sqlalchemy import select


ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
REPORTS_ROOT = ROOT / "reports"

os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.core.enums import MaterialStatus, ReleaseChannel  # noqa: E402
from app.domain.services.material_pipeline_v2_service import MaterialPipelineV2Service  # noqa: E402
from app.infra.db.orm.article import ArticleORM  # noqa: E402
from app.infra.db.orm.material_span import MaterialSpanORM  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402


FAMILY_PRIORITY = (
    "center_understanding",
    "title_selection",
    "continuation",
    "sentence_fill",
    "sentence_order",
)


@dataclass
class SampleRecord:
    material: MaterialSpanORM
    article: ArticleORM


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Context-expansion test agent for material-card ingest validation."
    )
    parser.add_argument(
        "--business-family-id",
        type=str,
        default="auto",
        help="Business family id. Use 'auto' to pick from each material's v2 families.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=40,
        help="How many existing materials to test.",
    )
    parser.add_argument(
        "--target-length",
        type=int,
        default=900,
        help="Target expanded character length (Chinese chars).",
    )
    parser.add_argument(
        "--length-tolerance",
        type=int,
        default=180,
        help="Allowed length tolerance for expansion target.",
    )
    parser.add_argument(
        "--candidate-limit",
        type=int,
        default=24,
        help="Candidate limit passed to V2 search.",
    )
    parser.add_argument(
        "--min-card-score",
        type=float,
        default=0.45,
        help="Material card threshold.",
    )
    parser.add_argument(
        "--min-business-card-score",
        type=float,
        default=0.2,
        help="Business card threshold.",
    )
    parser.add_argument(
        "--status",
        type=str,
        default=MaterialStatus.PROMOTED.value,
        help="Material status filter.",
    )
    parser.add_argument(
        "--release-channel",
        type=str,
        default=ReleaseChannel.STABLE.value,
        help="Material release channel filter.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for reports.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    init_db()
    load_plugins()

    output_dir = Path(args.output_dir) if args.output_dir else REPORTS_ROOT
    output_dir.mkdir(parents=True, exist_ok=True)

    session = get_session()
    try:
        service = MaterialPipelineV2Service(session)
        samples = load_samples(
            session=session,
            sample_limit=args.sample_limit,
            status=args.status,
            release_channel=args.release_channel,
        )
        if not samples:
            print("[ctx-expand-agent] no samples found under current filters")
            return 0

        rows: list[dict[str, Any]] = []
        for index, sample in enumerate(samples, start=1):
            row = run_single_sample(
                service=service,
                sample=sample,
                business_family_id=args.business_family_id,
                target_length=args.target_length,
                length_tolerance=args.length_tolerance,
                candidate_limit=args.candidate_limit,
                min_card_score=args.min_card_score,
                min_business_card_score=args.min_business_card_score,
            )
            rows.append(row)
            if index == 1 or index % 5 == 0 or index == len(samples):
                print(
                    f"[ctx-expand-agent] progress={index}/{len(samples)} "
                    f"success={sum(1 for item in rows if item['ingest_success'])} "
                    f"matched={sum(1 for item in rows if item['contains_original_material'])}"
                )

        report = build_report(
            rows=rows,
            args=args,
            sample_count=len(samples),
        )
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = output_dir / f"material_context_expand_agent_{timestamp}.json"
        md_path = output_dir / f"material_context_expand_agent_{timestamp}.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(build_markdown(report), encoding="utf-8")

        print(
            f"[ctx-expand-agent] done total={report['summary']['total']} "
            f"ingest_success={report['summary']['ingest_success_count']} "
            f"ingest_success_rate={report['summary']['ingest_success_rate']}"
        )
        print(f"[ctx-expand-agent] json_report={json_path}")
        print(f"[ctx-expand-agent] md_report={md_path}")
        return 0
    finally:
        session.close()


def load_samples(
    *,
    session,
    sample_limit: int,
    status: str,
    release_channel: str,
) -> list[SampleRecord]:
    stmt = (
        select(MaterialSpanORM, ArticleORM)
        .join(ArticleORM, MaterialSpanORM.article_id == ArticleORM.id)
        .where(
            MaterialSpanORM.is_primary.is_(True),
            MaterialSpanORM.status == status,
            MaterialSpanORM.release_channel == release_channel,
            MaterialSpanORM.v2_index_version.is_not(None),
        )
        .order_by(MaterialSpanORM.updated_at.desc())
        .limit(max(1, sample_limit))
    )
    rows = session.execute(stmt).all()
    return [SampleRecord(material=material, article=article) for material, article in rows]


def run_single_sample(
    *,
    service: MaterialPipelineV2Service,
    sample: SampleRecord,
    business_family_id: str,
    target_length: int,
    length_tolerance: int,
    candidate_limit: int,
    min_card_score: float,
    min_business_card_score: float,
) -> dict[str, Any]:
    material = sample.material
    article = sample.article
    selected_family = resolve_family(material=material, preferred=business_family_id)

    expanded_text, expansion_meta = expand_with_article_context(
        material_text=str(material.text or ""),
        article_text=str(article.clean_text or article.raw_text or ""),
        target_length=max(200, target_length),
        length_tolerance=max(0, length_tolerance),
    )
    pseudo_article = SimpleNamespace(
        id=f"ctx_expand::{material.id}",
        title=(article.title or "") + "（材料扩写测试）",
        source=article.source,
        source_url=article.source_url,
        domain=article.domain,
        clean_text=expanded_text,
        raw_text=expanded_text,
    )
    response = service.pipeline.search(
        articles=[pseudo_article],
        business_family_id=selected_family,
        candidate_limit=max(1, candidate_limit),
        min_card_score=min_card_score,
        min_business_card_score=min_business_card_score,
        target_length=max(200, target_length),
        length_tolerance=max(0, length_tolerance),
        enable_anchor_adaptation=True,
        preserve_anchor=True,
    )
    items = response.get("items") or []
    top = items[0] if items else {}
    contains_material = any(
        loosely_contains((item.get("text") or ""), str(material.text or "")) for item in items
    )
    ingest_success = bool(
        items
        and (top.get("question_ready_context") or {}).get("selected_material_card")
        and (top.get("question_ready_context") or {}).get("selected_business_card")
    )
    return {
        "material_id": material.id,
        "article_id": article.id,
        "business_family_id": selected_family,
        "material_length": len(str(material.text or "")),
        "article_length": len(str(article.clean_text or article.raw_text or "")),
        "expanded_length": len(expanded_text),
        "expansion_meta": expansion_meta,
        "candidate_count": len(items),
        "contains_original_material": contains_material,
        "ingest_success": ingest_success,
        "top_selected_material_card": ((top.get("question_ready_context") or {}).get("selected_material_card")),
        "top_selected_business_card": ((top.get("question_ready_context") or {}).get("selected_business_card")),
        "top_card_score": top.get("selected_card_score"),
        "top_business_card_score": top.get("selected_business_card_score"),
        "top_candidate_type": top.get("candidate_type"),
        "top_text_preview": clip(str(top.get("text") or ""), 180),
        "warnings": response.get("warnings") or [],
    }


def resolve_family(*, material: MaterialSpanORM, preferred: str) -> str:
    if preferred != "auto":
        return preferred
    families = [str(item) for item in (material.v2_business_family_ids or []) if str(item)]
    if not families:
        return "center_understanding"
    known = set(FAMILY_PRIORITY)
    for family in FAMILY_PRIORITY:
        if family in families:
            return family
    for family in families:
        if family in known:
            return family
    return families[0]


def expand_with_article_context(
    *,
    material_text: str,
    article_text: str,
    target_length: int,
    length_tolerance: int,
) -> tuple[str, dict[str, Any]]:
    source = normalize_space(article_text)
    material = normalize_space(material_text)
    if not source:
        return material, {"mode": "material_only_no_article", "found": False}
    if len(source) <= target_length + length_tolerance:
        return source, {"mode": "full_article_short", "found": material in source if material else False}
    if not material:
        return source[: target_length + length_tolerance], {"mode": "article_head_no_material", "found": False}

    start = source.find(material)
    if start < 0:
        anchor = material[: min(42, len(material))]
        if anchor:
            start = source.find(anchor)
    if start < 0:
        return source[: target_length + length_tolerance], {"mode": "fallback_head_slice", "found": False}

    end = start + len(material)
    desired = target_length
    left = start
    right = end
    while right - left < desired and (left > 0 or right < len(source)):
        if left > 0:
            left -= 1
        if right < len(source) and right - left < desired:
            right += 1
        if left == 0 and right == len(source):
            break
    snippet = source[left:right]
    snippet = align_to_sentence_boundary(
        text=source,
        left=left,
        right=right,
        max_length=target_length + length_tolerance,
    )
    return snippet, {"mode": "around_material_window", "found": True, "window": [left, right]}


def align_to_sentence_boundary(*, text: str, left: int, right: int, max_length: int) -> str:
    boundary = re.compile(r"[。！？!?；;\n]")
    left = max(0, left)
    right = min(len(text), right)

    while left > 0 and not boundary.match(text[left - 1]):
        left -= 1
        if right - left > max_length:
            left += 1
            break
    while right < len(text) and not boundary.match(text[right - 1]):
        right += 1
        if right - left > max_length:
            right -= 1
            break
    return text[left:right].strip()


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "")).strip()


def loosely_contains(haystack: str, needle: str) -> bool:
    h = normalize_space(haystack)
    n = normalize_space(needle)
    if not h or not n:
        return False
    if n in h:
        return True
    if len(n) < 30:
        return False
    anchor = n[:20]
    tail = n[-20:]
    return anchor in h and tail in h


def clip(text: str, limit: int) -> str:
    body = (text or "").strip()
    if len(body) <= limit:
        return body
    return body[:limit].rstrip() + "..."


def build_report(*, rows: list[dict[str, Any]], args: argparse.Namespace, sample_count: int) -> dict[str, Any]:
    total = len(rows)
    ingest_success_count = sum(1 for row in rows if row["ingest_success"])
    contains_count = sum(1 for row in rows if row["contains_original_material"])
    nonempty_count = sum(1 for row in rows if row["candidate_count"] > 0)
    avg_expanded_length = round(sum(row["expanded_length"] for row in rows) / total, 2) if total else 0.0
    avg_material_length = round(sum(row["material_length"] for row in rows) / total, 2) if total else 0.0
    return {
        "run_at": datetime.now().isoformat(timespec="seconds"),
        "agent": "material_context_expansion_agent.v1",
        "params": {
            "business_family_id": args.business_family_id,
            "sample_limit": sample_count,
            "target_length": args.target_length,
            "length_tolerance": args.length_tolerance,
            "candidate_limit": args.candidate_limit,
            "min_card_score": args.min_card_score,
            "min_business_card_score": args.min_business_card_score,
            "status": args.status,
            "release_channel": args.release_channel,
        },
        "summary": {
            "total": total,
            "candidate_nonempty_count": nonempty_count,
            "contains_original_material_count": contains_count,
            "ingest_success_count": ingest_success_count,
            "candidate_nonempty_rate": round(nonempty_count / total, 4) if total else 0.0,
            "contains_original_material_rate": round(contains_count / total, 4) if total else 0.0,
            "ingest_success_rate": round(ingest_success_count / total, 4) if total else 0.0,
            "avg_material_length": avg_material_length,
            "avg_expanded_length": avg_expanded_length,
        },
        "rows": rows,
    }


def build_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    params = report.get("params") or {}
    lines = [
        "# 材料上下文扩写吃入测试报告",
        "",
        f"- 运行时间：`{report.get('run_at')}`",
        f"- Agent：`{report.get('agent')}`",
        f"- 业务家族：`{params.get('business_family_id')}`",
        f"- 样本数量：`{summary.get('total')}`",
        f"- 候选非空率：`{summary.get('candidate_nonempty_rate')}`",
        f"- 命中原材料率：`{summary.get('contains_original_material_rate')}`",
        f"- 吃入成功率：`{summary.get('ingest_success_rate')}`",
        f"- 平均原材料长度：`{summary.get('avg_material_length')}`",
        f"- 平均扩写长度：`{summary.get('avg_expanded_length')}`",
        "",
        "## 失败样例（前 20）",
        "",
    ]
    failures = [row for row in (report.get("rows") or []) if not row.get("ingest_success")]
    if not failures:
        lines.append("- 无失败样例")
    else:
        for row in failures[:20]:
            lines.append(
                f"- `{row.get('material_id')}` family=`{row.get('business_family_id')}` "
                f"candidate_count=`{row.get('candidate_count')}` "
                f"contains_original=`{row.get('contains_original_material')}` "
                f"top_material_card=`{row.get('top_selected_material_card')}` "
                f"top_business_card=`{row.get('top_selected_business_card')}`"
            )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
