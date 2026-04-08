from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "passage_service" / "passage_service.db"
DEFAULT_OUTPUT_DIR = ROOT / "reports"

CURRENT_THRESHOLDS: dict[str, dict[str, float]] = {
    "main_idea": {
        "recommended": 0.50,
        "review_readiness": 0.60,
        "review_penalty": 0.28,
        "fallback_review_score": 0.40,
    },
    "sentence_fill": {
        "recommended": 0.54,
        "review_readiness": 0.58,
        "review_penalty": 0.28,
        "fallback_review_score": 0.42,
    },
    "sentence_order": {
        "recommended": 0.56,
        "review_readiness": 0.58,
        "review_penalty": 0.24,
        "fallback_review_score": 0.46,
    },
}

FAMILY_LABELS = {
    "title_selection": "main_idea",
    "main_idea": "main_idea",
    "sentence_fill": "sentence_fill",
    "sentence_order": "sentence_order",
}


@dataclass
class Record:
    material_id: str
    article_id: str
    business_family_id: str
    task_family: str
    material_status: str
    release_channel: str
    review_status: str | None
    review_created_at: str | None
    primary_family: str | None
    primary_label: str | None
    quality_score: float
    usage_count: int
    accept_count: int
    reject_count: int
    candidate_type: str
    text_length: int
    selected_material_card: str | None
    selected_business_card: str | None
    question_card_id: str | None
    has_scoring: bool
    final_candidate_score: float
    readiness_score: float
    total_penalty: float
    recommended: bool
    needs_review: bool
    difficulty_band_hint: str | None
    risk_penalties: dict[str, float]
    difficulty_vector: dict[str, float]
    text_preview: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Offline-only V2 material backtest against cached SQLite payloads."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to passage_service SQLite database.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for generated reports.",
    )
    parser.add_argument(
        "--families",
        nargs="*",
        default=None,
        help="Optional subset of business_family_id values to audit.",
    )
    parser.add_argument(
        "--review-top-k",
        type=int,
        default=20,
        help="How many gray/review candidates to include in the priority queue.",
    )
    return parser.parse_args()


def configure_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")


def load_records(db_path: Path, selected_families: set[str] | None) -> list[Record]:
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT
                ms.id,
                ms.article_id,
                ms.status AS material_status,
                ms.release_channel,
                ms.primary_family,
                ms.primary_label,
                ms.quality_score,
                ms.usage_count,
                ms.accept_count,
                ms.reject_count,
                ms.v2_index_payload,
                tr.status AS review_status,
                tr.created_at AS review_created_at
            FROM material_spans ms
            LEFT JOIN tagging_reviews tr ON tr.material_id = ms.id
            WHERE ms.v2_index_version IS NOT NULL
              AND ms.v2_index_payload IS NOT NULL
              AND ms.v2_index_payload != '{}'
            """
        ).fetchall()
    finally:
        connection.close()

    records: list[Record] = []
    for row in rows:
        payload = safe_json_load(row["v2_index_payload"], default={})
        if not isinstance(payload, dict):
            continue
        for business_family_id, item in payload.items():
            if selected_families and business_family_id not in selected_families:
                continue
            if not isinstance(item, dict):
                continue
            scoring = extract_scoring(item)
            task_family = normalize_task_family(
                business_family_id=business_family_id,
                scoring=scoring,
            )
            risk_penalties = safe_float_dict(scoring.get("risk_penalties"))
            difficulty_vector = safe_float_dict(scoring.get("difficulty_vector"))
            question_ready_context = item.get("question_ready_context") or {}
            text = str(item.get("text") or "")
            records.append(
                Record(
                    material_id=str(row["id"]),
                    article_id=str(row["article_id"]),
                    business_family_id=business_family_id,
                    task_family=task_family,
                    material_status=str(row["material_status"] or ""),
                    release_channel=str(row["release_channel"] or ""),
                    review_status=str(row["review_status"]) if row["review_status"] else None,
                    review_created_at=str(row["review_created_at"]) if row["review_created_at"] else None,
                    primary_family=str(row["primary_family"]) if row["primary_family"] else None,
                    primary_label=str(row["primary_label"]) if row["primary_label"] else None,
                    quality_score=to_float(row["quality_score"]),
                    usage_count=int(row["usage_count"] or 0),
                    accept_count=int(row["accept_count"] or 0),
                    reject_count=int(row["reject_count"] or 0),
                    candidate_type=str(item.get("candidate_type") or ""),
                    text_length=len(text),
                    selected_material_card=to_optional_str(question_ready_context.get("selected_material_card")),
                    selected_business_card=to_optional_str(question_ready_context.get("selected_business_card")),
                    question_card_id=to_optional_str(question_ready_context.get("question_card_id")),
                    has_scoring=bool(scoring),
                    final_candidate_score=to_float(scoring.get("final_candidate_score")),
                    readiness_score=to_float(scoring.get("readiness_score")),
                    total_penalty=round(sum(risk_penalties.values()), 4),
                    recommended=bool(scoring.get("recommended")),
                    needs_review=bool(scoring.get("needs_review")),
                    difficulty_band_hint=to_optional_str(scoring.get("difficulty_band_hint")),
                    risk_penalties=risk_penalties,
                    difficulty_vector=difficulty_vector,
                    text_preview=compact_text_preview(text),
                )
            )
    return records


def extract_scoring(item: dict[str, Any]) -> dict[str, Any]:
    scoring = item.get("selected_task_scoring")
    if isinstance(scoring, dict) and scoring:
        return scoring
    meta = item.get("meta") or {}
    fallback = meta.get("scoring")
    if isinstance(fallback, dict):
        return fallback
    return {}


def normalize_task_family(*, business_family_id: str, scoring: dict[str, Any]) -> str:
    task_family = str(scoring.get("task_family") or "").strip()
    if task_family:
        return task_family
    return FAMILY_LABELS.get(business_family_id, business_family_id)


def safe_json_load(raw: Any, *, default: Any) -> Any:
    if raw is None:
        return default
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return default


def safe_float_dict(payload: Any) -> dict[str, float]:
    if not isinstance(payload, dict):
        return {}
    result: dict[str, float] = {}
    for key, value in payload.items():
        result[str(key)] = round(to_float(value), 4)
    return result


def to_float(value: Any) -> float:
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def to_optional_str(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def compact_text_preview(text: str, *, limit: int = 120) -> str:
    compact = " ".join(text.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1] + "…"


def summarize_records(records: list[Record]) -> dict[str, Any]:
    family_groups: dict[str, list[Record]] = defaultdict(list)
    task_groups: dict[str, list[Record]] = defaultdict(list)
    status_counter = Counter()
    release_counter = Counter()
    review_counter = Counter()
    for record in records:
        family_groups[record.business_family_id].append(record)
        task_groups[record.task_family].append(record)
        status_counter[record.material_status] += 1
        release_counter[record.release_channel] += 1
        review_counter[record.review_status or "missing_review"] += 1

    return {
        "total_records": len(records),
        "total_materials": len({record.material_id for record in records}),
        "total_articles": len({record.article_id for record in records}),
        "business_families": sorted(family_groups.keys()),
        "material_status_distribution": dict(status_counter),
        "release_channel_distribution": dict(release_counter),
        "review_status_distribution": dict(review_counter),
        "by_business_family": {
            family_id: summarize_group(group)
            for family_id, group in sorted(family_groups.items())
        },
        "by_task_family": {
            task_family: summarize_group(group)
            for task_family, group in sorted(task_groups.items())
        },
    }


def summarize_group(records: list[Record]) -> dict[str, Any]:
    status_counter = Counter(record.material_status for record in records)
    release_counter = Counter(record.release_channel for record in records)
    review_counter = Counter(record.review_status or "missing_review" for record in records)
    difficulty_counter = Counter(record.difficulty_band_hint or "unknown" for record in records)
    scored_records = [record for record in records if record.has_scoring]

    return {
        "count": len(records),
        "distinct_materials": len({record.material_id for record in records}),
        "distinct_articles": len({record.article_id for record in records}),
        "scoring_available_count": len(scored_records),
        "scoring_missing_count": len(records) - len(scored_records),
        "recommended_count": sum(1 for record in records if record.recommended),
        "needs_review_count": sum(1 for record in records if record.needs_review),
        "gray_count": status_counter.get("gray", 0),
        "promoted_count": status_counter.get("promoted", 0),
        "deprecated_count": status_counter.get("deprecated", 0),
        "stable_release_count": release_counter.get("stable", 0),
        "review_pending_count": review_counter.get("review_pending", 0),
        "auto_tagged_count": review_counter.get("auto_tagged", 0),
        "avg_quality_score": round(mean(record.quality_score for record in records), 4),
        "avg_final_candidate_score": round(mean(record.final_candidate_score for record in scored_records), 4),
        "avg_readiness_score": round(mean(record.readiness_score for record in scored_records), 4),
        "avg_total_penalty": round(mean(record.total_penalty for record in scored_records), 4),
        "p75_final_candidate_score": round(percentile([record.final_candidate_score for record in scored_records], 0.75), 4),
        "p75_readiness_score": round(percentile([record.readiness_score for record in scored_records], 0.75), 4),
        "difficulty_band_distribution": dict(difficulty_counter),
        "status_distribution": dict(status_counter),
        "release_channel_distribution": dict(release_counter),
    }


def build_threshold_report(records: list[Record]) -> dict[str, Any]:
    groups: dict[str, list[Record]] = defaultdict(list)
    for record in records:
        if record.has_scoring:
            groups[record.task_family].append(record)

    report: dict[str, Any] = {}
    for task_family, task_records in sorted(groups.items()):
        if task_family not in CURRENT_THRESHOLDS:
            continue
        current = CURRENT_THRESHOLDS[task_family]
        final_grid = clamp_grid(
            [
                current["recommended"] - 0.08,
                current["recommended"] - 0.04,
                current["recommended"],
                current["recommended"] + 0.04,
                current["recommended"] + 0.08,
            ]
        )
        readiness_grid = clamp_grid(
            [
                current["review_readiness"] - 0.08,
                current["review_readiness"] - 0.04,
                current["review_readiness"],
                current["review_readiness"] + 0.04,
            ]
        )
        penalty_grid = clamp_grid(
            [
                current["review_penalty"] - 0.08,
                current["review_penalty"] - 0.04,
                current["review_penalty"],
                current["review_penalty"] + 0.04,
                current["review_penalty"] + 0.08,
            ]
        )

        scenarios: list[dict[str, Any]] = []
        for final_min in final_grid:
            for readiness_min in readiness_grid:
                for penalty_max in penalty_grid:
                    passed = [
                        record
                        for record in task_records
                        if record.final_candidate_score >= final_min
                        and record.readiness_score >= readiness_min
                        and record.total_penalty <= penalty_max
                    ]
                    if not passed:
                        continue
                    promoted = sum(
                        1
                        for record in passed
                        if record.material_status == "promoted" or record.release_channel == "stable"
                    )
                    deprecated = sum(1 for record in passed if record.material_status == "deprecated")
                    gray = sum(1 for record in passed if record.material_status == "gray")
                    proxy_precision = promoted / len(passed)
                    deprecated_leakage = deprecated / len(passed)
                    objective = round(
                        proxy_precision
                        - deprecated_leakage
                        + min(0.15, len(passed) / max(1, len(task_records)) * 0.15),
                        4,
                    )
                    scenarios.append(
                        {
                            "final_min": round(final_min, 4),
                            "readiness_min": round(readiness_min, 4),
                            "penalty_max": round(penalty_max, 4),
                            "passed_count": len(passed),
                            "passed_rate": round(len(passed) / max(1, len(task_records)), 4),
                            "proxy_stable_precision": round(proxy_precision, 4),
                            "deprecated_leakage": round(deprecated_leakage, 4),
                            "gray_share": round(gray / len(passed), 4),
                            "objective": objective,
                        }
                    )
        scenarios.sort(
            key=lambda item: (
                item["objective"],
                item["proxy_stable_precision"],
                -item["deprecated_leakage"],
                item["passed_count"],
            ),
            reverse=True,
        )
        report[task_family] = {
            "current_thresholds": current,
            "top_scenarios": scenarios[:8],
        }
    return report


def build_review_priority_queue(records: list[Record], *, top_k: int) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for record in records:
        if not record.has_scoring:
            continue
        if record.review_status not in {"review_pending"} and record.material_status != "gray":
            continue
        current = CURRENT_THRESHOLDS.get(record.task_family, {})
        boundary = to_float(current.get("recommended"))
        boundary_gap = abs(record.final_candidate_score - boundary)
        closeness = max(0.0, 1.0 - min(1.0, boundary_gap / 0.20)) if boundary else 0.0
        recency_days = age_in_days(record.review_created_at)
        age_bonus = min(0.20, recency_days / 30.0 * 0.20) if recency_days is not None else 0.0
        priority = (
            0.26 * closeness
            + 0.20 * record.readiness_score
            + 0.18 * min(1.0, record.total_penalty)
            + 0.16 * record.quality_score
            + 0.12 * (1.0 if record.needs_review else 0.0)
            + 0.08 * age_bonus
        )
        candidates.append(
            {
                "priority_score": round(priority, 4),
                "material_id": record.material_id,
                "article_id": record.article_id,
                "business_family_id": record.business_family_id,
                "task_family": record.task_family,
                "material_status": record.material_status,
                "review_status": record.review_status or "missing_review",
                "quality_score": round(record.quality_score, 4),
                "final_candidate_score": round(record.final_candidate_score, 4),
                "readiness_score": round(record.readiness_score, 4),
                "total_penalty": round(record.total_penalty, 4),
                "recommended": record.recommended,
                "needs_review": record.needs_review,
                "difficulty_band_hint": record.difficulty_band_hint,
                "selected_material_card": record.selected_material_card,
                "selected_business_card": record.selected_business_card,
                "review_age_days": recency_days,
                "top_penalties": top_float_items(record.risk_penalties),
                "top_difficulty_dimensions": top_float_items(record.difficulty_vector),
                "text_preview": record.text_preview,
            }
        )
    candidates.sort(
        key=lambda item: (
            item["priority_score"],
            item["needs_review"],
            item["readiness_score"],
            item["total_penalty"],
        ),
        reverse=True,
    )
    return candidates[:top_k]


def age_in_days(timestamp: str | None) -> int | None:
    if not timestamp:
        return None
    normalized = timestamp.replace("Z", "+00:00")
    try:
        instant = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - instant.astimezone(timezone.utc)
    return max(0, delta.days)


def top_float_items(values: dict[str, float], *, limit: int = 3) -> list[dict[str, Any]]:
    ranked = sorted(values.items(), key=lambda item: item[1], reverse=True)
    return [
        {"name": key, "value": round(value, 4)}
        for key, value in ranked[:limit]
        if value > 0
    ]


def clamp_grid(values: list[float]) -> list[float]:
    unique = sorted({round(max(0.0, min(1.0, value)), 4) for value in values})
    return unique


def mean(values: Any) -> float:
    data = list(values)
    if not data:
        return 0.0
    return statistics.fmean(data)


def percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * ratio
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    lower_value = ordered[lower]
    upper_value = ordered[upper]
    return lower_value + (upper_value - lower_value) * (index - lower)


def build_report(
    *,
    db_path: Path,
    records: list[Record],
    families: list[str] | None,
    review_top_k: int,
) -> dict[str, Any]:
    return {
        "run_at": datetime.now().isoformat(timespec="seconds"),
        "db_path": str(db_path),
        "selected_families": families or [],
        "summary": summarize_records(records),
        "threshold_sweep": build_threshold_report(records),
        "review_priority_queue": build_review_priority_queue(records, top_k=review_top_k),
        "notes": [
            "This report is offline-only and does not write back thresholds or statuses.",
            "Threshold sweep uses material status/release_channel as a weak proxy, not a ground-truth outcome label.",
            "Feedback tables are currently empty, so no online acceptance reward was used.",
        ],
    }


def render_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines: list[str] = [
        "# V2 离线回测审计报告",
        "",
        f"- 运行时间：`{report['run_at']}`",
        f"- 数据库：`{report['db_path']}`",
        f"- 选定家族：`{', '.join(report['selected_families']) if report['selected_families'] else 'all'}`",
        f"- 记录总数：`{summary['total_records']}`",
        f"- 去重材料数：`{summary['total_materials']}`",
        f"- 文章数：`{summary['total_articles']}`",
        "",
        "## 总览",
        "",
        f"- material_status：`{json.dumps(summary['material_status_distribution'], ensure_ascii=False)}`",
        f"- release_channel：`{json.dumps(summary['release_channel_distribution'], ensure_ascii=False)}`",
        f"- review_status：`{json.dumps(summary['review_status_distribution'], ensure_ascii=False)}`",
        "",
        "## 分业务家族",
        "",
    ]

    family_rows = summary["by_business_family"]
    if not family_rows:
        lines.append("无可用记录。")
    else:
        lines.extend(
            [
                "| family | count | scoring_ready | recommended | needs_review | gray | promoted | deprecated | avg_quality | avg_final | avg_readiness | avg_penalty |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for family_id, payload in family_rows.items():
            lines.append(
                "| {family} | {count} | {scoring_ready} | {recommended} | {needs_review} | {gray} | {promoted} | {deprecated} | {avg_quality:.4f} | {avg_final:.4f} | {avg_readiness:.4f} | {avg_penalty:.4f} |".format(
                    family=family_id,
                    count=payload["count"],
                    scoring_ready=payload["scoring_available_count"],
                    recommended=payload["recommended_count"],
                    needs_review=payload["needs_review_count"],
                    gray=payload["gray_count"],
                    promoted=payload["promoted_count"],
                    deprecated=payload["deprecated_count"],
                    avg_quality=payload["avg_quality_score"],
                    avg_final=payload["avg_final_candidate_score"],
                    avg_readiness=payload["avg_readiness_score"],
                    avg_penalty=payload["avg_total_penalty"],
                )
            )

    lines.extend(["", "## 阈值扫描", ""])
    threshold_sweep = report["threshold_sweep"]
    if not threshold_sweep:
        lines.append("无可扫描的任务家族。")
    else:
        for task_family, payload in threshold_sweep.items():
            current = payload["current_thresholds"]
            lines.extend(
                [
                    f"### {task_family}",
                    "",
                    f"- 当前阈值：`{json.dumps(current, ensure_ascii=False)}`",
                    "",
                    "| final_min | readiness_min | penalty_max | passed | passed_rate | stable_precision_proxy | deprecated_leakage | gray_share | objective |",
                    "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
                ]
            )
            for scenario in payload["top_scenarios"]:
                lines.append(
                    "| {final_min:.4f} | {readiness_min:.4f} | {penalty_max:.4f} | {passed_count} | {passed_rate:.4f} | {proxy_stable_precision:.4f} | {deprecated_leakage:.4f} | {gray_share:.4f} | {objective:.4f} |".format(
                        **scenario
                    )
                )
            lines.append("")

    lines.extend(["## 审核优先队列", ""])
    queue = report["review_priority_queue"]
    if not queue:
        lines.append("无待审核优先项。")
    else:
        lines.extend(
            [
                "| priority | material_id | family | final | readiness | penalty | recommended | needs_review | review_age_days | card |",
                "| ---: | --- | --- | ---: | ---: | ---: | --- | --- | ---: | --- |",
            ]
        )
        for item in queue:
            review_age_days = item["review_age_days"] if item["review_age_days"] is not None else -1
            selected_business_card = item["selected_business_card"] or ""
            lines.append(
                "| {priority_score:.4f} | `{material_id}` | {business_family_id} | {final_candidate_score:.4f} | {readiness_score:.4f} | {total_penalty:.4f} | {recommended} | {needs_review} | {review_age_days} | `{selected_business_card}` |".format(
                    priority_score=item["priority_score"],
                    material_id=item["material_id"],
                    business_family_id=item["business_family_id"],
                    final_candidate_score=item["final_candidate_score"],
                    readiness_score=item["readiness_score"],
                    total_penalty=item["total_penalty"],
                    recommended=item["recommended"],
                    needs_review=item["needs_review"],
                    review_age_days=review_age_days,
                    selected_business_card=selected_business_card,
                )
            )
        lines.append("")
        for item in queue[: min(10, len(queue))]:
            lines.extend(
                [
                    f"- `{item['material_id']}` {item['business_family_id']} | priority=`{item['priority_score']}` | penalties=`{json.dumps(item['top_penalties'], ensure_ascii=False)}` | preview={item['text_preview']}",
                ]
            )

    lines.extend(["", "## 说明", ""])
    for note in report["notes"]:
        lines.append(f"- {note}")
    lines.append("")
    return "\n".join(lines)


def write_report(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"v2_offline_backtest_{timestamp}.json"
    md_path = output_dir / f"v2_offline_backtest_{timestamp}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, md_path


def main() -> int:
    configure_stdout()
    args = parse_args()
    selected_families = set(args.families or [])
    records = load_records(args.db_path, selected_families if selected_families else None)
    report = build_report(
        db_path=args.db_path,
        records=records,
        families=args.families,
        review_top_k=max(1, args.review_top_k),
    )
    json_path, md_path = write_report(report, args.output_dir)
    print(json.dumps({"json_report": str(json_path), "markdown_report": str(md_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
