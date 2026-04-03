from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a human review pack from the latest business card coverage report.")
    parser.add_argument("--input", type=str, default=None, help="Coverage report json path.")
    parser.add_argument("--per-card", type=int, default=10, help="Max samples per business card.")
    return parser.parse_args()


def latest_coverage_report() -> Path:
    candidates = sorted(REPORTS_DIR.glob("business_card_coverage_*.json"))
    if not candidates:
        raise FileNotFoundError("No business_card_coverage_*.json report found.")
    return candidates[-1]


def main() -> int:
    args = parse_args()
    input_path = Path(args.input) if args.input else latest_coverage_report()
    data = json.loads(input_path.read_text(encoding="utf-8"))
    grouped: dict[str, list[dict]] = defaultdict(list)
    for item in data.get("successful_items", []):
        card_id = (item.get("question_ready_context") or {}).get("selected_business_card")
        if not card_id:
            continue
        if len(grouped[card_id]) >= args.per_card:
            continue
        grouped[card_id].append(item)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    md_path = REPORTS_DIR / f"business_card_review_pack_{timestamp}.md"
    csv_path = REPORTS_DIR / f"business_card_review_pack_{timestamp}.csv"

    md_lines = [
        "# 业务卡人工核查包",
        "",
        f"- 来源报告：`{input_path.name}`",
        f"- 每张业务卡样本上限：`{args.per_card}`",
        "",
    ]

    rows: list[dict[str, str]] = []
    for card_id in sorted(grouped.keys()):
        md_lines.append(f"## {card_id}")
        md_lines.append("")
        for index, item in enumerate(grouped[card_id], start=1):
            ctx = item.get("question_ready_context") or {}
            source = item.get("source") or {}
            profile = item.get("business_feature_profile") or {}
            text = str(item.get("consumable_text") or item.get("text") or "").strip()
            md_lines.extend(
                [
                    f"### 样本 {index}",
                    f"- article_id: `{item.get('article_id')}`",
                    f"- 标题: {item.get('article_title') or '无标题'}",
                    f"- 来源: {source.get('source_name') or '未知来源'}",
                    f"- 材料卡: `{ctx.get('selected_material_card')}`",
                    f"- 原型: `{ctx.get('generation_archetype')}`",
                    f"- 特征类型: `{profile.get('feature_type')}`",
                    f"- 质量分: `{item.get('quality_score')}`",
                    "",
                    "```text",
                    text,
                    "```",
                    "",
                ]
            )
            rows.append(
                {
                    "business_card_id": card_id,
                    "article_id": str(item.get("article_id") or ""),
                    "article_title": str(item.get("article_title") or ""),
                    "source_name": str(source.get("source_name") or ""),
                    "selected_material_card": str(ctx.get("selected_material_card") or ""),
                    "generation_archetype": str(ctx.get("generation_archetype") or ""),
                    "feature_type": str(profile.get("feature_type") or ""),
                    "quality_score": str(item.get("quality_score") or ""),
                    "text": text,
                }
            )

    md_path.write_text("\n".join(md_lines), encoding="utf-8")
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "business_card_id",
                "article_id",
                "article_title",
                "source_name",
                "selected_material_card",
                "generation_archetype",
                "feature_type",
                "quality_score",
                "text",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(md_path)
    print(csv_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
