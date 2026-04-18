from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
REPORTS_ROOT = ROOT / "reports" / "pressure_tests" / "runs"

os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.domain.services.material_pipeline_v2_service import MaterialPipelineV2Service  # noqa: E402
from app.infra.db.session import get_session, init_db  # noqa: E402
from app.infra.plugins.loader import load_plugins  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Depth2 pressure test agent (pipeline.search realtime chain).")
    parser.add_argument(
        "--input-jsonl",
        type=str,
        default=str(ROOT / "reports" / "pressure_tests" / "depth1" / "depth1_expanded_all_2_per_group.jsonl"),
    )
    parser.add_argument(
        "--depth1-report-json",
        type=str,
        default="",
        help="Optional depth1 report json path for consistency comparison.",
    )
    parser.add_argument("--candidate-limit", type=int, default=20)
    parser.add_argument("--min-card-score", type=float, default=0.55)
    parser.add_argument("--min-business-card-score", type=float, default=0.45)
    parser.add_argument("--target-length", type=int, default=700)
    parser.add_argument("--length-tolerance", type=int, default=180)
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


def _load_depth1_index(path: str) -> dict[str, dict[str, Any]]:
    if not path:
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = payload.get("rows") or []
    return {str(r.get("sample_id")): r for r in rows}


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


def main() -> int:
    args = parse_args()
    input_rows = _load_rows(args.input_jsonl)
    if args.max_samples > 0:
        input_rows = input_rows[: args.max_samples]
    if not input_rows:
        print("[depth2] no input rows")
        return 1
    depth1_index = _load_depth1_index(args.depth1_report_json)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    init_db()
    load_plugins()
    session = get_session()
    try:
        service = MaterialPipelineV2Service(session)
        result_rows: list[dict[str, Any]] = []
        for idx, row in enumerate(input_rows, start=1):
            sample_id = str(row.get("sample_id"))
            text = str(row.get("expanded_text") or row.get("original_text") or "")
            business_family_id = str(row.get("business_family_id") or "").strip()
            query_terms = _extract_query_terms(text, keep=8)
            topic_value = str(row.get("subfamily") or "") if args.use_label_filters else ""
            direction_value = str(row.get("pattern_tag") or "") if args.use_label_filters else ""
            pseudo_article = SimpleNamespace(
                id=f"pressure_depth2::{sample_id}",
                title=f"pressure_depth2::{sample_id}",
                source="pressure_test",
                source_url="",
                domain="pressure.test",
                clean_text=text,
                raw_text=text,
            )
            try:
                response = service.pipeline.search(
                    articles=[pseudo_article],
                    business_family_id=business_family_id,
                    query_terms=query_terms,
                    topic=topic_value or None,
                    text_direction=direction_value or None,
                    candidate_limit=max(1, int(args.candidate_limit)),
                    min_card_score=float(args.min_card_score),
                    min_business_card_score=float(args.min_business_card_score),
                    target_length=int(args.target_length),
                    length_tolerance=int(args.length_tolerance),
                    enable_anchor_adaptation=True,
                    preserve_anchor=True,
                )
                items = response.get("items") or []
                warnings = response.get("warnings") or []
                error_text = ""
            except Exception as exc:  # noqa: BLE001
                items = []
                warnings = []
                error_text = f"{type(exc).__name__}: {exc}"
            top = items[0] if items else {}
            qrc = top.get("question_ready_context") or {}
            depth1 = depth1_index.get(sample_id) or {}
            result_rows.append(
                {
                    "sample_id": sample_id,
                    "business_family_id": business_family_id,
                    "candidate_count": len(items),
                    "top_selected_material_card": qrc.get("selected_material_card"),
                    "top_selected_business_card": qrc.get("selected_business_card"),
                    "top_final_candidate_score": (top.get("selected_task_scoring") or {}).get("final_candidate_score"),
                    "depth1_top_selected_material_card": depth1.get("top_selected_material_card"),
                    "depth1_top_selected_business_card": depth1.get("top_selected_business_card"),
                    "match_material_card_with_depth1": (
                        bool(depth1.get("top_selected_material_card"))
                        and depth1.get("top_selected_material_card") == qrc.get("selected_material_card")
                    ),
                    "match_business_card_with_depth1": (
                        bool(depth1.get("top_selected_business_card"))
                        and depth1.get("top_selected_business_card") == qrc.get("selected_business_card")
                    ),
                    "warnings": warnings,
                    "error": error_text,
                }
            )
            if idx == 1 or idx % 10 == 0 or idx == len(input_rows):
                print(f"[depth2] progress {idx}/{len(input_rows)}")

        total = len(result_rows)
        ok_count = sum(1 for x in result_rows if x.get("top_selected_material_card") and x.get("top_selected_business_card"))
        err_count = sum(1 for x in result_rows if x.get("error"))
        material_match_count = sum(1 for x in result_rows if x.get("match_material_card_with_depth1"))
        business_match_count = sum(1 for x in result_rows if x.get("match_business_card_with_depth1"))
        error_types = Counter()
        for x in result_rows:
            if x.get("error"):
                error_types[x["error"].split(":", 1)[0]] += 1
        summary = {
            "total_samples": total,
            "success_with_top_card_count": ok_count,
            "success_with_top_card_rate": round(ok_count / max(1, total), 4),
            "error_count": err_count,
            "error_rate": round(err_count / max(1, total), 4),
            "material_card_match_count": material_match_count,
            "material_card_match_rate": round(material_match_count / max(1, total), 4),
            "business_card_match_count": business_match_count,
            "business_card_match_rate": round(business_match_count / max(1, total), 4),
            "error_types": dict(error_types),
        }

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_json = out_dir / f"depth2_realtime_agent_{ts}.json"
        out_md = out_dir / f"depth2_realtime_agent_{ts}.md"
        out_payload = {
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "args": vars(args),
            "summary": summary,
            "rows": result_rows,
        }
        out_json.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        md_lines = [
            "# Depth2 Realtime Agent Report",
            "",
            f"- total_samples: `{summary['total_samples']}`",
            f"- success_with_top_card_rate: `{summary['success_with_top_card_rate']}`",
            f"- error_rate: `{summary['error_rate']}`",
            f"- material_card_match_rate: `{summary['material_card_match_rate']}`",
            f"- business_card_match_rate: `{summary['business_card_match_rate']}`",
            "",
            "## Error Types",
        ]
        if not summary["error_types"]:
            md_lines.append("- none")
        else:
            for name, count in summary["error_types"].items():
                md_lines.append(f"- {name}: {count}")
        out_md.write_text("\n".join(md_lines), encoding="utf-8")
        print(f"[depth2] report_json={out_json}")
        print(f"[depth2] report_md={out_md}")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
