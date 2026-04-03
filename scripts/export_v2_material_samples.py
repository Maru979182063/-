from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]
PASSAGE_SERVICE_ROOT = ROOT / "passage_service"
os.chdir(PASSAGE_SERVICE_ROOT)
if str(PASSAGE_SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(PASSAGE_SERVICE_ROOT))

from app.main import app  # noqa: E402


FAMILY_REQUESTS = [
    {"label": "标题填入", "business_family_id": "title_selection"},
    {"label": "接语选择", "business_family_id": "continuation"},
    {"label": "语句排序", "business_family_id": "sentence_order"},
    {"label": "语句填空", "business_family_id": "sentence_fill"},
]


def export_samples() -> tuple[Path, Path]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / f"v2_material_samples_{timestamp}.json"
    md_path = reports_dir / f"v2_material_samples_{timestamp}.md"

    payload: dict[str, list[dict]] = {}
    markdown_sections: list[str] = ["# V2 Material Samples", ""]

    with TestClient(app) as client:
        for request in FAMILY_REQUESTS:
            response = client.post(
                "/materials/v2/search",
                json={
                    "business_family_id": request["business_family_id"],
                    "article_limit": 6,
                    "candidate_limit": 6,
                    "min_card_score": 0.55,
                },
            )
            response.raise_for_status()
            data = response.json()
            items = _dedupe_items(data.get("items", []))[:3]
            payload[request["business_family_id"]] = items

            markdown_sections.append(f"## {request['label']} ({request['business_family_id']})")
            markdown_sections.append("")
            if not items:
                markdown_sections.append("无命中样本。")
                markdown_sections.append("")
                continue
            for index, item in enumerate(items, start=1):
                ctx = item.get("question_ready_context") or {}
                presentation = item.get("presentation") or {}
                inspect_text = _inspect_text(item)
                markdown_sections.append(f"### Sample {index}")
                markdown_sections.append(f"- article_id: `{item.get('article_id')}`")
                markdown_sections.append(f"- article_title: {item.get('article_title')}")
                markdown_sections.append(f"- candidate_type: `{item.get('candidate_type')}`")
                markdown_sections.append(f"- selected_material_card: `{ctx.get('selected_material_card')}`")
                markdown_sections.append(f"- generation_archetype: `{ctx.get('generation_archetype')}`")
                markdown_sections.append(f"- quality_score: `{item.get('quality_score')}`")
                markdown_sections.append(f"- planner_source: `{((item.get('meta') or {}).get('planner_source') or 'heuristic_planner')}`")
                markdown_sections.append(f"- planner_score: `{((item.get('meta') or {}).get('planner_score'))}`")
                if presentation.get("mode") == "sentence_order":
                    hints = presentation.get("structure_hints") or {}
                    markdown_sections.append(f"- opening_anchor_type: `{hints.get('opening_anchor_type')}`")
                    markdown_sections.append(f"- middle_structure_type: `{hints.get('middle_structure_type')}`")
                    markdown_sections.append(f"- closing_anchor_type: `{hints.get('closing_anchor_type')}`")
                if presentation.get("mode") == "continuation":
                    markdown_sections.append(f"- anchor_focus: `{presentation.get('anchor_focus')}`")
                    markdown_sections.append(f"- ending_function: `{presentation.get('ending_function')}`")
                if presentation.get("mode") == "sentence_fill":
                    markdown_sections.append(f"- blank_position: `{presentation.get('blank_position')}`")
                    markdown_sections.append(f"- function_type: `{presentation.get('function_type')}`")
                markdown_sections.append("")
                markdown_sections.append("```text")
                markdown_sections.append(inspect_text)
                markdown_sections.append("```")
                markdown_sections.append("")

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text("\n".join(markdown_sections), encoding="utf-8")
    return json_path, md_path


def _dedupe_items(items: list[dict]) -> list[dict]:
    seen: set[str] = set()
    deduped: list[dict] = []
    for item in items:
        key = f"{item.get('article_id')}::{item.get('candidate_type')}::{(item.get('text') or '').strip()}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _inspect_text(item: dict) -> str:
    presentation = item.get("presentation") or {}
    if presentation.get("mode") == "sentence_order":
        parts: list[str] = []
        lead = str(presentation.get("lead_context") or "").strip()
        sortable = str(presentation.get("sortable_block") or item.get("text") or "").strip()
        follow = str(presentation.get("follow_context") or "").strip()
        if lead:
            parts.append("[上文托底]")
            parts.append(lead)
        parts.append("[可排序句组]")
        parts.append(sortable)
        if follow:
            parts.append("[下文托底]")
            parts.append(follow)
        return "\n".join(parts).strip()
    if presentation.get("mode") == "sentence_fill":
        return str(presentation.get("blanked_text") or item.get("consumable_text") or item.get("text") or "")
    if presentation.get("mode") == "continuation":
        return str(presentation.get("tail_window_text") or item.get("consumable_text") or item.get("text") or "")
    return str(item.get("consumable_text") or item.get("text") or "")


if __name__ == "__main__":
    json_path, md_path = export_samples()
    print(json_path)
    print(md_path)
