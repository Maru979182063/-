from __future__ import annotations

import json
from pathlib import Path

from playwright.sync_api import sync_playwright


DEMO_URL = "http://127.0.0.1:8011/demo"
EDGE_PATH = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"


def main() -> None:
    item = {
        "item_id": "item-1",
        "batch_id": "batch-1",
        "question_type": "sentence_order",
        "pattern_id": "dual_anchor_lock",
        "current_version_no": 1,
        "current_status": "pending_review",
        "selected_pattern": "dual_anchor_lock",
        "resolved_slots": {},
        "skeleton": {},
        "difficulty_target": "hard",
        "control_logic": {},
        "generation_logic": {},
        "prompt_package": {},
        "generated_question": {
            "question_type": "sentence_order",
            "pattern_id": "dual_anchor_lock",
            "stem": "将以下6个句子重新排列，语序正确的一项是：",
            "options": {
                "A": "①②③④⑤⑥",
                "B": "②①③④⑤⑥",
                "C": "①③②④⑤⑥",
                "D": "①②④③⑤⑥",
            },
            "answer": "A",
            "analysis": "先看首句，再看承接关系，最后确定尾句。",
        },
        "validation_result": {"passed": True, "score": 92},
        "evaluation_result": {"judge_reason": "当前版本可继续人工复核。"},
        "statuses": {
            "build_status": "success",
            "generation_status": "success",
            "validation_status": "passed",
            "review_status": "waiting_review",
        },
        "warnings": [],
        "notes": [],
        "request_snapshot": {"source_question": {"stem": "母题题干"}},
        "material_selection": {
            "material_id": "mat-1",
            "article_id": "art-1",
            "text": "①第一句。\n②第二句。\n③第三句。\n④第四句。\n⑤第五句。\n⑥第六句。",
            "original_text": "原始材料全文。",
            "source": {"source_name": "测试来源", "article_title": "测试标题"},
            "usage_count_before": 0,
        },
        "stem_text": "将以下6个句子重新排列，语序正确的一项是：",
        "material_text": "①第一句。\n②第二句。\n③第三句。\n④第四句。\n⑤第五句。\n⑥第六句。",
        "material_source": {"source_name": "测试来源", "article_title": "测试标题"},
        "material_usage_count_before": 0,
        "material_previously_used": False,
        "revision_count": 0,
    }

    results: dict[str, object] = {}
    errors: list[str] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=EDGE_PATH)
        page = browser.new_page()
        page.on("pageerror", lambda e: errors.append(f"pageerror: {e}"))
        page.on("console", lambda m: errors.append(f"console:{m.type}: {m.text}") if m.type == "error" else None)

        def fulfill_json(route, payload):
            route.fulfill(status=200, content_type="application/json", body=payload)

        page.route(
            "**/api/v1/questions/generate",
            lambda route: fulfill_json(
                route,
                json.dumps(
                    {"batch_id": "batch-1", "batch_meta": {}, "items": [item], "warnings": [], "notes": []},
                    ensure_ascii=False,
                ),
            ),
        )
        page.route(
            "**/api/v1/questions/source-question/parse",
            lambda route: fulfill_json(
                route,
                json.dumps(
                    {
                        "source_question": {
                            "passage": "材料段",
                            "stem": "将以下6个句子重新排列，语序正确的一项是：",
                            "options": {
                                "A": "①②③④⑤⑥",
                                "B": "②①③④⑤⑥",
                                "C": "①③②④⑤⑥",
                                "D": "①②④③⑤⑥",
                            },
                            "answer": "A",
                            "analysis": "解析文本",
                        }
                    },
                    ensure_ascii=False,
                ),
            ),
        )
        page.route(
            "**/replacement-materials?limit=8",
            lambda route: fulfill_json(
                route,
                json.dumps(
                    {
                        "item_id": "item-1",
                        "count": 1,
                        "items": [
                            {
                                "material_id": "mat-2",
                                "label": "备选1",
                                "article_title": "备选标题",
                                "source_name": "备选来源",
                                "material_text": "备选材料全文",
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
            ),
        )
        page.route(
            "**/review-actions",
            lambda route: fulfill_json(
                route,
                json.dumps({"action_id": "act-1", "action": "manual_edit", "item": item}, ensure_ascii=False),
            ),
        )
        page.route(
            "**/confirm",
            lambda route: fulfill_json(
                route,
                json.dumps(
                    {"action_id": "act-2", "action": "confirm", "item": {**item, "current_status": "approved"}},
                    ensure_ascii=False,
                ),
            ),
        )
        page.route("**/api/v1/questions/item-1", lambda route: fulfill_json(route, json.dumps(item, ensure_ascii=False)))
        page.route(
            "**/delivery/export?format=markdown",
            lambda route: route.fulfill(status=200, content_type="text/markdown", body="# export"),
        )

        page.goto(DEMO_URL, wait_until="networkidle", timeout=30000)

        results["question_focus_options"] = page.locator("#questionFocus option").count()

        page.locator("#sourceQuestionPassage").fill("原题整段\n正确答案：A\n解析：解析文本")
        page.locator("#sourceQuestionDetectBtn").click()
        page.wait_for_timeout(300)
        results["auto_detect_bound"] = bool(page.locator("#sourceQuestionStem").input_value())

        page.locator("#questionFocus").select_option(value="sentence_order")
        page.route(
            "**/api/v1/questions/generate",
            lambda route: fulfill_json(
                route,
                json.dumps(
                    {"batch_id": "batch-1", "batch_meta": {}, "items": [item], "warnings": [], "notes": []},
                    ensure_ascii=False,
                ),
            ),
        )
        page.locator("#generateBtn").click()
        page.wait_for_timeout(400)
        results["submit_bound"] = page.locator("#resultScreen").evaluate("el => el.classList.contains('active')")

        page.locator('[data-action="load-replacements"]').click()
        page.wait_for_timeout(200)
        results["load_replacements"] = page.locator(".replacement-select option").count() >= 2

        page.locator(".replacement-select").select_option(value="mat-2")
        page.locator('[data-action="apply-replacement"]').click()
        page.wait_for_timeout(200)
        results["apply_replacement"] = True

        page.locator(".custom-material-input").fill("我自己粘贴的材料")
        page.locator('[data-action="apply-custom-material"]').click()
        page.wait_for_timeout(200)
        results["apply_custom_material"] = True

        page.locator(".manual-stem").fill("新的题干")
        page.locator('[data-action="manual-save"]').click()
        page.wait_for_timeout(200)
        results["manual_save"] = True

        page.locator('[data-action="confirm"]').click()
        page.wait_for_timeout(200)
        results["confirm"] = True

        page.locator('[data-action="discard"]').click()
        page.wait_for_timeout(200)
        results["discard"] = True

        with page.expect_download(timeout=5000) as dl:
            page.locator("#exportApprovedBtn").click()
        results["export"] = dl.value.suggested_filename == "batch_batch-1.md"

        page.locator("#backToBuilderBtn").click()
        page.wait_for_timeout(100)
        results["back_to_builder"] = page.locator("#builderScreen").evaluate("el => el.classList.contains('active')")

        page.evaluate('switchScreen("loading")')
        page.locator("#cancelLoadingBtn").click()
        page.wait_for_timeout(100)
        results["cancel_loading"] = page.locator("#builderScreen").evaluate("el => el.classList.contains('active')")

        browser.close()

    print(json.dumps({"results": results, "errors": errors}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
