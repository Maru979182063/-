from __future__ import annotations

import json
import os
from copy import deepcopy

from playwright.sync_api import sync_playwright


DEMO_URL = os.getenv("DEMO_URL", "http://127.0.0.1:8111/demo")
EDGE_PATH = os.getenv("EDGE_PATH", r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe")

BASE_ITEM = {
    "item_id": "item-1",
    "batch_id": "batch-1",
    "question_type": "sentence_order",
    "pattern_id": "dual_anchor_lock",
    "current_version_no": 1,
    "current_status": "pending_review",
    "latest_action": "generate",
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
    "validation_result": {
        "passed": True,
        "score": 92,
        "errors": [],
        "warnings": ["材料结构与目标难度接近上限。"],
    },
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
        "document_genre": "说明文",
        "source": {"source_name": "测试来源", "article_title": "测试标题"},
        "usage_count_before": 0,
    },
    "stem_text": "将以下6个句子重新排列，语序正确的一项是：",
    "material_text": "①第一句。\n②第二句。\n③第三句。\n④第四句。\n⑤第五句。\n⑥第六句。",
    "material_source": {
        "source_name": "测试来源",
        "article_title": "测试标题",
        "document_genre": "说明文",
        "feedback_snapshot": {
            "selection_state": "hold",
            "review_like_risk": True,
            "repair_suggested": True,
            "final_candidate_score": 0.6723,
            "readiness_score": 0.7312,
            "total_penalty": 0.1804,
            "difficulty_band_hint": "medium_to_hard",
            "difficulty_vector": {
                "ambiguity_score": 0.58,
                "complexity_score": 0.71,
                "reasoning_depth_score": 0.63,
            },
            "key_penalties": {"role_ambiguity_penalty": 0.12, "overlong_penalty": 0.06},
            "key_difficulty_dimensions": {"complexity_score": 0.71, "reasoning_depth_score": 0.63},
            "decision_reason": "borderline_hold_candidate",
            "repair_reason": "role_ambiguity_repairable_risk",
            "quality_difficulty_note": "not_hard_but_currently_weak_candidate",
        },
    },
    "material_usage_count_before": 0,
    "material_previously_used": False,
    "revision_count": 0,
}

CONTROLS_PAYLOAD = {
    "item_id": "item-1",
    "controls": [
        {
            "control_key": "difficulty_target",
            "label": "难度目标",
            "description": "沿用后端控件返回，验证 question_modify 真实提交流程。",
            "mapped_action": "question_modify",
            "read_only": False,
            "current_value": "hard",
            "options": [
                {"value": "medium", "label": "中等"},
                {"value": "hard", "label": "困难"},
            ],
        }
    ],
}

REPLACEMENTS_PAYLOAD = {
    "item_id": "item-1",
    "count": 1,
    "items": [
        {
            "material_id": "mat-2",
            "label": "备选1",
            "article_title": "备选标题",
            "source_name": "备选来源",
            "document_genre": "说明文",
            "material_text": "备选材料全文",
        }
    ],
}


def clone_current_item(item: dict) -> dict:
    return json.loads(json.dumps(item, ensure_ascii=False))


def main() -> None:
    current_item = deepcopy(BASE_ITEM)
    review_action_requests: list[dict] = []
    confirm_requests: list[dict] = []
    results: dict[str, object] = {}
    errors: list[str] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=EDGE_PATH)
        page = browser.new_page()
        page.on("pageerror", lambda e: errors.append(f"pageerror: {e}"))
        page.on("console", lambda m: errors.append(f"console:{m.type}: {m.text}") if m.type == "error" else None)

        def fulfill_json(route, payload):
            route.fulfill(status=200, content_type="application/json", body=json.dumps(payload, ensure_ascii=False))

        def handle_generate(route):
            fulfill_json(
                route,
                {"batch_id": "batch-1", "batch_meta": {}, "items": [clone_current_item(current_item)], "warnings": [], "notes": []},
            )

        def handle_controls(route):
            fulfill_json(route, CONTROLS_PAYLOAD)

        def handle_replacements(route):
            fulfill_json(route, REPLACEMENTS_PAYLOAD)

        def handle_review_actions(route):
            nonlocal current_item
            payload = route.request.post_data_json or {}
            action = payload.get("action")
            review_action_requests.append(payload)

            updated = clone_current_item(current_item)
            updated["current_version_no"] = int(updated.get("current_version_no", 1)) + 1
            updated["latest_action"] = action

            if action == "discard":
                updated["current_status"] = "discarded"
            elif action == "manual_edit":
                patch = payload.get("control_overrides", {}).get("manual_patch", {})
                updated["material_text"] = patch.get("material_text") or updated["material_text"]
                updated["generated_question"]["stem"] = patch.get("stem") or updated["generated_question"]["stem"]
                updated["generated_question"]["options"] = patch.get("options") or updated["generated_question"]["options"]
                updated["generated_question"]["answer"] = patch.get("answer") or updated["generated_question"]["answer"]
                updated["generated_question"]["analysis"] = patch.get("analysis") or updated["generated_question"]["analysis"]
            elif action == "text_modify":
                overrides = payload.get("control_overrides", {})
                if overrides.get("material_text"):
                    updated["material_text"] = overrides["material_text"]
                if overrides.get("material_id") == "mat-2":
                    updated["material_text"] = "备选材料全文"
                    updated["material_source"]["article_title"] = "备选标题"
                    updated["material_source"]["source_name"] = "备选来源"
            elif action == "question_modify":
                updated["difficulty_target"] = payload.get("control_overrides", {}).get("difficulty_target", "hard")

            current_item = updated
            fulfill_json(route, {"action_id": f"act-{len(review_action_requests)}", "action": action, "item": updated})

        def handle_confirm(route):
            nonlocal current_item
            confirm_requests.append(route.request.post_data_json or {})
            updated = clone_current_item(current_item)
            updated["current_status"] = "approved"
            updated["latest_action"] = "confirm"
            current_item = updated
            fulfill_json(route, {"action_id": f"confirm-{len(confirm_requests)}", "action": "confirm", "item": updated})

        page.route("**/api/v1/questions/generate", handle_generate)
        page.route(
            "**/api/v1/questions/source-question/parse",
            lambda route: fulfill_json(
                route,
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
            ),
        )
        page.route("**/api/v1/questions/item-1/controls", handle_controls)
        page.route("**/replacement-materials?limit=8", handle_replacements)
        page.route("**/review-actions", handle_review_actions)
        page.route("**/confirm", handle_confirm)
        page.route(
            "**/delivery/export?format=markdown",
            lambda route: route.fulfill(status=200, content_type="text/markdown", body="# export"),
        )

        page.goto(DEMO_URL, wait_until="networkidle", timeout=30000)

        results["three_screens"] = all(page.locator(f"#{screen}").count() == 1 for screen in ("builderScreen", "loadingScreen", "resultScreen"))
        results["question_focus_options"] = page.locator("#questionFocus option").count()

        page.locator("#sourceQuestionPassage").fill("原题整段\n正确答案：A\n解析：解析文本")
        page.locator("#sourceQuestionDetectBtn").click()
        page.wait_for_timeout(300)
        results["auto_detect_bound"] = bool(page.locator("#sourceQuestionStem").input_value())

        page.locator("#questionFocus").select_option(value="sentence_order")
        page.locator("#generateBtn").click()
        page.wait_for_selector("#resultScreen.active", timeout=5000)
        page.wait_for_selector(".question-card", timeout=5000)
        results["submit_bound"] = page.locator("#resultScreen").evaluate("el => el.classList.contains('active')")

        result_text = page.locator("#resultList").inner_text()
        results["result_fields_rendered"] = all(
            text in result_text for text in ("推荐状态", "最终得分", "难度带提示", "决策原因", "修复原因")
        )

        page.locator("details.result-collapse").nth(1).evaluate("el => el.open = true")
        page.locator('[data-action="question-modify"]').click()
        page.wait_for_timeout(200)
        card_text = page.locator(".question-card").inner_text()
        results["question_modify_request"] = any(req.get("action") == "question_modify" for req in review_action_requests)
        results["question_modify_feedback"] = "最新动作：按参数重做" in card_text

        page.locator("details.result-collapse").nth(2).evaluate("el => el.open = true")
        page.locator('[data-action="load-replacements"]').click()
        page.wait_for_timeout(200)
        page.locator("details.result-collapse").nth(2).evaluate("el => el.open = true")
        results["load_replacements"] = page.locator(".replacement-select option").count() >= 2

        page.locator(".replacement-select").select_option(value="mat-2")
        page.wait_for_timeout(100)
        results["replacement_preview"] = "备选材料全文" in page.locator(".replacement-preview-slot").inner_text()
        page.locator('[data-action="apply-replacement"]').click()
        page.wait_for_timeout(200)
        results["apply_replacement"] = any(
            req.get("action") == "text_modify" and req.get("control_overrides", {}).get("material_id") == "mat-2"
            for req in review_action_requests
        )

        page.locator("details.result-collapse").nth(2).evaluate("el => el.open = true")
        page.locator(".custom-material-input").fill("我自己粘贴的材料")
        page.locator('[data-action="apply-custom-material"]').click()
        page.wait_for_timeout(200)
        results["apply_custom_material"] = any(
            req.get("action") == "text_modify" and req.get("control_overrides", {}).get("material_text") == "我自己粘贴的材料"
            for req in review_action_requests
        )

        page.locator("details.result-collapse").nth(3).evaluate("el => el.open = true")
        page.locator(".manual-stem").fill("新的题干")
        page.locator('[data-action="manual-save"]').click()
        page.wait_for_timeout(200)
        card_text = page.locator(".question-card").inner_text()
        results["manual_save"] = any(req.get("action") == "manual_edit" for req in review_action_requests)
        results["manual_save_feedback"] = "最新动作：手工编辑" in card_text and "新的题干" in card_text

        page.locator('[data-action="confirm"]').click()
        page.wait_for_timeout(200)
        card_text = page.locator(".question-card").inner_text()
        results["confirm"] = bool(confirm_requests) and "当前状态：已通过" in card_text

        page.locator('[data-action="discard"]').click()
        page.wait_for_timeout(200)
        card_text = page.locator(".question-card").inner_text()
        results["discard"] = any(req.get("action") == "discard" for req in review_action_requests) and "已丢弃" in card_text

        browser.close()

    print(json.dumps({"results": results, "errors": errors}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
