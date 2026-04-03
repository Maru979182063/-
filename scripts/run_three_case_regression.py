from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROMPT_ROOT = ROOT / "prompt_skeleton_service"
REPORTS_DIR = ROOT / "reports"
TEST_FILE = Path(r"C:\Users\Maru\Desktop\data\outputs_6types\test.jsonl")
ENV_FILE = ROOT / ".env.demo"


def _load_env() -> None:
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        os.environ[key] = value


def _split_cases(raw_text: str) -> tuple[str, str, str]:
    q1, rest = raw_text.strip().split("\n二、", 1)
    q2_body, q3_body = rest.split("\n三、", 1)
    return q1, "二、" + q2_body, "三、" + q3_body


def _parse_case(block: str, stem_marker: str) -> dict:
    passage, tail = block.split(f"\n{stem_marker}\n", 1)
    lines = tail.splitlines()
    options: dict[str, str] = {}
    idx = 0
    for letter in ("A", "B", "C", "D"):
        line = lines[idx]
        if not line.startswith(f"{letter}."):
            raise ValueError(f"Unexpected option line for {letter}: {line!r}")
        options[letter] = line[2:].strip()
        idx += 1
    answer = lines[idx].split("：", 1)[1].strip()
    idx += 1
    if lines[idx] != "解析：":
        raise ValueError(f"Unexpected analysis marker: {lines[idx]!r}")
    idx += 1
    analysis = "\n".join(lines[idx:]).strip()
    return {
        "passage": passage.split("、", 1)[1].strip(),
        "stem": stem_marker,
        "options": options,
        "answer": answer,
        "analysis": analysis,
    }


def _build_cases() -> list[dict]:
    raw_text = TEST_FILE.read_text(encoding="utf-8")
    q1, q2, q3 = _split_cases(raw_text)
    return [
        {
            "case_id": "Q1",
            "label": "中心理解",
            "question_focus": "中心理解题",
            "payload": _parse_case(q1, "这段文字意在说明："),
        },
        {
            "case_id": "Q2",
            "label": "语句排序",
            "question_focus": "语句排序题",
            "payload": _parse_case(q2, "将以上6个句子重新排列，语序正确的一项是："),
        },
        {
            "case_id": "Q3",
            "label": "语句填空",
            "question_focus": "语句填空题",
            "payload": _parse_case(q3, "填入画横线部分最恰当的一项是："),
        },
    ]


def _run_cases() -> list[dict]:
    _load_env()
    sys.path.insert(0, str(PROMPT_ROOT))

    from app.core.dependencies import (
        get_prompt_template_registry,
        get_question_repository,
        get_registry,
        get_runtime_registry,
    )
    from app.schemas.question import QuestionGenerateRequest, SourceQuestionPayload
    from app.services.prompt_orchestrator import PromptOrchestratorService
    from app.services.question_generation import QuestionGenerationService

    service = QuestionGenerationService(
        orchestrator=PromptOrchestratorService(get_registry()),
        runtime_config=get_runtime_registry().get(),
        repository=get_question_repository(),
        prompt_template_registry=get_prompt_template_registry(),
    )

    results: list[dict] = []
    for case in _build_cases():
        request = QuestionGenerateRequest(
            question_focus=case["question_focus"],
            difficulty_level="中等",
            count=1,
            source_question=SourceQuestionPayload(**case["payload"]),
        )
        try:
            response = service.generate(request)
            item = response["items"][0]
            results.append(
                {
                    "case_id": case["case_id"],
                    "label": case["label"],
                    "ok": True,
                    "status": item.get("current_status"),
                    "selected_business_card": (((item.get("material_selection") or {}).get("source") or {}).get("selected_business_card")),
                    "selected_material_card": (((item.get("material_selection") or {}).get("source") or {}).get("selected_material_card")),
                    "material_text": item.get("material_text"),
                    "original_material_text": ((item.get("material_selection") or {}).get("original_text")),
                    "generated_question": item.get("generated_question"),
                    "validation_result": item.get("validation_result"),
                    "source_question_analysis": ((item.get("request_snapshot") or {}).get("source_question_analysis")),
                }
            )
        except Exception as exc:  # noqa: BLE001
            results.append(
                {
                    "case_id": case["case_id"],
                    "label": case["label"],
                    "ok": False,
                    "error": repr(exc),
                }
            )
    return results


def _render_markdown(results: list[dict]) -> str:
    lines = [
        "# 三道题回归测试结果",
        "",
        f"- 测试时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 测试文件：`{TEST_FILE}`",
        "",
    ]
    for result in results:
        lines.extend(
            [
                f"## {result['case_id']} {result['label']}",
                "",
            ]
        )
        if not result.get("ok"):
            lines.extend(
                [
                    f"- 结果：失败",
                    f"- 错误：`{result.get('error')}`",
                    "",
                ]
            )
            continue

        validation = result.get("validation_result") or {}
        generated = result.get("generated_question") or {}
        options = generated.get("options") or {}
        source_analysis = result.get("source_question_analysis") or {}

        lines.extend(
            [
                f"- 结果：{'通过' if validation.get('passed') else '未通过'}",
                f"- 当前状态：`{result.get('status')}`",
                f"- 选中业务卡：`{result.get('selected_business_card')}`",
                f"- 选中材料卡：`{result.get('selected_material_card')}`",
                f"- 校验分：`{validation.get('score')}`",
                "",
                "### 参考题分析",
                "",
                f"- 命中业务卡：`{', '.join(source_analysis.get('business_card_ids') or [])}`",
                f"- 检索词：`{', '.join(source_analysis.get('query_terms') or [])}`",
                f"- 目标长度：`{source_analysis.get('target_length')}`",
                f"- 结构约束：`{json.dumps(source_analysis.get('structure_constraints') or {}, ensure_ascii=False)}`",
                "",
                "### 材料",
                "",
                "#### 加工后材料",
                "",
                result.get("material_text") or "",
                "",
                "#### 原始材料",
                "",
                result.get("original_material_text") or "",
                "",
                "### 生成题目",
                "",
                generated.get("stem") or "",
                "",
                f"- A. {options.get('A', '')}",
                f"- B. {options.get('B', '')}",
                f"- C. {options.get('C', '')}",
                f"- D. {options.get('D', '')}",
                f"- 答案：`{generated.get('answer')}`",
                "",
                "### 解析",
                "",
                generated.get("analysis") or "",
                "",
                "### 校验",
                "",
                f"- `passed`: {validation.get('passed')}",
                f"- `warnings`: {json.dumps(validation.get('warnings') or [], ensure_ascii=False)}",
                f"- `errors`: {json.dumps(validation.get('errors') or [], ensure_ascii=False)}",
                "",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    results = _run_cases()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = REPORTS_DIR / f"three_case_regression_pack_{timestamp}.json"
    md_path = REPORTS_DIR / f"three_case_regression_pack_{timestamp}.md"
    json_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_markdown(results), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
