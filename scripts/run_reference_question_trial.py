from __future__ import annotations

import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROMPT_ROOT = ROOT / "prompt_skeleton_service"
PASSAGE_ENV = ROOT / "passage_service" / ".env"


def _load_llm_env() -> None:
    for line in PASSAGE_ENV.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        if key == "PASSAGE_OPENAI_API_KEY":
            os.environ["OPENAI_API_KEY"] = value
        elif key == "PASSAGE_OPENAI_BASE_URL":
            os.environ["OPENAI_BASE_URL"] = value


def main() -> None:
    _load_llm_env()
    sys.path.insert(0, str(PROMPT_ROOT))

    from app.core.dependencies import (
        get_prompt_template_registry,
        get_question_repository,
        get_registry,
        get_runtime_registry,
    )
    from app.schemas.question import (
        MaterialSelectionResult,
        QuestionGenerateRequest,
        SourceQuestionPayload,
    )
    from app.services.prompt_orchestrator import PromptOrchestratorService
    from app.services.question_generation import QuestionGenerationService

    registry = get_registry()
    runtime_registry = get_runtime_registry()
    prompt_template_registry = get_prompt_template_registry()
    repository = get_question_repository()
    orchestrator = PromptOrchestratorService(registry)
    service = QuestionGenerationService(
        orchestrator=orchestrator,
        runtime_config=runtime_registry.get(),
        repository=repository,
        prompt_template_registry=prompt_template_registry,
    )

    material = MaterialSelectionResult(
        material_id="trial_ref_q_material_001",
        article_id="article_e497e01c9f884b9487faa3b4d0c29bab",
        text="\u0034\u8282\u8f66\u53a2\u914d\u5907\u4e86\u0031\u0032\u0036\u4e2a\u53ef\u8eba\u5367\u7684\u7535\u52a8\u6c99\u53d1\uff0c\u6052\u6e29\u7a7a\u8c03\u7cfb\u7edf\u786e\u4fdd\u8212\u9002\u7684\u4f11\u606f\u73af\u5883\uff0c\u201c\u5973\u6027\u53cb\u597d\u8f66\u53a2\u201d\u5316\u89e3\u5973\u6027\u5b89\u5168\u7126\u8651\uff0c\u5e26\u72ec\u7acb\u7535\u5b50\u9501\u7684\u50a8\u7269\u7a7a\u95f4\u3001\u67d4\u5149\u5c0f\u591c\u706f\u3001\u5145\u7535\u63a5\u53e3\u3001\u968f\u65f6\u4f9b\u5e94\u7684\u70ed\u6c34\u4e00\u5e94\u4ff1\u5168\u2026\u2026\u8fd9\u4e9b\u670d\u52a1\u5e76\u4e0d\u201c\u82b1\u54e8\u201d\uff0c\u4f46\u6b63\u662f\u4e2d\u8f6c\u65c5\u5ba2\u6700\u9700\u8981\u7684\u3002",
        original_text="\u0034\u8282\u8f66\u53a2\u914d\u5907\u4e86\u0031\u0032\u0036\u4e2a\u53ef\u8eba\u5367\u7684\u7535\u52a8\u6c99\u53d1\uff0c\u6052\u6e29\u7a7a\u8c03\u7cfb\u7edf\u786e\u4fdd\u8212\u9002\u7684\u4f11\u606f\u73af\u5883\uff0c\u201c\u5973\u6027\u53cb\u597d\u8f66\u53a2\u201d\u5316\u89e3\u5973\u6027\u5b89\u5168\u7126\u8651\uff0c\u5e26\u72ec\u7acb\u7535\u5b50\u9501\u7684\u50a8\u7269\u7a7a\u95f4\u3001\u67d4\u5149\u5c0f\u591c\u706f\u3001\u5145\u7535\u63a5\u53e3\u3001\u968f\u65f6\u4f9b\u5e94\u7684\u70ed\u6c34\u4e00\u5e94\u4ff1\u5168\u2026\u2026\u8fd9\u4e9b\u670d\u52a1\u5e76\u4e0d\u201c\u82b1\u54e8\u201d\uff0c\u4f46\u6b63\u662f\u4e2d\u8f6c\u65c5\u5ba2\u6700\u9700\u8981\u7684\u3002\n\n\u7eb5\u89c2\u6625\u8fd0\u8fd9\u573a\u201c\u4eba\u7c7b\u6700\u5927\u89c4\u6a21\u7684\u5468\u671f\u6027\u8fc1\u5f99\u201d\u7684\u53d1\u5c55\u53d8\u8fc1\uff0c\u4e00\u6761\u4e3b\u7ebf\u4fbf\u662f\u4e0d\u65ad\u4e30\u6ee1\u7684\u4eba\u6587\u5173\u6000\u3002\u0032\u0030\u0031\u0030\u5e74\uff0c\u4e00\u5f20\u201c\u6625\u8fd0\u6bcd\u4eb2\u201d\u7684\u7167\u7247\u5f15\u53d1\u5e7f\u6cdb\u5171\u9e23\u3002\u66fe\u7ecf\uff0c\u5728\u62e5\u6324\u4eba\u6f6e\u4e2d\u201c\u62fc\u4f53\u529b\u201d\u201c\u62fc\u610f\u5fd7\u201d\uff0c\u201c\u5fcd\u4e00\u5fcd\u5c31\u5230\u4e86\u201d\u662f\u6625\u8fd0\u51fa\u884c\u7684\u5e38\u6001\u3002\u5982\u4eca\uff0c\u8d8a\u6765\u8d8a\u591a\u7ec6\u8282\u88ab\u770b\u89c1\u3001\u88ab\u6539\u5584\u3002",
        source={
            "source_name": "\u4eba\u6c11\u7f51",
            "source_url": "http://opinion.people.com.cn/",
            "article_title": "\u4e3a\u201c\u6c38\u4e0d\u53d1\u8f66\u201d\u7684\u5217\u8f66\u70b9\u8d5e\uff08\u6696\u95fb\u70ed\u8bc4\uff09 --\u89c2\u70b9--\u4eba\u6c11\u7f51",
            "selected_business_card": "theme_word_focus__main_idea",
            "selected_material_card": "title_material.problem_essence_judgement",
        },
        source_tail="http://opinion.people.com.cn/",
        primary_label="title_material.problem_essence_judgement",
        document_genre="\u8bc4\u8bba\u6587",
        material_structure_label="\u95ee\u9898-\u5206\u6790-\u7ed3\u8bba",
        material_structure_reason="trial_manual_pick",
        standalone_readability=0.86,
        quality_score=0.83,
        fit_scores={"title_material.problem_essence_judgement": 0.79},
        knowledge_tags=["theme_word_focus__main_idea", "\u8bc4\u8bba\u6587"],
        selection_reason="manual_trial_reference_question_chain",
        anchor_adapted=True,
        anchor_adaptation_reason="trimmed_around_anchor",
        anchor_span={"adapted": True, "reason": "trimmed_around_anchor"},
    )

    class FakeMaterialBridge:
        def __init__(self, chosen: MaterialSelectionResult) -> None:
            self.chosen = chosen
            self.calls: list[dict] = []

        def select_materials(self, **kwargs):
            self.calls.append(kwargs)
            return [self.chosen], []

    fake_bridge = FakeMaterialBridge(material)
    service.material_bridge = fake_bridge

    request = QuestionGenerateRequest(
        question_focus="\u4e2d\u5fc3\u7406\u89e3\u9898",
        difficulty_level="\u4e2d\u7b49",
        text_direction="\u8bc4\u8bba\u6587",
        count=1,
        topic="\u516c\u5171\u670d\u52a1\u4f18\u5316",
        source_question=SourceQuestionPayload(
            passage="\u8fd1\u5e74\u6765\u793e\u4f1a\u5404\u754c\u7684\u7248\u6743\u610f\u8bc6\u666e\u904d\u63d0\u9ad8\uff0c\u516c\u4f17\u5bf9\u4ed8\u8d39\u9605\u8bfb\u3001\u4ed8\u8d39\u542c\u6b4c\u7b49\u5f62\u5f0f\u8d8a\u6765\u8d8a\u63a5\u53d7\uff0c\u4f46\u662f\u5c06\u5168\u6c11\u65cf\u751a\u81f3\u5168\u4e16\u754c\u7684\u6587\u5316\u7470\u5b9d\u79c1\u6709\u5316\u5e76\u4ee5\u6b64\u725f\u5229\uff0c\u6216\u8005\u6253\u7740\u4fdd\u62a4\u6570\u5b57\u7248\u6743\u7684\u65d7\u53f7\u201c\u5f3a\u4e70\u5f3a\u5356\u201d\uff0c\u5e76\u4e0d\u7b26\u5408\u516c\u4f17\u7684\u671f\u5f85\u3002\u60f3\u8bfb\u7535\u5b50\u7248\u540d\u8457\u5fc5\u987b\u8d2d\u4e70\u4ed8\u8d39\u7ae0\u8282\uff0c\u60f3\u542c\u4e00\u9996\u6b4c\u5374\u88ab\u5f3a\u5236\u8d2d\u4e70\u6574\u5f20\u4e13\u8f91\u6216\u5145\u503cVIP\uff0c\u60f3\u67e5\u9605\u8bba\u6587\u4e0d\u5f97\u4e0d\u6309\u9875\u4ed8\u8d39\u2014\u2014\u5f88\u591a\u4eba\u90fd\u4eb2\u8eab\u4f53\u9a8c\u8fc7\u8fd9\u79cd\u65e0\u5948\u3002\u66f4\u8ba9\u4eba\u82e6\u4e0d\u582a\u8a00\u7684\u662f\uff0c\u5404\u79cd\u4f5c\u54c1\u7684\u7248\u672c\u4e94\u82b1\u516b\u95e8\uff0c\u5f80\u5f80\u662f\u82b1\u4e86\u94b1\u5374\u4e70\u4e0d\u5230\u6ee1\u610f\u7248\u672c\u7684\u4f5c\u54c1\u3002",
            stem="\u8fd9\u6bb5\u6587\u5b57\u610f\u5728\u5f3a\u8c03\uff08 \uff09\u3002",
            options={
                "A": "\u516c\u4f17\u671f\u5f85\u66f4\u9ad8\u8d28\u91cf\u7684\u4ed8\u8d39\u9605\u8bfb\u5185\u5bb9",
                "B": "\u6570\u5b57\u7248\u6743\u51fa\u73b0\u7684\u4e00\u4e9b\u4e71\u8c61\u4e0d\u5bb9\u5ffd\u89c6",
                "C": "\u4ed8\u8d39\u9605\u8bfb\u4e0d\u5e94\u6210\u4e3a\u67d0\u4e9b\u4eba\u655b\u8d22\u7684\u624b\u6bb5",
                "D": "\u52a0\u5f3a\u76d1\u7ba1\u662f\u89e3\u51b3\u6570\u5b57\u7248\u6743\u95ee\u9898\u7684\u5173\u952e",
            },
            answer="C",
            analysis="\u6587\u6bb5\u524d\u534a\u627f\u8ba4\u516c\u4f17\u63a5\u53d7\u4ed8\u8d39\u5f62\u5f0f\uff0c\u4f46\u8f6c\u6298\u540e\u91cd\u70b9\u6279\u8bc4\u501f\u6570\u5b57\u7248\u6743\u4e4b\u540d\u5f3a\u4e70\u5f3a\u5356\u3001\u8fc7\u5ea6\u725f\u5229\u4ee5\u53ca\u7248\u672c\u6df7\u4e71\u7b49\u95ee\u9898\uff0c\u6838\u5fc3\u5728\u4e8e\u4ed8\u8d39\u4e0d\u5e94\u5f02\u5316\u4e3a\u655b\u8d22\u624b\u6bb5\u3002",
        ),
    )

    result = service.generate(request)
    sanitized_call = {}
    if fake_bridge.calls:
        for key, value in fake_bridge.calls[0].items():
            if callable(value):
                sanitized_call[key] = f"<callable:{getattr(value, '__name__', 'anonymous')}>"
            else:
                sanitized_call[key] = value
    output = {
        "batch_id": result["batch_id"],
        "notes": result.get("notes", []),
        "warnings": result.get("warnings", []),
        "material_bridge_call": sanitized_call,
        "item": result["items"][0],
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
