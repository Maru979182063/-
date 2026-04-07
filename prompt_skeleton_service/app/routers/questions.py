from fastapi import APIRouter, Depends

from app.core.dependencies import get_prompt_template_registry, get_question_repository, get_registry, get_runtime_registry
from app.core.exceptions import DomainError
from app.schemas.question import (
    QuestionBatchDetailResponse,
    QuestionBatchListResponse,
    QuestionConfirmRequest,
    QuestionControlPanelResponse,
    QuestionFineTuneRequest,
    QuestionGenerateRequest,
    QuestionGenerationBatchResponse,
    QuestionGenerationItem,
    QuestionItemListResponse,
    ReplacementMaterialListResponse,
    QuestionReviewActionLog,
    QuestionReviewActionRequest,
    QuestionReviewActionResponse,
    QuestionReviewQueueResponse,
    SourceQuestionDetectRequest,
    SourceQuestionDetectResponse,
    SourceQuestionParseRequest,
    SourceQuestionParseResponse,
)
from app.services.config_registry import ConfigRegistry
from app.services.item_control_service import ItemControlService
from app.services.prompt_orchestrator import PromptOrchestratorService
from app.services.question_generation import QuestionGenerationService
from app.services.question_repository import QuestionRepository
from app.services.question_review import QuestionReviewService
from app.services.prompt_template_registry import PromptTemplateRegistry
from app.services.runtime_registry import RuntimeConfigRegistry
from app.services.source_question_analyzer import SourceQuestionAnalyzer
from app.services.source_question_parser import SourceQuestionParserService

router = APIRouter(prefix="/api/v1/questions", tags=["questions"])


@router.post("/source-question/parse", response_model=SourceQuestionParseResponse)
def parse_source_question(
    request: SourceQuestionParseRequest,
    runtime_registry: RuntimeConfigRegistry = Depends(get_runtime_registry),
) -> SourceQuestionParseResponse:
    service = SourceQuestionParserService(runtime_registry.get())
    parsed = service.parse(request.raw_text)
    return SourceQuestionParseResponse(source_question=parsed)


@router.post("/source-question/detect", response_model=SourceQuestionDetectResponse)
def detect_source_question_fields(request: SourceQuestionDetectRequest) -> SourceQuestionDetectResponse:
    analyzer = SourceQuestionAnalyzer()
    analysis = analyzer.analyze(
        source_question=request.source_question,
        question_type="main_idea",
        business_subtype="center_understanding",
    )
    stem = (request.source_question.stem or "").strip()
    passage = (request.source_question.passage or "").strip()
    combined = f"{stem}\n{passage}"

    question_focus = "中心理解题"
    special_question_type = None
    if any(token in stem for token in ("排序", "重新排列", "语序正确")):
        question_focus = "语句排序题"
        special_question_type = None
    elif any(token in stem for token in ("填入", "横线", "最恰当的一项")):
        question_focus = "语句填空题"
        special_question_type = None
    elif any(token in stem for token in ("接在", "接续", "接语", "衔接")):
        question_focus = "接语选择题"
        special_question_type = None
    elif "标题" in stem:
        question_focus = "标题填入题"
        special_question_type = "选择标题"

    business_card_ids = analysis.get("business_card_ids") or []
    material_structure = "总分归纳"
    if "turning_relation_focus__main_idea" in business_card_ids:
        material_structure = "转折归旨"
    elif "necessary_condition_countermeasure__main_idea" in business_card_ids:
        material_structure = "问题-对策"
    elif "parallel_comprehensive_summary__main_idea" in business_card_ids:
        material_structure = "并列推进"
    elif "cause_effect__conclusion_focus__main_idea" in business_card_ids:
        material_structure = "背景-核心结论"

    text_direction = "评论文"
    if any(token in combined for token in ("根据《", "办法", "条例", "通知", "规定", "意见", "机制", "治理")):
        text_direction = "政策文"
    elif any(token in combined for token in ("实验", "研究", "科学家", "发现", "细胞", "物理", "化学", "生物")):
        text_direction = "科普文"
    elif any(token in combined for token in ("记者", "报道", "消息", "近日", "发布会")):
        text_direction = "新闻报道"

    return SourceQuestionDetectResponse(
        question_focus=question_focus,
        special_question_type=special_question_type,
        text_direction=text_direction,
        material_structure=material_structure,
        topic=analysis.get("topic"),
        business_card_ids=business_card_ids,
        query_terms=analysis.get("query_terms") or [],
    )


@router.post("/generate", response_model=QuestionGenerationBatchResponse)
def generate_questions(
    request: QuestionGenerateRequest,
    registry: ConfigRegistry = Depends(get_registry),
    runtime_registry: RuntimeConfigRegistry = Depends(get_runtime_registry),
    prompt_template_registry: PromptTemplateRegistry = Depends(get_prompt_template_registry),
    repository: QuestionRepository = Depends(get_question_repository),
) -> QuestionGenerationBatchResponse:
    orchestrator = PromptOrchestratorService(registry)
    service = QuestionGenerationService(
        orchestrator=orchestrator,
        runtime_config=runtime_registry.get(),
        repository=repository,
        prompt_template_registry=prompt_template_registry,
    )
    return QuestionGenerationBatchResponse.model_validate(service.generate(request))


@router.get("/batches", response_model=QuestionBatchListResponse)
def list_question_batches(
    limit: int = 50,
    repository: QuestionRepository = Depends(get_question_repository),
) -> QuestionBatchListResponse:
    items = repository.list_batches(limit=limit)
    return QuestionBatchListResponse(count=len(items), items=items)


@router.get("/batches/{batch_id}", response_model=QuestionBatchDetailResponse)
def get_question_batch(
    batch_id: str,
    repository: QuestionRepository = Depends(get_question_repository),
) -> QuestionBatchDetailResponse:
    batch = repository.get_batch(batch_id)
    if batch is None:
        raise DomainError("Question batch not found.", status_code=404, details={"batch_id": batch_id})
    return QuestionBatchDetailResponse.model_validate(batch)


@router.get("", response_model=QuestionItemListResponse)
def list_question_items(
    review_status: str | None = None,
    generation_status: str | None = None,
    question_type: str | None = None,
    batch_id: str | None = None,
    limit: int = 100,
    repository: QuestionRepository = Depends(get_question_repository),
) -> QuestionItemListResponse:
    items = repository.list_items(
        review_status=review_status,
        generation_status=generation_status,
        question_type=question_type,
        batch_id=batch_id,
        limit=limit,
    )
    return QuestionItemListResponse(count=len(items), items=items)


@router.get("/review-queue", response_model=QuestionReviewQueueResponse)
def get_review_queue(
    review_status: str = "waiting_review",
    question_type: str | None = None,
    batch_id: str | None = None,
    limit: int = 100,
    repository: QuestionRepository = Depends(get_question_repository),
) -> QuestionReviewQueueResponse:
    items = repository.list_items(
        review_status=review_status,
        question_type=question_type,
        batch_id=batch_id,
        limit=limit,
    )
    return QuestionReviewQueueResponse(count=len(items), review_status=review_status, items=items)


@router.get("/{item_id}", response_model=QuestionGenerationItem)
def get_question_item(
    item_id: str,
    repository: QuestionRepository = Depends(get_question_repository),
) -> QuestionGenerationItem:
    item = repository.get_item(item_id)
    if item is None:
        raise DomainError("Question item not found.", status_code=404, details={"item_id": item_id})
    return QuestionGenerationItem.model_validate(item)


@router.get("/{item_id}/review-actions", response_model=list[QuestionReviewActionLog])
def list_question_review_actions(
    item_id: str,
    limit: int = 100,
    repository: QuestionRepository = Depends(get_question_repository),
) -> list[QuestionReviewActionLog]:
    item = repository.get_item(item_id)
    if item is None:
        raise DomainError("Question item not found.", status_code=404, details={"item_id": item_id})
    actions = repository.list_review_actions(item_id=item_id, limit=limit)
    return [QuestionReviewActionLog.model_validate(action) for action in actions]


@router.get("/{item_id}/controls", response_model=QuestionControlPanelResponse)
def get_question_item_controls(
    item_id: str,
    registry: ConfigRegistry = Depends(get_registry),
    repository: QuestionRepository = Depends(get_question_repository),
) -> QuestionControlPanelResponse:
    service = ItemControlService(repository, registry)
    return QuestionControlPanelResponse.model_validate(service.get_item_controls(item_id))


@router.get("/{item_id}/replacement-materials", response_model=ReplacementMaterialListResponse)
def list_replacement_materials(
    item_id: str,
    limit: int = 8,
    registry: ConfigRegistry = Depends(get_registry),
    runtime_registry: RuntimeConfigRegistry = Depends(get_runtime_registry),
    prompt_template_registry: PromptTemplateRegistry = Depends(get_prompt_template_registry),
    repository: QuestionRepository = Depends(get_question_repository),
) -> ReplacementMaterialListResponse:
    item = repository.get_item(item_id)
    if item is None:
        raise DomainError("Question item not found.", status_code=404, details={"item_id": item_id})
    orchestrator = PromptOrchestratorService(registry)
    generation_service = QuestionGenerationService(
        orchestrator=orchestrator,
        runtime_config=runtime_registry.get(),
        repository=repository,
        prompt_template_registry=prompt_template_registry,
    )
    options = generation_service.list_replacement_materials(item, limit=limit)
    return ReplacementMaterialListResponse(item_id=item_id, count=len(options), items=options)


@router.post("/{item_id}/fine-tune", response_model=QuestionReviewActionResponse)
def fine_tune_question_item(
    item_id: str,
    request: QuestionFineTuneRequest,
    registry: ConfigRegistry = Depends(get_registry),
    runtime_registry: RuntimeConfigRegistry = Depends(get_runtime_registry),
    prompt_template_registry: PromptTemplateRegistry = Depends(get_prompt_template_registry),
    repository: QuestionRepository = Depends(get_question_repository),
) -> QuestionReviewActionResponse:
    orchestrator = PromptOrchestratorService(registry)
    generation_service = QuestionGenerationService(
        orchestrator=orchestrator,
        runtime_config=runtime_registry.get(),
        repository=repository,
        prompt_template_registry=prompt_template_registry,
    )
    service = QuestionReviewService(repository, generation_service)
    action_request = QuestionReviewActionRequest(
        action="minor_edit",
        requested_action="fine_tune",
        instruction=request.instruction,
        operator=request.operator,
    )
    return QuestionReviewActionResponse.model_validate(service.apply_action(item_id, action_request))


@router.post("/{item_id}/confirm", response_model=QuestionReviewActionResponse)
def confirm_question_item(
    item_id: str,
    request: QuestionConfirmRequest,
    registry: ConfigRegistry = Depends(get_registry),
    runtime_registry: RuntimeConfigRegistry = Depends(get_runtime_registry),
    prompt_template_registry: PromptTemplateRegistry = Depends(get_prompt_template_registry),
    repository: QuestionRepository = Depends(get_question_repository),
) -> QuestionReviewActionResponse:
    orchestrator = PromptOrchestratorService(registry)
    generation_service = QuestionGenerationService(
        orchestrator=orchestrator,
        runtime_config=runtime_registry.get(),
        repository=repository,
        prompt_template_registry=prompt_template_registry,
    )
    service = QuestionReviewService(repository, generation_service)
    action_request = QuestionReviewActionRequest(
        action="confirm",
        operator=request.operator,
    )
    return QuestionReviewActionResponse.model_validate(service.apply_action(item_id, action_request))


@router.post("/{item_id}/review-actions", response_model=QuestionReviewActionResponse)
def review_question_item(
    item_id: str,
    request: QuestionReviewActionRequest,
    registry: ConfigRegistry = Depends(get_registry),
    runtime_registry: RuntimeConfigRegistry = Depends(get_runtime_registry),
    prompt_template_registry: PromptTemplateRegistry = Depends(get_prompt_template_registry),
    repository: QuestionRepository = Depends(get_question_repository),
) -> QuestionReviewActionResponse:
    orchestrator = PromptOrchestratorService(registry)
    generation_service = QuestionGenerationService(
        orchestrator=orchestrator,
        runtime_config=runtime_registry.get(),
        repository=repository,
        prompt_template_registry=prompt_template_registry,
    )
    service = QuestionReviewService(repository, generation_service)
    return QuestionReviewActionResponse.model_validate(service.apply_action(item_id, request))
