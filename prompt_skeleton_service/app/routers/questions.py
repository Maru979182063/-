from uuid import uuid4

from contextlib import nullcontext

from fastapi import APIRouter, Depends, Request

from app.core.dependencies import get_prompt_template_registry, get_question_repository, get_registry, get_runtime_registry
from app.core.exceptions import DomainError
from app.schemas.question import (
    QuestionBatchDetailResponse,
    QuestionBatchListResponse,
    QuestionConfirmRequest,
    QuestionControlPanelResponse,
    QuestionDownloadRequest,
    QuestionDownloadResponse,
    QuestionFineTuneRequest,
    QuestionGenerateRequest,
    QuestionGenerationBatchResponse,
    QuestionGenerationItem,
    QuestionItemListResponse,
    QuestionUsageEventLog,
    ReplacementMaterialListResponse,
    QuestionReviewActionLog,
    QuestionReviewActionRequest,
    QuestionReviewActionResponse,
    QuestionReviewQueueResponse,
    SourceQuestionAssetListResponse,
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
from app.services.generation_gate import acquire_generation_slot

router = APIRouter(prefix="/api/v1/questions", tags=["questions"])
HEAVY_REVIEW_ACTIONS = {"minor_edit", "question_modify", "text_modify", "distractor_patch"}

_UI_SPECIAL_TYPE_BY_BUSINESS_CARD_ID = {
    "turning_relation_focus__main_idea": "turning_relation_focus",
    "cause_effect__conclusion_focus__main_idea": "cause_effect__conclusion_focus",
    "necessary_condition_countermeasure__main_idea": "necessary_condition_countermeasure",
    "parallel_comprehensive_summary__main_idea": "parallel_comprehensive_summary",
    "theme_word_focus__main_idea": "theme_word_focus",
    "sentence_order__head_tail_logic__abstract": "first_background_intro",
    "sentence_order__head_tail_lock__abstract": "rel_turning",
    "sentence_order__deterministic_binding__abstract": "rel_parallel",
    "sentence_order__discourse_logic__abstract": "writing_view_explain",
    "sentence_order__timeline_action_sequence__abstract": "daily_time_timeline",
    "sentence_fill__opening_summary__abstract": "opening_summary",
    "sentence_fill__opening_topic_intro__abstract": "opening_topic_intro",
    "sentence_fill__middle_carry_previous__abstract": "middle_carry_previous",
    "sentence_fill__middle_lead_next__abstract": "middle_lead_next",
    "sentence_fill__middle_bridge_both_sides__abstract": "middle_bridge_both_sides",
    "sentence_fill__ending_summary__abstract": "ending_summary",
    "sentence_fill__ending_countermeasure__abstract": "ending_countermeasure",
}

_UI_CHILD_FAMILY_BY_BUSINESS_CARD_ID = {
    "turning_relation_focus__main_idea": "center_understanding_relation_words",
    "cause_effect__conclusion_focus__main_idea": "center_understanding_relation_words",
    "necessary_condition_countermeasure__main_idea": "center_understanding_relation_words",
    "parallel_comprehensive_summary__main_idea": "center_understanding_relation_words",
    "theme_word_focus__main_idea": "center_understanding_relation_words",
    "sentence_order__head_tail_logic__abstract": "sentence_order_first_sentence",
    "sentence_order__head_tail_lock__abstract": "sentence_order_fixed_bundle",
    "sentence_order__deterministic_binding__abstract": "sentence_order_fixed_bundle",
    "sentence_order__discourse_logic__abstract": "sentence_order_sequence",
    "sentence_order__timeline_action_sequence__abstract": "sentence_order_sequence",
    "sentence_fill__opening_summary__abstract": "sentence_fill_head_start",
    "sentence_fill__opening_topic_intro__abstract": "sentence_fill_head_start",
    "sentence_fill__middle_carry_previous__abstract": "sentence_fill_middle",
    "sentence_fill__middle_lead_next__abstract": "sentence_fill_middle",
    "sentence_fill__middle_bridge_both_sides__abstract": "sentence_fill_middle",
    "sentence_fill__ending_summary__abstract": "sentence_fill_tail_end",
    "sentence_fill__ending_countermeasure__abstract": "sentence_fill_tail_end",
}

_MATERIAL_STRUCTURE_BY_MAIN_IDEA_CARD_ID = {
    "turning_relation_focus__main_idea": "转折归旨",
    "necessary_condition_countermeasure__main_idea": "问题-对策",
    "parallel_comprehensive_summary__main_idea": "并列推进",
    "cause_effect__conclusion_focus__main_idea": "背景-核心结论",
    "theme_word_focus__main_idea": "总分归纳",
}


def _question_focus_for_detected_target(*, question_type: str, business_subtype: str | None) -> str:
    if question_type == "sentence_order":
        return "sentence_order"
    if question_type == "sentence_fill":
        return "sentence_fill"
    if question_type == "main_idea" and business_subtype in {"center_understanding", "title_selection"}:
        return "center_understanding"
    return "center_understanding"


def _business_subtype_for_detected_target(
    *,
    question_type: str,
    business_subtype: str | None,
    business_card_ids: list[str],
) -> str | None:
    for card_id in business_card_ids:
        mapped = _UI_CHILD_FAMILY_BY_BUSINESS_CARD_ID.get(str(card_id or "").strip())
        if mapped:
            return mapped
    if question_type == "sentence_order":
        return None
    if question_type == "sentence_fill":
        return None
    if question_type == "main_idea" and business_subtype in {"center_understanding", "title_selection"}:
        return "center_understanding_relation_words"
    return business_subtype


def _primary_special_type_from_business_cards(business_card_ids: list[str]) -> str | None:
    for card_id in business_card_ids:
        mapped = _UI_SPECIAL_TYPE_BY_BUSINESS_CARD_ID.get(str(card_id or "").strip())
        if mapped:
            return mapped
    return None


@router.post("/source-question/parse", response_model=SourceQuestionParseResponse)
def parse_source_question(
    request: SourceQuestionParseRequest,
    runtime_registry: RuntimeConfigRegistry = Depends(get_runtime_registry),
) -> SourceQuestionParseResponse:
    service = SourceQuestionParserService(runtime_registry.get())
    parsed = service.parse(request.raw_text)
    return SourceQuestionParseResponse(source_question=parsed)


@router.post("/source-question/detect", response_model=SourceQuestionDetectResponse)
def detect_source_question_fields(
    request: SourceQuestionDetectRequest,
    runtime_registry: RuntimeConfigRegistry = Depends(get_runtime_registry),
) -> SourceQuestionDetectResponse:
    analyzer = SourceQuestionAnalyzer(runtime_registry.get())
    inferred_target = analyzer.infer_request_target(request.source_question) or {}
    question_type = str(inferred_target.get("question_type") or "main_idea").strip() or "main_idea"
    business_subtype = inferred_target.get("business_subtype")
    if question_type == "main_idea" and business_subtype not in {"center_understanding", "title_selection"}:
        business_subtype = "center_understanding"
    analysis = analyzer.analyze(
        source_question=request.source_question,
        question_type=question_type,
        business_subtype=business_subtype,
    )
    business_card_ids = analysis.get("business_card_ids") or []
    question_focus = _question_focus_for_detected_target(
        question_type=question_type,
        business_subtype=business_subtype,
    )
    resolved_business_subtype = _business_subtype_for_detected_target(
        question_type=question_type,
        business_subtype=business_subtype,
        business_card_ids=business_card_ids,
    )
    special_question_type = _primary_special_type_from_business_cards(business_card_ids)
    material_structure = None
    if question_type == "main_idea" and business_subtype == "center_understanding":
        for card_id in business_card_ids:
            material_structure = _MATERIAL_STRUCTURE_BY_MAIN_IDEA_CARD_ID.get(str(card_id or "").strip())
            if material_structure:
                break

    return SourceQuestionDetectResponse(
        question_focus=question_focus,
        business_subtype=resolved_business_subtype,
        special_question_type=special_question_type,
        material_structure=material_structure,
        topic=analysis.get("topic"),
        business_card_ids=business_card_ids,
        query_terms=analysis.get("query_terms") or [],
        leaf_id_primary=analysis.get("leaf_id_primary"),
        leaf_id_candidates=analysis.get("leaf_id_candidates") or [],
        analysis_confidence=analysis.get("analysis_confidence"),
    )


@router.post("/generate", response_model=QuestionGenerationBatchResponse)
def generate_questions(
    request: QuestionGenerateRequest,
    http_request: Request,
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
    with acquire_generation_slot() as gate_state:
        http_request.state.generation_gate = gate_state
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


@router.get("/source-question/assets", response_model=SourceQuestionAssetListResponse)
def list_source_question_assets(
    limit: int = 100,
    source_type: str | None = None,
    question_card_id: str | None = None,
    repository: QuestionRepository = Depends(get_question_repository),
) -> SourceQuestionAssetListResponse:
    items = repository.list_source_question_assets(
        limit=limit,
        source_type=source_type,
        question_card_id=question_card_id,
    )
    return SourceQuestionAssetListResponse(count=len(items), items=items)


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
    http_request: Request,
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
    with acquire_generation_slot() as gate_state:
        http_request.state.generation_gate = gate_state
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


@router.post("/{item_id}/download", response_model=QuestionDownloadResponse)
def record_question_download(
    item_id: str,
    request: QuestionDownloadRequest,
    repository: QuestionRepository = Depends(get_question_repository),
) -> QuestionDownloadResponse:
    item = repository.get_item(item_id)
    if item is None:
        raise DomainError("Question item not found.", status_code=404, details={"item_id": item_id})

    latest_action = str(item.get("latest_action") or "")
    revision_count = int(item.get("revision_count", 0) or 0)
    statuses = item.get("statuses") or {}
    download_variant = "accepted_after_edit" if revision_count > 0 or latest_action == "manual_edit" else "accepted_direct"
    payload = {
        "channel": request.channel,
        "export_format": request.export_format,
        "metadata": request.metadata,
        "download_variant": download_variant,
        "question_card_id": (item.get("request_snapshot") or {}).get("question_card_id"),
        "question_type": item.get("question_type"),
        "business_subtype": item.get("business_subtype"),
        "pattern_id": item.get("pattern_id"),
        "difficulty_target": item.get("difficulty_target"),
        "review_status": statuses.get("review_status"),
        "generation_status": statuses.get("generation_status"),
        "current_status": item.get("current_status"),
        "latest_action": latest_action,
        "current_version_no": item.get("current_version_no"),
        "revision_count": revision_count,
        "material_id": (item.get("material_selection") or {}).get("material_id"),
    }
    event_id = str(uuid4())
    repository.save_usage_event(
        event_id,
        item_id,
        "download",
        payload,
        operator=request.operator or "system",
    )
    event = repository.list_usage_events(item_id=item_id, limit=1, event_type="download")[0]
    return QuestionDownloadResponse(
        event=QuestionUsageEventLog.model_validate(event),
        item=QuestionGenerationItem.model_validate(item),
    )


@router.get("/{item_id}/usage-events", response_model=list[QuestionUsageEventLog])
def list_question_usage_events(
    item_id: str,
    limit: int = 100,
    event_type: str | None = None,
    repository: QuestionRepository = Depends(get_question_repository),
) -> list[QuestionUsageEventLog]:
    item = repository.get_item(item_id)
    if item is None:
        raise DomainError("Question item not found.", status_code=404, details={"item_id": item_id})
    events = repository.list_usage_events(item_id=item_id, limit=limit, event_type=event_type)
    return [QuestionUsageEventLog.model_validate(event) for event in events]


@router.post("/{item_id}/review-actions", response_model=QuestionReviewActionResponse)
def review_question_item(
    item_id: str,
    request: QuestionReviewActionRequest,
    http_request: Request,
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
    gate_context = acquire_generation_slot() if request.action in HEAVY_REVIEW_ACTIONS else nullcontext(None)
    with gate_context as gate_state:
        if gate_state is not None:
            http_request.state.generation_gate = gate_state
        return QuestionReviewActionResponse.model_validate(service.apply_action(item_id, request))
