from app.core.config import get_config_bundle
from app.core.enums import ArticleStatus, CandidateSpanStatus, MaterialStatus, ReleaseChannel
from app.domain.models.plugin_contracts import RunContext
from app.domain.services._common import ServiceBase
from app.domain.services.pool_service import PoolService
from app.domain.services.review_service import ReviewService
from app.domain.services.sync_service import SyncService
from app.infra.plugins.loader import load_plugins
from app.infra.plugins.registries import tagger_registry
from app.infra.tagging.fit.mapper import FitMapper
from app.rules.family_config import get_family_names, get_thresholds
from app.schemas.span import SpanRecord, SpanVersionSet
from app.services.family_router import FamilyRouter
from app.services.document_genre_classifier import DocumentGenreClassifier
from app.services.family_taggers.continuation_family_tagger import ContinuationFamilyTagger
from app.services.family_taggers.fill_family_tagger import FillFamilyTagger
from app.services.family_taggers.ordering_family_tagger import OrderingFamilyTagger
from app.services.family_taggers.summarization_family_tagger import SummarizationFamilyTagger
from app.services.family_taggers.title_family_tagger import TitleFamilyTagger
from app.services.material_integrity_gate import MaterialIntegrityGate
from app.services.material_governance import MaterialCandidate, MaterialGovernanceService
from app.services.material_merge_service import MaterialMergeService
from app.services.pool_writer import PoolWriter
from app.services.universal_tagger import UniversalTagger


class TagService(ServiceBase):
    def tag_article(self, article_id: str) -> dict:
        article = self.article_repo.get(article_id)
        if article is None:
            return {"article_id": article_id, "count": 0, "status": "not_found"}

        if not tagger_registry.list():
            load_plugins()

        config_bundle = get_config_bundle()
        tree_config = config_bundle.knowledge_tree
        genre_classifier = DocumentGenreClassifier(config_bundle.document_genres)
        fit_mapper = FitMapper(config_bundle.fit_mapping)
        threshold_low = get_thresholds()["threshold_low"]
        run_context = RunContext(
            segmentation_version=config_bundle.segmentation.get("version", "seg.v1"),
            tag_version=tree_config.get("version", "tag.v1"),
            fit_version=config_bundle.fit_mapping.get("version", "fit.v1"),
            knowledge_tree_version=tree_config.get("version", "kt.v1"),
        )

        family_names = get_family_names()
        family_taggers = self._build_family_taggers(family_names)
        created_material_ids: list[str] = []
        rejected_candidates: list[dict] = []
        governed_candidates: list[MaterialCandidate] = []

        universal_tagger = UniversalTagger()
        family_router = FamilyRouter()
        pool_writer = PoolWriter()
        governance = MaterialGovernanceService()
        merge_service = MaterialMergeService()
        integrity_gate = MaterialIntegrityGate()

        source_info = governance.build_source_info(article)
        source_payload = source_info.model_dump()
        source_tail = governance.build_source_tail(source_info)
        self.material_repo.demote_existing_for_article(article_id)

        candidates = self.candidate_repo.list_new(article_id)
        spans: list[SpanRecord] = []
        for candidate in candidates:
            spans.append(
                SpanRecord(
                    span_id=candidate.id,
                    article_id=article.id,
                    text=candidate.text,
                    paragraph_count=max(1, candidate.end_paragraph - candidate.start_paragraph + 1),
                    sentence_count=max(1, (candidate.end_sentence or 0) - (candidate.start_sentence or 0) + 1),
                    source_domain=article.domain,
                    source=source_info,
                    status="new",
                    version=SpanVersionSet(
                        segment_version=run_context.segmentation_version or "seg.v1",
                        universal_tag_version=run_context.tag_version or "universal.v1",
                        route_version=config_bundle.family_routing.get("version", "route.v1"),
                        family_tag_version="family.v1",
                    ),
                )
            )

        passed_records: list[tuple[object, SpanRecord, dict]] = []
        for candidate, span in zip(candidates, spans, strict=False):
            integrity = integrity_gate.evaluate(
                text=candidate.text,
                paragraph_count=span.paragraph_count,
                sentence_count=span.sentence_count,
            )
            admission_status = integrity.get("admission_status", "reject")
            if admission_status != "allow":
                reject_reason = f"integrity_gate:{admission_status}:{integrity.get('admission_reason') or integrity.get('reason', 'unknown')}"
                if admission_status == "gray_hold":
                    self._hold_candidate(candidate.id, reject_reason, integrity=integrity)
                else:
                    self._reject_candidate(
                        candidate.id,
                        {},
                        reject_reason,
                        integrity=integrity,
                        stage="material_integrity_gate",
                    )
                rejected_candidates.append(
                    {
                        "candidate_span_id": candidate.id,
                        "admission_status": admission_status,
                        "reject_reason": reject_reason,
                        "integrity": integrity,
                    }
                )
                continue
            passed_records.append((candidate, span, integrity))

        universal_profiles = universal_tagger.tag_many([span for _, span, _ in passed_records])

        for (candidate, span, integrity), universal_profile in zip(passed_records, universal_profiles, strict=False):
            genre_result = genre_classifier.classify(
                title=article.title,
                text=candidate.text,
                source=article.source,
            )
            universal_profile.document_genre = genre_result["document_genre"]
            universal_profile.document_genre_candidates = genre_result["document_genre_candidates"]
            routed = family_router.route(span, universal_profile)
            family_scores = routed["family_scores"]
            capability_scores = routed["capability_scores"]
            parallel_families = routed["parallel_families"]
            structure_features = routed["structure_features"]
            primary_route = routed["primary_route"].model_dump()
            top_candidates = routed["top_candidates"]
            decision = routed["decision"]

            reject_reason = None
            if not family_scores.primary_family or max(family_scores.family_scores.values() or [0.0]) < threshold_low:
                reject_reason = "all_family_scores_below_threshold_low"
            else:
                reject_reason = governance.check_minimum_line(
                    text=candidate.text,
                    sentence_count=span.sentence_count,
                    universal_profile=universal_profile.model_dump(),
                )

            if reject_reason is not None:
                self._reject_candidate(candidate.id, family_scores.family_scores, reject_reason)
                rejected_candidates.append(
                    {
                        "candidate_span_id": candidate.id,
                        "reject_reason": reject_reason,
                        "family_scores": family_scores.family_scores,
                        "integrity": integrity,
                    }
                )
                continue

            subtype_candidates: list[dict] = []
            family_profiles: dict = {}
            for family_name in top_candidates:
                tagger = family_taggers.get(family_name)
                if tagger is None:
                    continue
                family_subtypes, family_profile = tagger.score(span, universal_profile)
                subtype_candidates.extend([item.model_dump() for item in family_subtypes])
                family_profiles[family_name] = family_profile

            subtype_candidates = sorted(subtype_candidates, key=lambda item: item["score"], reverse=True)[:8]
            candidate_labels, primary_label, secondary_candidates, decision_trace, coverage_result = governance.govern_labels(
                text=candidate.text,
                paragraph_count=span.paragraph_count,
                sentence_count=span.sentence_count,
                universal_profile=universal_profile.model_dump(),
                family_scores=family_scores.family_scores,
                parallel_families=parallel_families,
                structure_features=structure_features,
                family_profiles=family_profiles,
                subtype_candidates=subtype_candidates,
                primary_family=family_scores.primary_family,
            )
            if primary_label is None:
                reject_reason = "no_primary_label_selected"
                self._reject_candidate(candidate.id, family_scores.family_scores, reject_reason)
                rejected_candidates.append(
                    {
                        "candidate_span_id": candidate.id,
                        "reject_reason": reject_reason,
                        "family_scores": family_scores.family_scores,
                        "integrity": integrity,
                    }
                )
                continue

            selected_subtype = next((item for item in coverage_result["validated"] if item["subtype"] == primary_label), None)
            if selected_subtype is not None:
                primary_route["family"] = selected_subtype["family"]
                primary_route["subtype"] = selected_subtype["subtype"]
            else:
                primary_route["family"] = family_scores.primary_family
                primary_route["subtype"] = None

            fit_scores = fit_mapper.compute(universal_profile.model_dump())
            quality_flags = pool_writer.build_quality_flags(
                universal_profile.model_dump(),
                family_scores.family_scores,
                decision,
            )
            release_channel = decision.get("release_channel", ReleaseChannel.GRAY.value)
            if integrity.get("needs_llm_review") and integrity.get("llm_passed") is None:
                release_channel = ReleaseChannel.GRAY.value
                quality_flags.append("integrity_pending_review")
            if integrity.get("risk_level") == "medium":
                quality_flags.append("integrity_medium_risk")
            if primary_label in governance.wide_labels:
                quality_flags.append("wide_primary_label")

            governed_candidates.append(
                MaterialCandidate(
                    candidate_span_id=candidate.id,
                    article_id=article.id,
                    text=candidate.text,
                    span_type=candidate.span_type,
                    paragraph_count=span.paragraph_count,
                    sentence_count=span.sentence_count,
                    universal_profile=universal_profile.model_dump(),
                    family_scores=family_scores.family_scores,
                    capability_scores=capability_scores,
                    parallel_families=parallel_families,
                    structure_features=structure_features,
                    family_profiles=family_profiles,
                    subtype_candidates=coverage_result["validated"],
                    top_candidates=top_candidates,
                    primary_route=primary_route,
                    release_channel=release_channel,
                    decision_action=decision.get("action", "gray_top1"),
                    quality_flags=quality_flags,
                    fit_scores=fit_scores,
                    feature_profile=universal_profile.model_dump(),
                    quality_score=max(family_scores.family_scores.values() or [0.0]),
                    tag_version=span.version.universal_tag_version,
                    fit_version=span.version.route_version,
                    segmentation_version=span.version.segment_version,
                    source=source_payload,
                    source_tail=source_tail,
                    integrity=integrity,
                    candidate_labels=candidate_labels,
                    primary_label=primary_label,
                    secondary_candidates=secondary_candidates,
                    decision_trace=decision_trace,
                )
            )

        merged_candidates = merge_service.merge(governed_candidates)

        for material_candidate in merged_candidates:
            material_family_id = material_candidate.primary_route.get("material_family_id")
            material = PoolService(self.session).create_material(
                article_id=article.id,
                candidate_span_id=material_candidate.candidate_span_id,
                text=material_candidate.text,
                normalized_text_hash=material_candidate.normalized_text_hash,
                material_family_id=material_family_id,
                is_primary=True,
                span_type=material_candidate.span_type,
                length_bucket=material_candidate.universal_profile["text_shape"]["length_bucket"],
                paragraph_count=material_candidate.paragraph_count,
                sentence_count=material_candidate.sentence_count,
                status=MaterialStatus.PROMOTED.value if material_candidate.release_channel == ReleaseChannel.STABLE.value else MaterialStatus.GRAY.value,
                release_channel=material_candidate.release_channel,
                gray_ratio=config_bundle.release.get("default_gray_ratio", 0.1) if material_candidate.release_channel == "gray" else 0.0,
                gray_reason=material_candidate.decision_action,
                segmentation_version=material_candidate.segmentation_version,
                tag_version=material_candidate.tag_version,
                fit_version=material_candidate.fit_version,
                prompt_version=None,
                primary_family=material_candidate.primary_route.get("family"),
                primary_subtype=material_candidate.primary_route.get("subtype"),
                secondary_subtypes=[item.get("label") for item in material_candidate.secondary_candidates or []],
                universal_profile=material_candidate.universal_profile,
                family_scores=material_candidate.family_scores,
                capability_scores=material_candidate.capability_scores,
                parallel_families=material_candidate.parallel_families,
                structure_features=material_candidate.structure_features,
                family_profiles=material_candidate.family_profiles,
                subtype_candidates=material_candidate.subtype_candidates,
                secondary_candidates=material_candidate.secondary_candidates or [],
                candidate_labels=material_candidate.candidate_labels or [],
                primary_label=material_candidate.primary_label,
                decision_trace=material_candidate.decision_trace or {},
                primary_route=material_candidate.primary_route,
                reject_reason=None,
                variants=material_candidate.variants or [],
                source=material_candidate.source,
                source_tail=material_candidate.source_tail,
                integrity=material_candidate.integrity,
                quality_flags=material_candidate.quality_flags,
                knowledge_tags=[
                    *(material_candidate.candidate_labels or [])[:4],
                    *(
                        [f"genre:{material_candidate.feature_profile.get('document_genre')}"]
                        if material_candidate.feature_profile.get("document_genre")
                        else []
                    ),
                ],
                fit_scores=material_candidate.fit_scores,
                feature_profile={
                    **material_candidate.feature_profile,
                    "document_genre": material_candidate.feature_profile.get("document_genre"),
                    "document_genre_candidates": material_candidate.feature_profile.get("document_genre_candidates", []),
                },
                quality_score=material_candidate.quality_score,
            )

            review_status = "review_pending" if material_candidate.release_channel == "gray" else "auto_tagged"
            ReviewService(self.session).init_review(material.id, review_status)
            SyncService(self.session).upsert_material(material.id)
            self.candidate_repo.mark_status(material_candidate.candidate_span_id, CandidateSpanStatus.PROMOTED.value)
            for variant in material_candidate.variants or []:
                self.candidate_repo.mark_status(variant["candidate_span_id"], CandidateSpanStatus.PROMOTED.value)
            self.audit_repo.log(
                "material",
                material.id,
                "tag",
                {
                    "candidate_span_id": material_candidate.candidate_span_id,
                    "primary_route": material_candidate.primary_route,
                    "primary_label": material_candidate.primary_label,
                    "candidate_labels": material_candidate.candidate_labels,
                    "variants": len(material_candidate.variants or []),
                },
            )
            created_material_ids.append(material.id)

        self.article_repo.update_status(article_id, ArticleStatus.TAGGED.value)
        return {
            "article_id": article_id,
            "created_material_ids": created_material_ids,
            "rejected_candidates": rejected_candidates,
            "summary": {
                "created_count": len(created_material_ids),
                "rejected_count": len(rejected_candidates),
            },
            "count": len(created_material_ids),
        }

    def _build_family_taggers(self, family_names: list[str]) -> dict[str, object]:
        tagger_instances = [
            SummarizationFamilyTagger(),
            TitleFamilyTagger(),
            FillFamilyTagger(),
            OrderingFamilyTagger(),
            ContinuationFamilyTagger(),
        ]
        family_taggers: dict[str, object] = {}
        for family_name, tagger in zip(family_names, tagger_instances, strict=False):
            family_taggers[family_name] = tagger
        return family_taggers

    def _reject_candidate(
        self,
        candidate_span_id: str,
        family_scores: dict[str, float],
        reject_reason: str,
        *,
        integrity: dict | None = None,
        stage: str = "material_governance",
    ) -> None:
        self.candidate_repo.mark_status(candidate_span_id, CandidateSpanStatus.REJECTED.value)
        self.audit_repo.log(
            "candidate_span",
            candidate_span_id,
            "tag",
            {
                "result": "rejected",
                "stage": stage,
                "reject_reason": reject_reason,
                "family_scores": family_scores,
                "integrity": integrity or {},
            },
        )

    def _hold_candidate(
        self,
        candidate_span_id: str,
        hold_reason: str,
        *,
        integrity: dict | None = None,
    ) -> None:
        self.candidate_repo.mark_status(candidate_span_id, CandidateSpanStatus.GRAY_HOLD.value)
        self.audit_repo.log(
            "candidate_span",
            candidate_span_id,
            "tag",
            {
                "result": "gray_hold",
                "stage": "material_integrity_gate",
                "hold_reason": hold_reason,
                "integrity": integrity or {},
            },
        )
