from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace

from app.domain.services._common import ServiceBase
from app.rules.family_config import get_family_names
from app.services.main_card_family_landing_resolver import MainCardFamilyLandingResolver
from app.services.material_pipeline_v2 import MaterialPipelineV2


class MaterialV2IndexService(ServiceBase):
    def __init__(self, session) -> None:
        super().__init__(session)
        self.pipeline = MaterialPipelineV2()
        self.main_card_family_landing = MainCardFamilyLandingResolver(provider=self.pipeline.provider, llm_config=self.pipeline.llm_config)
        family_names = get_family_names()
        self.family_to_v2 = {}
        if len(family_names) >= 1:
            self.family_to_v2[family_names[0]] = "title_selection"
        if len(family_names) >= 2:
            self.family_to_v2[family_names[1]] = "title_selection"
        if len(family_names) >= 3:
            self.family_to_v2[family_names[2]] = "sentence_fill"
        if len(family_names) >= 4:
            self.family_to_v2[family_names[3]] = "sentence_order"
        if len(family_names) >= 5:
            self.family_to_v2[family_names[4]] = "continuation"

    def precompute(self, payload: dict) -> dict:
        materials = self.material_repo.list_for_v2_index(
            material_ids=payload.get("material_ids") or None,
            article_ids=payload.get("article_ids") or None,
            status=payload.get("status"),
            release_channel=payload.get("release_channel"),
            primary_only=payload.get("primary_only", True),
            limit=payload.get("limit"),
        )
        article_cache: dict[str, object] = {}
        updated = 0
        skipped = 0
        family_counter: Counter[str] = Counter()
        for material in materials:
            article = article_cache.get(material.article_id)
            if article is None:
                article = self.article_repo.get(material.article_id)
                article_cache[material.article_id] = article
            if article is None:
                skipped += 1
                continue
            families = self._resolve_v2_families(material=material, article=article)
            payload_by_family: dict[str, dict] = {}
            for family in families:
                cached_item = self._build_cached_item(material=material, article=article, family=family)
                if cached_item:
                    payload_by_family[family] = cached_item
                    family_counter[family] += 1
            if not payload_by_family:
                skipped += 1
                continue
            self.material_repo.update_metrics(
                material.id,
                v2_index_version=self.pipeline.INDEX_VERSION,
                v2_business_family_ids=sorted(payload_by_family.keys()),
                v2_index_payload=payload_by_family,
            )
            updated += 1
        return {
            "index_version": self.pipeline.INDEX_VERSION,
            "material_count": len(materials),
            "updated_count": updated,
            "skipped_count": skipped,
            "families": dict(family_counter),
        }

    def bootstrap_precompute(self, payload: dict) -> dict:
        materials = self.material_repo.list_for_v2_index(
            material_ids=payload.get("material_ids") or None,
            article_ids=payload.get("article_ids") or None,
            status=payload.get("status"),
            release_channel=payload.get("release_channel"),
            primary_only=payload.get("primary_only", True),
            limit=payload.get("limit"),
        )
        if payload.get("only_missing_index", True):
            materials = [
                material
                for material in materials
                if not getattr(material, "v2_index_version", None)
                or not isinstance(getattr(material, "v2_index_payload", None), dict)
                or not (getattr(material, "v2_index_payload", None) or {})
            ]
        article_cache: dict[str, object] = {}
        updated = 0
        skipped = 0
        direct_pass_count = 0
        repaired_pass_count = 0
        discarded_count = 0
        family_counter: Counter[str] = Counter()
        discard_reason_counter: Counter[str] = Counter()
        mechanical_only = bool(payload.get("use_mechanical_depth3", False))
        for material in materials:
            article = article_cache.get(material.article_id)
            if article is None:
                article = self.article_repo.get(material.article_id)
                article_cache[material.article_id] = article
            if article is None:
                skipped += 1
                continue
            families = self._resolve_bootstrap_v2_families(
                material=material,
                article=article,
                use_llm_family_landing=bool(payload.get("use_llm_family_landing", False)),
            )
            payload_by_family: dict[str, dict] = {}
            bootstrap_trace: dict[str, dict] = {}
            for family in families:
                outcome = self._bootstrap_build_family_payload(
                    material=material,
                    article=article,
                    family=family,
                    mechanical_only=mechanical_only,
                )
                bootstrap_trace[family] = dict(outcome.get("trace") or {})
                cached_item = outcome.get("item")
                if cached_item:
                    payload_by_family[family] = cached_item
                    family_counter[family] += 1
                    if outcome.get("outcome") == "depth2_repaired_pass":
                        repaired_pass_count += 1
                    else:
                        direct_pass_count += 1
                else:
                    discarded_count += 1
                    discard_reason = str((outcome.get("trace") or {}).get("failure_reason") or "bootstrap_family_rejected")
                    discard_reason_counter[discard_reason] += 1
            if not families:
                skipped += 1
                continue
            decision_trace = dict(getattr(material, "decision_trace", {}) or {})
            decision_trace["bootstrap_v2_index"] = {
                "index_version": self.pipeline.INDEX_VERSION,
                "ran_at": datetime.now().isoformat(timespec="seconds"),
                "families": bootstrap_trace,
            }
            if payload_by_family:
                quality_flags = self._merge_material_quality_flags(
                    material=material,
                    add_flags=["bootstrap_v2_indexed"] + (["bootstrap_v2_repaired"] if any(
                        trace.get("outcome") == "depth2_repaired_pass" for trace in bootstrap_trace.values()
                    ) else []),
                    remove_flags=["bootstrap_v2_discarded"],
                )
                self.material_repo.update_metrics(
                    material.id,
                    v2_index_version=self.pipeline.INDEX_VERSION,
                    v2_business_family_ids=sorted(payload_by_family.keys()),
                    v2_index_payload=payload_by_family,
                    decision_trace=decision_trace,
                    reject_reason=None,
                    quality_flags=quality_flags,
                )
                updated += 1
            else:
                quality_flags = self._merge_material_quality_flags(
                    material=material,
                    add_flags=["bootstrap_v2_discarded"],
                )
                self.material_repo.update_metrics(
                    material.id,
                    v2_index_version=self.pipeline.INDEX_VERSION,
                    v2_business_family_ids=[],
                    v2_index_payload={},
                    decision_trace=decision_trace,
                    reject_reason=self._bootstrap_material_reject_reason(bootstrap_trace),
                    quality_flags=quality_flags,
                )
                updated += 1
        return {
            "index_version": self.pipeline.INDEX_VERSION,
            "material_count": len(materials),
            "updated_count": updated,
            "skipped_count": skipped,
            "direct_pass_count": direct_pass_count,
            "repaired_pass_count": repaired_pass_count,
            "discarded_family_count": discarded_count,
            "families": dict(family_counter),
            "discard_reasons": dict(discard_reason_counter),
        }

    def _build_cached_item(self, *, material, article, family: str) -> dict | None:
        return self.pipeline.build_cached_item_from_material(
            material=material,
            article=article,
            business_family_id=family,
            enable_fill_formalization_bridge=(family == "sentence_fill"),
            enable_sentence_order_weak_formal_bridge=(family == "sentence_order"),
            enable_sentence_order_weak_formal_gate=(family == "sentence_order"),
            enable_sentence_order_weak_formal_closing_gate=(family == "sentence_order"),
            enable_sentence_order_strong_formal_demote=(family == "sentence_order"),
        )

    def _build_cached_item_diagnostics(self, *, material, article, family: str, mechanical_only: bool = False) -> dict:
        return dict(
            self.pipeline.build_cached_item_from_material(
                material=material,
                article=article,
                business_family_id=family,
                enable_fill_formalization_bridge=(family == "sentence_fill"),
                enable_sentence_order_weak_formal_bridge=(family == "sentence_order"),
                enable_sentence_order_weak_formal_gate=(family == "sentence_order"),
                enable_sentence_order_weak_formal_closing_gate=(family == "sentence_order"),
                enable_sentence_order_strong_formal_demote=(family == "sentence_order"),
                skip_llm_signal_resolution=mechanical_only,
                skip_llm_adjudication=mechanical_only,
                return_diagnostics=True,
            )
            or {}
        )

    def _bootstrap_build_family_payload(self, *, material, article, family: str, mechanical_only: bool = False) -> dict:
        diagnostics = self._build_cached_item_diagnostics(material=material, article=article, family=family, mechanical_only=mechanical_only)
        accepted_item = diagnostics.get("accepted_item")
        if accepted_item:
            return {
                "item": accepted_item,
                "outcome": "depth3_pass",
                "trace": {
                    "outcome": "depth3_pass",
                    "repair_attempted": False,
                    "failure_reason": "",
                },
            }
        original_item = diagnostics.get("item")
        failure_reason = str(diagnostics.get("failure_reason") or "runtime_material_gate_failed")
        if not original_item:
            return {
                "item": None,
                "outcome": "discarded",
                "trace": {
                    "outcome": "discarded",
                    "repair_attempted": False,
                    "failure_reason": failure_reason,
                },
            }
        question_card_id = str(((original_item.get("question_ready_context") or {}).get("question_card_id")) or "")
        repair_entry = self.pipeline.near_miss_repair_service.evaluate_entry(
            item=original_item,
            business_family_id=family,
            failure_reason=failure_reason,
            question_card_id=question_card_id,
        )
        if not repair_entry.get("repair_candidate"):
            return {
                "item": None,
                "outcome": "discarded",
                "trace": {
                    "outcome": "discarded",
                    "repair_attempted": False,
                    "failure_reason": str(repair_entry.get("entry_reason") or failure_reason),
                },
            }
        repair_result = self.pipeline.near_miss_repair_service.repair(
            item=original_item,
            business_family_id=family,
            question_card_id=question_card_id,
            target_business_card=str(repair_entry.get("target_business_card") or ""),
            failure_reason=failure_reason,
            dirty_states=list(repair_entry.get("dirty_states") or []),
        )
        if repair_result is None:
            self.pipeline.near_miss_repair_service.mark_failure(
                item=original_item,
                business_family_id=family,
                target_business_card=str(repair_entry.get("target_business_card") or ""),
            )
            return {
                "item": None,
                "outcome": "discarded",
                "trace": {
                    "outcome": "discarded",
                    "repair_attempted": True,
                    "failure_reason": "depth2_repair_returned_none",
                    "repair_entry_reason": repair_entry.get("entry_reason"),
                    "repair_dirty_states": list(repair_entry.get("dirty_states") or []),
                },
            }
        repaired_material = self._material_clone_with_text(
            material=material,
            text=str(repair_result.get("rewritten_text") or "").strip(),
            candidate_id_suffix="repair",
            extra_quality_flags=["depth2_repair"],
        )
        repaired_diagnostics = self._build_cached_item_diagnostics(
            material=repaired_material,
            article=article,
            family=family,
            mechanical_only=mechanical_only,
        )
        repaired_item = repaired_diagnostics.get("accepted_item")
        if not repaired_item or not self.pipeline._repair_outcome_improved(before_item=original_item, after_item=repaired_item):
            self.pipeline.near_miss_repair_service.mark_failure(
                item=original_item,
                business_family_id=family,
                target_business_card=str(repair_entry.get("target_business_card") or ""),
            )
            return {
                "item": None,
                "outcome": "discarded",
                "trace": {
                    "outcome": "discarded",
                    "repair_attempted": True,
                    "failure_reason": str(repaired_diagnostics.get("failure_reason") or "repair_rejudge_failed"),
                    "repair_entry_reason": repair_entry.get("entry_reason"),
                    "repair_dirty_states": list(repair_entry.get("dirty_states") or []),
                    "repair_mode": repair_result.get("rewrite_mode"),
                },
            }
        repaired_item = self.pipeline._attach_runtime_repair_trace(
            item=repaired_item,
            original_item=original_item,
            failure_reason=failure_reason,
            repaired_gate_reason=str(repaired_diagnostics.get("failure_reason") or ""),
            repair_entry=repair_entry,
            repair_result=repair_result,
        )
        return {
            "item": repaired_item,
            "outcome": "depth2_repaired_pass",
            "trace": {
                "outcome": "depth2_repaired_pass",
                "repair_attempted": True,
                "repair_entry_reason": repair_entry.get("entry_reason"),
                "repair_dirty_states": list(repair_entry.get("dirty_states") or []),
                "repair_mode": repair_result.get("rewrite_mode"),
                "repair_target_business_card": repair_entry.get("target_business_card"),
            },
        }

    @staticmethod
    def _material_clone_with_text(*, material, text: str, candidate_id_suffix: str, extra_quality_flags: list[str] | None = None):
        quality_flags = list(dict.fromkeys(list(getattr(material, "quality_flags", []) or []) + list(extra_quality_flags or [])))
        return SimpleNamespace(
            id=f"{getattr(material, 'id', '')}:{candidate_id_suffix}",
            article_id=getattr(material, "article_id", None),
            candidate_span_id=getattr(material, "candidate_span_id", None),
            text=text,
            span_type=getattr(material, "span_type", "material_span"),
            paragraph_count=getattr(material, "paragraph_count", 1),
            sentence_count=getattr(material, "sentence_count", 1),
            start_paragraph=getattr(material, "start_paragraph", 0),
            end_paragraph=getattr(material, "end_paragraph", 0),
            start_sentence=getattr(material, "start_sentence", 0),
            end_sentence=getattr(material, "end_sentence", 0),
            quality_flags=quality_flags,
        )

    @staticmethod
    def _bootstrap_material_reject_reason(bootstrap_trace: dict[str, dict]) -> str:
        reasons = []
        for family, trace in sorted(bootstrap_trace.items()):
            reason = str((trace or {}).get("failure_reason") or "bootstrap_family_rejected")
            reasons.append(f"{family}:{reason}")
        return "bootstrap_depth2_discarded|" + ";".join(reasons)

    @staticmethod
    def _merge_material_quality_flags(*, material, add_flags: list[str], remove_flags: list[str] | None = None) -> list[str]:
        flags = list(getattr(material, "quality_flags", []) or [])
        remove_set = set(remove_flags or [])
        merged = [flag for flag in flags if flag not in remove_set]
        for flag in add_flags:
            if flag and flag not in merged:
                merged.append(flag)
        return merged

    def _resolve_v2_families(self, *, material, article) -> list[str]:
        mechanical_families = self._resolve_mechanical_v2_families(material)
        resolved = self.main_card_family_landing.resolve(
            material=material,
            article=article,
            mechanical_v2_families=mechanical_families,
        )
        if not resolved:
            return mechanical_families
        consensus = dict(resolved.get("consensus") or {})
        llm_runtime_families = set(resolved.get("runtime_families") or [])
        non_main_mechanical = {
            family
            for family in mechanical_families
            if family not in {"title_selection", "sentence_fill", "sentence_order"}
        }
        if not llm_runtime_families:
            if str(consensus.get("status") or "") in {"error", "insufficient_votes"}:
                families = set(mechanical_families)
            else:
                families = set(non_main_mechanical)
        else:
            families = set(non_main_mechanical.union(llm_runtime_families))

        # Keep center_understanding cache payload in sync with main_idea/title_selection
        # so question cards that route material search to center_understanding can still
        # leverage v2 cached materials without falling back to article-level rebuild.
        if "title_selection" in families:
            families.add("center_understanding")
        return sorted(families)

    def _resolve_bootstrap_v2_families(self, *, material, article, use_llm_family_landing: bool) -> list[str]:
        if use_llm_family_landing:
            return self._resolve_v2_families(material=material, article=article)
        families = set(self._resolve_mechanical_v2_families(material))
        if "title_selection" in families:
            families.add("center_understanding")
        return sorted(families)

    def _resolve_mechanical_v2_families(self, material) -> list[str]:
        families: set[str] = set()
        primary_family = getattr(material, "primary_family", None)
        if primary_family and primary_family in self.family_to_v2:
            families.add(self.family_to_v2[primary_family])
        for item in getattr(material, "parallel_families", []) or []:
            family = item.get("family") if isinstance(item, dict) else None
            if family and family in self.family_to_v2:
                families.add(self.family_to_v2[family])
        return sorted(families)
