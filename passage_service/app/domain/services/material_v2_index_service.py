from __future__ import annotations

from collections import Counter

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
                return mechanical_families
            return sorted(non_main_mechanical)
        return sorted(non_main_mechanical.union(llm_runtime_families))

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
