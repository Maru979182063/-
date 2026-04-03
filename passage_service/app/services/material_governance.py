from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.core.config import get_config_bundle
from app.rules.family_config import get_family_names
from app.schemas.span import LabelDecisionTrace, SourceInfo


@dataclass
class MaterialCandidate:
    candidate_span_id: str
    article_id: str
    text: str
    span_type: str
    paragraph_count: int
    sentence_count: int
    universal_profile: dict[str, Any]
    family_scores: dict[str, float]
    capability_scores: dict[str, float]
    parallel_families: list[dict[str, Any]]
    structure_features: dict[str, Any]
    family_profiles: dict[str, Any]
    subtype_candidates: list[dict[str, Any]]
    top_candidates: list[str]
    primary_route: dict[str, Any]
    release_channel: str
    decision_action: str
    quality_flags: list[str]
    fit_scores: dict[str, float]
    feature_profile: dict[str, Any]
    quality_score: float
    tag_version: str
    fit_version: str
    segmentation_version: str
    source: dict[str, Any]
    source_tail: str
    integrity: dict[str, Any]
    normalized_text_hash: str = ""
    candidate_labels: list[str] | None = None
    primary_label: str | None = None
    secondary_candidates: list[dict[str, Any]] | None = None
    decision_trace: dict[str, Any] | None = None
    variants: list[dict[str, Any]] | None = None


class MaterialGovernanceService:
    def __init__(self) -> None:
        self.config = get_config_bundle().material_governance
        minimums = self.config.get("minimums", {})
        labels = self.config.get("labels", {})
        self.min_sentences = int(minimums.get("min_sentences", 4))
        self.min_chars = int(minimums.get("min_chars", 150))
        self.min_independence = float(minimums.get("min_independence_score", 0.48))
        self.min_info_density = float(minimums.get("min_information_density", 0.52))
        self.top_k = int(labels.get("top_k", 4))
        self.secondary_k = int(labels.get("secondary_k", 3))
        self.wide_labels = set(labels.get("wide_labels", []))
        self.family_names = get_family_names()
        self.summarization_family = self.family_names[0] if len(self.family_names) >= 1 else ""
        self.title_family = self.family_names[1] if len(self.family_names) >= 2 else ""
        self.fill_family = self.family_names[2] if len(self.family_names) >= 3 else ""
        self.ordering_family = self.family_names[3] if len(self.family_names) >= 4 else ""
        self.continuation_family = self.family_names[4] if len(self.family_names) >= 5 else ""

    def build_source_info(self, article: Any) -> SourceInfo:
        publish_time = getattr(article, "published_at", None) or getattr(article, "created_at", None)
        publish_time_text = publish_time.isoformat() if publish_time is not None else ""
        crawl_batch = article.created_at.strftime("%Y%m%d") if getattr(article, "created_at", None) is not None else ""
        return SourceInfo(
            source_id=str(getattr(article, "source", "") or ""),
            source_name=str(getattr(article, "source", "") or ""),
            source_url=str(getattr(article, "source_url", "") or ""),
            article_title=str(getattr(article, "title", "") or ""),
            publish_time=publish_time_text,
            channel=str(getattr(article, "domain", "") or ""),
            crawl_batch=crawl_batch,
        )

    def build_source_tail(self, source: SourceInfo) -> str:
        name = source.source_name or "\u672a\u77e5\u6765\u6e90"
        title = source.article_title or "\u672a\u547d\u540d\u6587\u7ae0"
        publish_time = source.publish_time or "\u672a\u77e5\u65f6\u95f4"
        return f"\u3010\u6765\u6e90\uff1a{name}\u300a{title}\u300b\uff0c{publish_time}\u3011"

    def check_minimum_line(self, *, text: str, sentence_count: int, universal_profile: dict[str, Any]) -> str | None:
        text_length = len(text.strip())
        if sentence_count < self.min_sentences:
            return "below_min_sentences"
        if text_length < self.min_chars:
            return "below_min_chars"
        if universal_profile.get("independence_score", 0.0) < self.min_independence:
            return "low_independence_score"
        if self._information_density(universal_profile) < self.min_info_density and text_length < max(self.min_chars * 2, 320):
            return "low_information_density"
        if self._looks_like_bridge(universal_profile):
            return "bridge_like_fragment"
        if self._looks_like_half_turn(text, universal_profile):
            return "half_transition_fragment"
        if self._looks_like_isolated_example(universal_profile):
            return "isolated_example_fragment"
        return None

    def govern_labels(
        self,
        *,
        text: str,
        paragraph_count: int,
        sentence_count: int,
        universal_profile: dict[str, Any],
        family_scores: dict[str, float],
        parallel_families: list[dict[str, Any]],
        structure_features: dict[str, Any],
        family_profiles: dict[str, Any],
        subtype_candidates: list[dict[str, Any]],
        primary_family: str | None,
    ) -> tuple[list[str], str | None, list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        coverage_failures: list[dict[str, Any]] = []
        validated_subtypes: list[dict[str, Any]] = []
        for item in sorted(subtype_candidates, key=lambda entry: entry.get("score", 0.0), reverse=True):
            is_valid, coverage_reason = self._validate_subtype_coverage(
                text=text,
                paragraph_count=paragraph_count,
                sentence_count=sentence_count,
                universal_profile=universal_profile,
                structure_features=structure_features,
                family_profiles=family_profiles,
                subtype_candidate=item,
            )
            enriched = dict(item)
            enriched["coverage_valid"] = is_valid
            enriched["coverage_reason"] = coverage_reason
            if is_valid:
                validated_subtypes.append(enriched)
            else:
                coverage_failures.append(
                    {
                        "label": item.get("subtype"),
                        "family": item.get("family"),
                        "reason": coverage_reason,
                    }
                )

        label_entries: list[dict[str, Any]] = []
        seen_labels: set[str] = set()

        for item in validated_subtypes:
            family = item.get("family") or primary_family or ""
            subtype = item.get("subtype") or family
            label = subtype
            is_wide = label in self.wide_labels or label == family
            if label in seen_labels:
                continue
            seen_labels.add(label)
            label_entries.append(
                {
                    "label": label,
                    "family": family,
                    "score": float(item.get("score", 0.0)),
                    "adjusted_score": round(float(item.get("score", 0.0)) - (0.15 if is_wide else 0.0), 4),
                    "is_wide": is_wide,
                    "coverage_reason": item.get("coverage_reason"),
                }
            )

        for family, score in sorted(family_scores.items(), key=lambda entry: entry[1], reverse=True):
            if family in seen_labels:
                continue
            seen_labels.add(family)
            label_entries.append(
                {
                    "label": family,
                    "family": family,
                    "score": float(score),
                    "adjusted_score": round(float(score) - (0.15 if family in self.wide_labels else 0.0), 4),
                    "is_wide": True,
                    "coverage_reason": "family_level_fallback",
                }
            )

        if structure_features.get("strong_ordering_signal"):
            for family_entry in parallel_families:
                family = family_entry["family"]
                if family in seen_labels:
                    continue
                label_entries.append(
                    {
                        "label": family,
                        "family": family,
                        "score": float(family_entry["score"]),
                        "adjusted_score": round(float(family_entry["score"]) + 0.12, 4),
                        "is_wide": False,
                        "coverage_reason": "strong_structure_family_preserved",
                    }
                )
                seen_labels.add(family)

        label_entries.sort(key=lambda item: item["adjusted_score"], reverse=True)

        selected_entries: list[dict[str, Any]] = []
        selected_families: set[str] = set()
        for entry in label_entries:
            if len(selected_entries) >= self.top_k:
                break
            if len(selected_entries) < 2 and entry["family"] not in selected_families:
                selected_entries.append(entry)
                selected_families.add(entry["family"])
                continue
            selected_entries.append(entry)
            selected_families.add(entry["family"])

        fine_entry = next((item for item in label_entries if not item["is_wide"]), None)
        if fine_entry is not None and selected_entries and all(item["is_wide"] for item in selected_entries):
            selected_entries[-1] = fine_entry

        if structure_features.get("strong_ordering_signal"):
            structural_entry = next(
                (
                    item
                    for item in label_entries
                    if item["family"] in {self.ordering_family, self.fill_family, self.continuation_family}
                ),
                None,
            )
            if structural_entry is not None and structural_entry not in selected_entries:
                if len(selected_entries) >= self.top_k:
                    selected_entries[-1] = structural_entry
                else:
                    selected_entries.append(structural_entry)

        for entry in label_entries:
            if len(selected_entries) >= self.top_k:
                break
            if entry not in selected_entries:
                selected_entries.append(entry)

        selected_entries = selected_entries[: self.top_k]
        primary_entry = next((item for item in selected_entries if not item["is_wide"]), None)
        if primary_entry is None and selected_entries:
            primary_entry = selected_entries[0]

        secondary_candidates = [
            {
                "label": item["label"],
                "family": item["family"],
                "score": item["score"],
                "is_wide": item["is_wide"],
            }
            for item in selected_entries
            if item is not primary_entry
        ][: self.secondary_k]

        trace = LabelDecisionTrace(
            selected=primary_entry["label"] if primary_entry else None,
            rejected=[item["label"] for item in label_entries if item not in selected_entries],
            reason=self._build_decision_reason(primary_entry, structure_features, universal_profile),
        )
        trace_payload = trace.model_dump()
        selected_family = primary_entry["family"] if primary_entry else None
        trace_payload["selected_family"] = selected_family
        trace_payload["parallel_kept"] = [
            {
                "family": item["family"],
                "reason": self._build_family_reason(item["family"], structure_features, universal_profile),
            }
            for item in parallel_families[:4]
            if item["family"] != selected_family
        ]
        if coverage_failures:
            trace_payload["coverage_downgraded"] = coverage_failures
        return (
            [item["label"] for item in selected_entries],
            primary_entry["label"] if primary_entry else None,
            secondary_candidates,
            trace_payload,
            {"validated": validated_subtypes, "rejected": coverage_failures},
        )

    def _information_density(self, universal_profile: dict[str, Any]) -> float:
        keys = (
            "single_center_strength",
            "summary_strength",
            "explanation_strength",
            "problem_signal_strength",
            "method_signal_strength",
            "value_judgement_strength",
        )
        values = [float(universal_profile.get(key, 0.0)) for key in keys]
        return sum(values) / len(values)

    def _looks_like_bridge(self, universal_profile: dict[str, Any]) -> bool:
        roles = universal_profile.get("position_roles", [])
        role_signal = 1.0 if roles else 0.0
        return (
            universal_profile.get("independence_score", 0.0) < 0.58
            and max(
                universal_profile.get("transition_strength", 0.0),
                universal_profile.get("explanation_strength", 0.0),
            )
            > 0.65
            and role_signal > 0.0
        )

    def _looks_like_half_turn(self, text: str, universal_profile: dict[str, Any]) -> bool:
        starters = (
            "\u4f46",
            "\u4f46\u662f",
            "\u7136\u800c",
            "\u4e0d\u8fc7",
            "\u4e0e\u6b64\u540c\u65f6",
            "\u56e0\u6b64",
            "\u6240\u4ee5",
        )
        stripped = text.strip()
        return universal_profile.get("transition_strength", 0.0) > 0.7 and stripped.startswith(starters) and len(stripped) < 220

    def _looks_like_isolated_example(self, universal_profile: dict[str, Any]) -> bool:
        return (
            universal_profile.get("example_to_theme_strength", 0.0) > 0.7
            and universal_profile.get("single_center_strength", 0.0) < 0.55
            and universal_profile.get("summary_strength", 0.0) < 0.45
        )

    def _validate_subtype_coverage(
        self,
        *,
        text: str,
        paragraph_count: int,
        sentence_count: int,
        universal_profile: dict[str, Any],
        structure_features: dict[str, Any],
        family_profiles: dict[str, Any],
        subtype_candidate: dict[str, Any],
    ) -> tuple[bool, str]:
        family = subtype_candidate.get("family") or ""
        subtype = subtype_candidate.get("subtype") or ""
        lowered = subtype.lower()
        summary_closure = structure_features.get("has_summary_closure", False) or self._has_summary_sentence(text)
        opening_anchor = self._has_opening_anchor(text, universal_profile)
        multi_support = paragraph_count >= 2 or sentence_count >= 5
        stable_center = universal_profile.get("single_center_strength", 0.0) >= 0.65
        ordering_signal = structure_features.get("is_order_sensitive", False) or structure_features.get("strong_ordering_signal", False)

        if family == self.ordering_family:
            if "棣栧熬" in subtype or "首尾" in subtype:
                if opening_anchor and summary_closure and ordering_signal:
                    return True, "current_material_contains_opening_and_closing_ordering_anchors"
                return False, "missing_head_or_tail_ordering_anchor_in_material"
            if "\u6392\u5e8f" in subtype or "\u627f\u63a5" in subtype or "鎵挎帴" in subtype:
                if ordering_signal and structure_features.get("sequence_marker_count", 0) >= 2:
                    return True, "current_material_contains_sequence_markers_and_order_dependency"
                return False, "missing_local_order_dependency_in_material"
            if structure_features.get("sequence_marker_count", 0) >= 2 or ordering_signal:
                return True, "ordering_family_supported_by_material_level_structure"
            return False, "ordering_family_lacks_local_structure_evidence"

        if family == self.summarization_family:
            if "缁撳熬" in subtype or "\u7ed3\u5c3e" in subtype:
                if summary_closure:
                    return True, "current_material_contains_closing_summary_anchor"
                return False, "missing_closing_summary_anchor_in_material"
            if "寮€澶" in subtype or "\u5f00\u5934" in subtype:
                if opening_anchor:
                    return True, "current_material_contains_opening_anchor"
                return False, "missing_opening_anchor_in_material"
            if "鍏ㄦ枃" in subtype or "\u5355\u4e2d\u5fc3" in subtype:
                if multi_support and stable_center:
                    return True, "current_material_contains_multiple_support_points_under_single_center"
                return False, "material_does_not_cover_enough_support_points_for_full_integration"
            if stable_center:
                return True, "current_material_has_stable_main_idea"
            return False, "material_main_idea_not_stable_enough"

        if family == self.title_family:
            if universal_profile.get("titleability", 0.0) >= 0.65 and stable_center:
                return True, "current_material_supports_title_abstraction"
            return False, "material_center_not_clear_enough_for_title_naming"

        if family == self.fill_family:
            if "灏惧彞" in subtype or "\u5c3e\u53e5" in subtype:
                if summary_closure:
                    return True, "current_material_contains_tail_sentence_closure"
                return False, "missing_tail_sentence_closure_in_material"
            if universal_profile.get("transition_strength", 0.0) >= 0.62 or universal_profile.get("explanation_strength", 0.0) >= 0.62:
                return True, "current_material_has_local_bridge_or_explanation_need"
            return False, "material_lacks_local_fill_gap_evidence"

        if family == self.continuation_family:
            tail = text.strip()[-100:]
            has_complete_tail = text.strip().endswith(("\u3002", "\uff01", "\uff1f", "!", "?"))
            has_tail_extension_signal = structure_features.get("has_tail_extension_signal", False) or any(
                token in tail
                for token in (
                    "\u8fd9\u4e5f\u610f\u5473\u7740",
                    "\u8fd9\u8981\u6c42",
                    "\u8fd9\u63d0\u9192\u6211\u4eec",
                    "\u5173\u952e\u5728\u4e8e",
                    "\u8fd8\u9700\u8981",
                    "\u8fdb\u4e00\u6b65",
                    "\u624d\u80fd",
                )
            )
            if (
                has_complete_tail
                and has_tail_extension_signal
                and universal_profile.get("continuation_openness", 0.0) >= 0.62
                and universal_profile.get("direction_uniqueness", 0.0) >= 0.5
            ):
                return True, "current_material_is_complete_but_tail_sentence_retains_extension_slot"
            return False, "continuation_requires_complete_tail_sentence_with_unique_extension_signal"

        if family_profiles.get(family):
            return True, "family_profile_present"
        return True, "fallback_accept_without_specific_coverage_rule"

    def _has_opening_anchor(self, text: str, universal_profile: dict[str, Any]) -> bool:
        if "\u5f00\u5934\u603b\u9886" in universal_profile.get("position_roles", []):
            return True
        lead = text.strip()[:80]
        return any(token in lead for token in ("\u8981", "\u9996\u5148", "\u5f53\u524d", "\u63a8\u52a8", "\u5173\u952e\u5728\u4e8e"))

    def _has_summary_sentence(self, text: str) -> bool:
        tail = text.strip()[-80:]
        return any(token in tail for token in ("\u603b\u4e4b", "\u53ef\u89c1", "\u56e0\u6b64", "\u7531\u6b64", "\u624d\u80fd", "\u8fdb\u800c"))

    def _build_decision_reason(
        self,
        primary_entry: dict[str, Any] | None,
        structure_features: dict[str, Any],
        universal_profile: dict[str, Any],
    ) -> str:
        if primary_entry is None:
            return "no_primary_label_selected"
        family_reason = self._build_family_reason(primary_entry["family"], structure_features, universal_profile)
        if primary_entry["is_wide"]:
            return f"fallback_to_family_level_label_after_subtype_coverage_validation: {family_reason}"
        return f"selected_primary_subtype_after_coverage_validation: {family_reason}"

    def _build_family_reason(self, family: str, structure_features: dict[str, Any], universal_profile: dict[str, Any]) -> str:
        if family == self.ordering_family:
            markers = "/".join(structure_features.get("sequence_markers", [])[:3]) or "\u9996\u5148/\u5176\u6b21/\u518d\u6b21"
            return f"\u5b58\u5728\u201c{markers}\u201d\u7b49\u5e8f\u5217\u6807\u8bb0\uff0c\u6bb5\u95f4\u987a\u5e8f\u4f9d\u8d56\u660e\u663e\uff0c\u6253\u4e71\u540e\u903b\u8f91\u4f1a\u53d7\u635f"
        if family == self.summarization_family:
            if universal_profile.get("summary_strength", 0.0) >= 0.68:
                return "\u5168\u6587\u56f4\u7ed5\u5355\u4e00\u4e2d\u5fc3\u5c55\u5f00\uff0c\u591a\u4e2a\u5206\u70b9\u5171\u540c\u652f\u6491\u4e3b\u65e8\uff0c\u9002\u5408\u6982\u62ec\u6574\u5408"
            return "\u6587\u6bb5\u4e3b\u65e8\u96c6\u4e2d\uff0c\u4fe1\u606f\u53ef\u6536\u675f\u4e3a\u7edf\u4e00\u4e2d\u5fc3\uff0c\u9002\u5408\u6982\u62ec\u5f52\u7eb3"
        if family == self.title_family:
            if universal_profile.get("titleability", 0.0) >= 0.72:
                return "\u6587\u6bb5\u4e2d\u5fc3\u660e\u786e\uff0c\u53ef\u8fdb\u4e00\u6b65\u62bd\u8c61\u4e3a\u6807\u9898\u6216\u547d\u540d\u8868\u8fbe"
            return "\u6750\u6599\u6709\u7a33\u5b9a\u4e3b\u65e8\uff0c\u9002\u5408\u8fdb\u884c\u6807\u9898\u63d0\u70bc"
        if family == self.fill_family:
            return "\u5c40\u90e8\u5b58\u5728\u627f\u63a5\u3001\u89e3\u91ca\u6216\u8f6c\u6298\u8865\u4f4d\u9700\u6c42\uff0c\u9002\u5408\u8003\u67e5\u8bed\u53e5\u8854\u63a5\u4e0e\u8865\u4f4d"
        if family == self.continuation_family:
            return "\u6587\u6bb5\u672c\u8eab\u5df2\u5b8c\u6574\u6536\u675f\uff0c\u4f46\u5c3e\u53e5\u4ecd\u7559\u6709\u660e\u786e\u5ef6\u5c55\u843d\u70b9\uff0c\u9002\u5408\u5c3e\u6bb5\u7eed\u5199"
        return "\u4fdd\u7559\u8be5\u65cf\u7c7b\u4f5c\u4e3a\u5e76\u5217\u80fd\u529b\u5907\u9009"
