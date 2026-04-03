from __future__ import annotations

import hashlib
import re
from collections import defaultdict

from app.core.config import get_config_bundle
from app.services.material_governance import MaterialCandidate


class MaterialMergeService:
    def __init__(self) -> None:
        merge_config = get_config_bundle().material_governance.get("merge", {})
        self.containment_ratio = float(merge_config.get("containment_ratio", 0.8))
        self.jaccard_ratio = float(merge_config.get("jaccard_ratio", 0.85))
        self.lcs_ratio = float(merge_config.get("lcs_ratio", 0.8))
        self.target_length = int(merge_config.get("target_length", 320))

    def merge(self, items: list[MaterialCandidate]) -> list[MaterialCandidate]:
        if not items:
            return []

        normalized_items: list[tuple[MaterialCandidate, str, str]] = []
        for item in items:
            normalized = self.normalize_text(item.text)
            normalized_hash = hashlib.sha1(normalized.encode("utf-8")).hexdigest()
            item.normalized_text_hash = normalized_hash
            normalized_items.append((item, normalized, normalized_hash))

        parent = list(range(len(normalized_items)))

        def find(index: int) -> int:
            while parent[index] != index:
                parent[index] = parent[parent[index]]
                index = parent[index]
            return index

        def union(left: int, right: int) -> None:
            root_left = find(left)
            root_right = find(right)
            if root_left != root_right:
                parent[root_right] = root_left

        for left in range(len(normalized_items)):
            for right in range(left + 1, len(normalized_items)):
                if self.should_merge(normalized_items[left][1], normalized_items[right][1]):
                    union(left, right)

        groups: dict[int, list[tuple[MaterialCandidate, str, str]]] = defaultdict(list)
        for index, payload in enumerate(normalized_items):
            groups[find(index)].append(payload)

        merged: list[MaterialCandidate] = []
        for group_index, group in enumerate(groups.values(), start=1):
            primary, _, primary_hash = sorted(group, key=lambda entry: self._sort_key(entry[0]))[0]
            variants = []
            for sibling, _, _ in group:
                if sibling.candidate_span_id == primary.candidate_span_id:
                    continue
                variants.append(
                    {
                        "candidate_span_id": sibling.candidate_span_id,
                        "text": sibling.text,
                        "primary_label": sibling.primary_label,
                        "candidate_labels": sibling.candidate_labels or [],
                    }
                )
            primary.normalized_text_hash = primary_hash
            primary.decision_trace = {
                **(primary.decision_trace or {}),
                "merge_group_size": len(group),
            }
            primary.variants = variants
            primary.source = dict(primary.source)
            primary.source["variant_count"] = len(variants)
            primary.primary_route = {
                **primary.primary_route,
                "material_family_id": f"{primary.article_id}:family:{group_index}",
            }
            merged.append(primary)
        return merged

    def normalize_text(self, text: str) -> str:
        translation = str.maketrans(
            {
                "\u3000": " ",
                "\uFF0C": ",",
                "\u3002": ".",
                "\uFF1B": ";",
                "\uFF1A": ":",
                "\uFF01": "!",
                "\uFF1F": "?",
                "\u201C": "\"",
                "\u201D": "\"",
                "\u2018": "'",
                "\u2019": "'",
            }
        )
        normalized = text.strip().translate(translation)
        normalized = re.sub(r"\s+", "", normalized)
        return normalized

    def should_merge(self, left: str, right: str) -> bool:
        if not left or not right:
            return False
        if left == right:
            return True
        shorter, longer = (left, right) if len(left) <= len(right) else (right, left)
        if shorter and shorter in longer and len(shorter) / max(len(longer), 1) >= self.containment_ratio:
            return True
        if self._jaccard(left, right) >= self.jaccard_ratio:
            return True
        if self._lcs_ratio(left, right) >= self.lcs_ratio:
            return True
        return False

    def _jaccard(self, left: str, right: str) -> float:
        left_set = self._shingles(left)
        right_set = self._shingles(right)
        if not left_set or not right_set:
            return 0.0
        return len(left_set & right_set) / len(left_set | right_set)

    def _shingles(self, text: str, width: int = 3) -> set[str]:
        if len(text) <= width:
            return {text}
        return {text[index : index + width] for index in range(len(text) - width + 1)}

    def _lcs_ratio(self, left: str, right: str) -> float:
        rows = len(left) + 1
        cols = len(right) + 1
        current = [0] * cols
        best = 0
        for row in range(1, rows):
            previous = 0
            for col in range(1, cols):
                temp = current[col]
                if left[row - 1] == right[col - 1]:
                    current[col] = previous + 1
                    best = max(best, current[col])
                else:
                    current[col] = 0
                previous = temp
        return best / max(min(len(left), len(right)), 1)

    def _sort_key(self, item: MaterialCandidate) -> tuple[int, int, int, float, int]:
        primary_family = item.primary_route.get("family")
        family_score = float(item.family_scores.get(primary_family, 0.0)) if primary_family else 0.0
        return (
            len(item.quality_flags),
            0 if item.release_channel == "stable" else 1,
            0 if item.primary_route.get("subtype") else 1,
            -family_score,
            abs(len(item.text) - self.target_length),
        )
