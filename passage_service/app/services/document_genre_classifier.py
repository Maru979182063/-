from __future__ import annotations

from dataclasses import dataclass


@dataclass
class GenreRule:
    label: str
    title_keywords: list[str]
    text_keywords: list[str]
    source_keywords: list[str]


class DocumentGenreClassifier:
    def __init__(self, config: dict) -> None:
        self.default_genre = config.get("default_genre", "通用说明文")
        self.rules = [
            GenreRule(
                label=item.get("label", self.default_genre),
                title_keywords=item.get("title_keywords", []),
                text_keywords=item.get("text_keywords", []),
                source_keywords=item.get("source_keywords", []),
            )
            for item in config.get("genres", [])
        ]

    def classify(self, *, title: str | None, text: str, source: str | None) -> dict:
        title_text = title or ""
        source_text = source or ""
        scored: list[tuple[str, float]] = []
        for rule in self.rules:
            score = 0.0
            score += self._match_score(title_text, rule.title_keywords, weight=2.0)
            score += self._match_score(text, rule.text_keywords, weight=1.0)
            score += self._match_score(source_text, rule.source_keywords, weight=1.5)
            if score > 0:
                scored.append((rule.label, score))

        scored.sort(key=lambda item: item[1], reverse=True)
        candidates = [label for label, _ in scored[:3]]
        primary = candidates[0] if candidates else self.default_genre
        return {
            "document_genre": primary,
            "document_genre_candidates": candidates or [self.default_genre],
            "document_genre_scores": {label: round(score, 2) for label, score in scored[:5]},
        }

    def _match_score(self, text: str, keywords: list[str], *, weight: float) -> float:
        return sum(weight for keyword in keywords if keyword and keyword in text)
