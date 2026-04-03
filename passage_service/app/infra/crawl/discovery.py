import re
from collections.abc import Iterable
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


def discover_article_urls(html: str, base_url: str, source_config: dict, limit: int = 50) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    allowed_domains = source_config.get("allowed_domains") or [urlparse(base_url).netloc]
    patterns = [re.compile(p) for p in source_config.get("article_url_patterns", [])]
    exclude_patterns = [re.compile(p) for p in source_config.get("exclude_url_patterns", [])]
    keywords = tuple(source_config.get("priority_keywords", []))
    require_pattern_match = bool(source_config.get("require_pattern_match", True))
    scored: list[tuple[int, str]] = []
    seen: set[str] = set()

    for anchor in soup.select("a[href]"):
        href = anchor.get("href", "").strip()
        if not href:
            continue
        url = urljoin(base_url, href)
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if not any(domain in parsed.netloc for domain in allowed_domains):
            continue
        if any(pattern.search(url) for pattern in exclude_patterns):
            continue
        if url in seen:
            continue
        title = anchor.get_text(" ", strip=True)
        matched_pattern = any(pattern.search(url) for pattern in patterns) if patterns else False
        if require_pattern_match and patterns and not matched_pattern:
            continue
        score = _score_candidate(url, title, patterns, keywords, matched_pattern=matched_pattern)
        if score <= 0:
            continue
        seen.add(url)
        scored.append((score, url))

    scored.sort(key=lambda item: item[0], reverse=True)
    return [url for _, url in scored[:limit]]


def _score_candidate(
    url: str,
    title: str,
    patterns: Iterable[re.Pattern],
    keywords: tuple[str, ...],
    *,
    matched_pattern: bool | None = None,
) -> int:
    score = 0
    if matched_pattern is None:
        matched_pattern = any(pattern.search(url) for pattern in patterns)
    if matched_pattern:
        score += 10
    elif patterns:
        score -= 6
    if re.search(r"/\d{4}[-/]\d{2}[-/]\d{2}/", url) or re.search(r"\d{8}", url):
        score += 5
    if re.search(r"/content[_/]", url) or re.search(r"/c\.html?$", url) or re.search(r"/c\d+-\d+\.html", url):
        score += 4
    if re.search(r"/news/\d+\.html$", url) or re.search(r"/detail/\d{8}/", url) or re.search(r"/t\d+_\d+\.htm[l]?$", url):
        score += 3
    if len(title) >= 8:
        score += 2
    if any(keyword in title for keyword in keywords):
        score += 4
    if any(token in url for token in ("article", "content", "detail", "news")):
        score += 2
    if any(token in url.lower() for token in ("index", "node_", "home.htm", "category", "recommend", "agreement", "list", "channel", "column")):
        score -= 8
    if re.fullmatch(r"https?://[^/]+/?", url):
        score -= 12
    return score
