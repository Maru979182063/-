import re
from collections.abc import Iterable
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup


def discover_article_urls(html: str, base_url: str, source_config: dict, limit: int = 50) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    allowed_domains = source_config.get("allowed_domains") or [urlparse(base_url).netloc]
    patterns = [re.compile(p) for p in source_config.get("article_url_patterns", [])]
    exclude_patterns = [re.compile(p) for p in source_config.get("exclude_url_patterns", [])]
    exclude_title_patterns = [re.compile(p, re.IGNORECASE) for p in source_config.get("exclude_title_patterns", [])]
    keywords = tuple(source_config.get("priority_keywords", []))
    required_title_keywords = tuple(source_config.get("required_title_keywords", []))
    require_pattern_match = bool(source_config.get("require_pattern_match", True))
    scored: list[tuple[int, str]] = []
    seen: set[str] = set()

    for href, title in _iter_candidate_links(soup, html):
        if not href:
            continue
        url = _normalize_url(urljoin(base_url, href))
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if not _is_allowed_domain(parsed.netloc, allowed_domains):
            continue
        if any(pattern.search(url) for pattern in exclude_patterns):
            continue
        if title and any(pattern.search(title) for pattern in exclude_title_patterns):
            continue
        if url in seen:
            continue
        matched_pattern = any(pattern.search(url) for pattern in patterns) if patterns else False
        if require_pattern_match and patterns and not matched_pattern:
            continue
        if required_title_keywords:
            if not title:
                continue
            if not any(keyword in title for keyword in required_title_keywords):
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
    lower_url = url.lower()
    lower_title = title.lower()
    score = 0
    if matched_pattern is None:
        matched_pattern = any(pattern.search(url) for pattern in patterns)
    if matched_pattern:
        score += 12
    elif patterns:
        score -= 8
    if re.search(r"/\d{4}[-/]\d{2}[-/]\d{2}/", url) or re.search(r"\d{8}", url):
        score += 6
    if re.search(r"/content[_/]", url) or re.search(r"/c\.html?$", url) or re.search(r"/c\d+-\d+\.html", url):
        score += 5
    if re.search(r"/news/\d+\.html$", url) or re.search(r"/detail/\d{8}/", url) or re.search(r"/t\d+_\d+\.htm[l]?$", url) or re.search(
        r"/newsdetail(?:_forward)?_\d+$",
        lower_url,
    ):
        score += 5
    if len(title) >= 12:
        score += 3
    elif len(title) >= 8:
        score += 2
    if any(keyword in title for keyword in keywords):
        score += 5
    if any(token in lower_url for token in ("article", "content", "detail", "news", "forward")):
        score += 2
    if re.search(r"/\d{8}/\d+[_-]\d+(_\d+)?\.html?$", lower_url):
        score += 3
    if any(
        token in lower_url
        for token in (
            "index",
            "node_",
            "home.htm",
            "category",
            "recommend",
            "agreement",
            "privacy",
            "useragreement",
            "list",
            "channel",
            "column",
            "topic",
            "login",
            "register",
            "undefined",
        )
    ):
        score -= 10
    if any(token in lower_title for token in ("privacy", "agreement", "user agreement", "terms", "login", "register")):
        score -= 10
    if re.fullmatch(r"https?://[^/]+/?", url):
        score -= 12
    return score


def _is_allowed_domain(host: str, allowed_domains: Iterable[str]) -> bool:
    normalized_host = host.strip().lower().split(":", 1)[0]
    if not normalized_host:
        return False
    for allowed in allowed_domains:
        normalized_allowed = str(allowed).strip().lower()
        if not normalized_allowed:
            continue
        if normalized_host == normalized_allowed or normalized_host.endswith(f".{normalized_allowed}"):
            return True
    return False


def _normalize_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path or "/"
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    filtered_pairs = [
        (key, value)
        for key, value in query_pairs
        if key.lower() not in {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "spm", "from", "ref"}
    ]
    normalized_query = urlencode(filtered_pairs, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, path, "", normalized_query, ""))


def _iter_candidate_links(soup: BeautifulSoup, html: str) -> Iterable[tuple[str, str]]:
    for anchor in soup.select("a[href]"):
        href = anchor.get("href", "").strip()
        if href:
            yield href, anchor.get_text(" ", strip=True)

    for href in _extract_embedded_urls(html):
        yield href, ""


def _extract_embedded_urls(html: str) -> list[str]:
    raw_matches = re.findall(r"""["']((?:https?://|/)[^"'<>\s]{6,})["']""", html)
    urls: list[str] = []
    seen: set[str] = set()
    for match in raw_matches:
        candidate = match.replace("\\/", "/").strip().rstrip(",;")
        lower_candidate = candidate.lower()
        if not candidate or lower_candidate.startswith(("javascript:", "mailto:", "tel:")):
            continue
        path_lower = urlparse(candidate).path.lower()
        if any(
            path_lower.endswith(ext)
            for ext in (".css", ".js", ".json", ".xml", ".txt", ".map", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".woff", ".ttf")
        ):
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        urls.append(candidate)
    return urls
