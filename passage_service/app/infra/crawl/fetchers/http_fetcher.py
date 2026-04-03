import re

import httpx

from app.infra.crawl.fetchers.base import BaseCrawlerFetcher


class HttpCrawlerFetcher(BaseCrawlerFetcher):
    def __init__(self, timeout: float = 15.0, user_agent: str | None = None) -> None:
        self.timeout = timeout
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )

    def fetch_text(self, url: str) -> str:
        with httpx.Client(
            timeout=self.timeout,
            follow_redirects=True,
            headers={"User-Agent": self.user_agent},
        ) as client:
            response = client.get(url)
            response.raise_for_status()
            return self._decode_response(response)

    def _decode_response(self, response: httpx.Response) -> str:
        raw = response.content
        encodings: list[str] = []

        if response.encoding:
            encodings.append(response.encoding)

        content_type = response.headers.get("content-type", "")
        match = re.search(r"charset=([a-zA-Z0-9._-]+)", content_type, re.IGNORECASE)
        if match:
            encodings.append(match.group(1))

        head = raw[:4096].decode("ascii", errors="ignore")
        meta_patterns = [
            r'<meta[^>]+charset=["\']?([a-zA-Z0-9._-]+)',
            r'<meta[^>]+content=["\'][^"\']*charset=([a-zA-Z0-9._-]+)',
        ]
        for pattern in meta_patterns:
            meta_match = re.search(pattern, head, re.IGNORECASE)
            if meta_match:
                encodings.append(meta_match.group(1))

        encodings.extend(["utf-8", "gb18030", "gbk", "gb2312"])

        tried: set[str] = set()
        for encoding in encodings:
            normalized = encoding.strip().lower()
            if not normalized or normalized in tried:
                continue
            tried.add(normalized)
            try:
                return raw.decode(normalized)
            except (LookupError, UnicodeDecodeError):
                continue

        return raw.decode("utf-8", errors="replace")
