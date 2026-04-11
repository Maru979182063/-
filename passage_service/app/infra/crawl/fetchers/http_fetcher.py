import re
import time
from urllib.parse import urlparse, urlunparse

import httpx

from app.infra.crawl.fetchers.base import BaseCrawlerFetcher


class HttpCrawlerFetcher(BaseCrawlerFetcher):
    def __init__(
        self,
        timeout: float = 15.0,
        user_agent: str | None = None,
        retries: int = 2,
        backoff_seconds: float = 0.8,
    ) -> None:
        self.timeout = timeout
        self.retries = max(0, retries)
        self.backoff_seconds = max(0.1, backoff_seconds)
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )

    def fetch_text(self, url: str) -> str:
        profiles = self._build_request_profiles(url)
        last_exception: Exception | None = None

        for profile in profiles:
            request_url = profile["url"]
            timeout = float(profile["timeout"])
            headers = profile["headers"]

            for attempt in range(self.retries + 1):
                try:
                    with httpx.Client(timeout=timeout, follow_redirects=True, headers=headers) as client:
                        response = client.get(request_url)
                        response.raise_for_status()
                        return self._decode_response(response)
                except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError) as exc:
                    last_exception = exc
                    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in {401, 403, 404}:
                        break
                    if attempt < self.retries:
                        time.sleep(self.backoff_seconds * (2**attempt))

        if last_exception is not None:
            raise last_exception
        raise RuntimeError(f"failed to fetch {url}")

    def _build_request_profiles(self, url: str) -> list[dict]:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        default_headers = self._merge_headers({})
        default_timeout = self.timeout
        profiles: list[dict] = [
            {"url": url, "timeout": default_timeout, "headers": default_headers},
        ]

        if host.endswith("thepaper.cn"):
            profiles.insert(
                0,
                {
                    "url": url,
                    "timeout": max(default_timeout, 20.0),
                    "headers": self._merge_headers(
                        {
                            "Referer": "https://m.thepaper.cn/",
                            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                        }
                    ),
                },
            )
        elif host.endswith("guokr.com"):
            profiles.insert(
                0,
                {
                    "url": url,
                    "timeout": max(default_timeout, 18.0),
                    "headers": self._merge_headers({"Referer": "https://www.guokr.com/science/"}),
                },
            )
        elif host.endswith("whb.cn") or host.endswith("ce.cn"):
            profiles[0]["timeout"] = max(default_timeout, 25.0)

        # Fallback to HTTP for sites that occasionally fail TLS handshake.
        if parsed.scheme == "https" and (host.endswith("whb.cn") or host.endswith("ce.cn")):
            http_url = urlunparse(("http", parsed.netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
            profiles.append(
                {
                    "url": http_url,
                    "timeout": max(default_timeout, 25.0),
                    "headers": default_headers,
                }
            )

        return profiles

    def _merge_headers(self, extra_headers: dict[str, str]) -> dict[str, str]:
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
        }
        headers.update(extra_headers)
        return headers

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
