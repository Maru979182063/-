import json
import re
from typing import Any

from bs4 import BeautifulSoup


class ReadabilityLikeExtractor:
    TITLE_SELECTORS = ["h1", ".title", ".article-title", ".content-title"]
    CONTENT_SELECTORS = [
        "article",
        ".article",
        ".article-content",
        ".content",
        ".pages_content",
        ".TRS_Editor",
        ".rm_txt_con",
        ".detail",
        ".post_content",
        "#p-detail",
        "#main-content",
    ]
    DATE_PATTERNS = [
        re.compile(r"\d{4}-\d{2}-\d{2}"),
        re.compile(r"\d{4}/\d{2}/\d{2}"),
        re.compile(r"\d{4}年\d{1,2}月\d{1,2}日"),
    ]

    def extract(self, html: str, url: str, source_config: dict[str, Any]) -> dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")

        title = ""
        title_selectors = source_config.get("title_selectors", []) + self.TITLE_SELECTORS
        for selector in title_selectors:
            node = soup.select_one(selector)
            if node and node.get_text(strip=True):
                title = node.get_text(" ", strip=True)
                break
        if not title and soup.title:
            title = soup.title.get_text(" ", strip=True)

        published_at = None
        meta_candidates = [
            soup.find("meta", attrs={"property": "article:published_time"}),
            soup.find("meta", attrs={"name": "publishdate"}),
            soup.find("meta", attrs={"name": "PubDate"}),
            soup.find("meta", attrs={"name": "publish-date"}),
        ]
        for meta in meta_candidates:
            if meta and meta.get("content"):
                published_at = meta["content"].strip()
                break
        if not published_at:
            text = soup.get_text("\n", strip=True)
            for pattern in self.DATE_PATTERNS:
                match = pattern.search(text)
                if match:
                    published_at = match.group(0)
                    break

        body_text = ""
        content_selectors = source_config.get("content_selectors", []) + self.CONTENT_SELECTORS
        best_body_text = ""
        for selector in content_selectors:
            node = soup.select_one(selector)
            if not node:
                continue
            candidate_text = self._extract_node_text(node)
            if len(candidate_text) > len(best_body_text):
                best_body_text = candidate_text
        body_text = best_body_text

        embedded = self._extract_embedded_payload(soup, html)
        if embedded:
            if embedded.get("title") and (not title or len(title.strip()) <= 2):
                title = embedded["title"].strip()
            if embedded.get("published_at") and not published_at:
                published_at = embedded["published_at"].strip()
            embedded_body = (embedded.get("raw_text") or "").strip()
            if len(embedded_body) > len(body_text):
                body_text = embedded_body

        if not published_at:
            for selector in source_config.get("date_selectors", []):
                node = soup.select_one(selector)
                if node and node.get_text(strip=True):
                    published_at = node.get_text(" ", strip=True)
                    break

        if not body_text:
            paragraphs = [p.get_text(" ", strip=True) for p in soup.select("p") if p.get_text(strip=True)]
            body_text = "\n\n".join(paragraphs[:80])

        body_text = self._cleanup_text(body_text)
        return {
            "title": title,
            "published_at": published_at,
            "raw_text": body_text,
            "source_url": url,
        }

    def _extract_embedded_payload(self, soup: BeautifulSoup, raw_html: str) -> dict[str, str]:
        next_payload = self._extract_from_next_data(soup)
        if next_payload and next_payload.get("raw_text"):
            return next_payload
        json_payload = self._extract_from_json_response(raw_html)
        if json_payload and json_payload.get("raw_text"):
            return json_payload
        return {}

    def _extract_from_next_data(self, soup: BeautifulSoup) -> dict[str, str]:
        script = soup.find("script", attrs={"id": "__NEXT_DATA__", "type": "application/json"})
        if script is None:
            return {}
        script_text = script.string or script.get_text(strip=True)
        if not script_text:
            return {}
        try:
            payload = json.loads(script_text)
        except json.JSONDecodeError:
            return {}

        page_data = (((payload.get("props") or {}).get("pageProps") or {}).get("data") or {})
        if not isinstance(page_data, dict):
            return {}

        content_html = ((page_data.get("textInfo") or {}).get("content") or page_data.get("content") or "").strip()
        body_text = self._extract_text_from_html_fragment(content_html)
        if not body_text:
            body_text = str(page_data.get("summary") or "").strip()
        return {
            "title": str(page_data.get("title") or page_data.get("name") or "").strip(),
            "published_at": str(page_data.get("pubTime") or page_data.get("publishTime") or "").strip(),
            "raw_text": body_text,
        }

    def _extract_from_json_response(self, raw_html: str) -> dict[str, str]:
        stripped = raw_html.strip()
        if not (stripped.startswith("{") and stripped.endswith("}")):
            return {}
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            return {}

        model = payload.get("model")
        if isinstance(model, list):
            model = model[0] if model else {}
        if not isinstance(model, dict):
            return {}

        content_html = ((model.get("textInfo") or {}).get("content") or model.get("content") or "").strip()
        body_text = self._extract_text_from_html_fragment(content_html)
        if not body_text:
            body_text = str(model.get("summary") or "").strip()
        return {
            "title": str(model.get("title") or model.get("name") or "").strip(),
            "published_at": str(model.get("pubTime") or model.get("publishTime") or "").strip(),
            "raw_text": body_text,
        }

    def _extract_text_from_html_fragment(self, fragment_html: str) -> str:
        if not fragment_html:
            return ""
        fragment_soup = BeautifulSoup(fragment_html, "html.parser")
        container = fragment_soup.select_one("article, .article, .article-content, .content, .TRS_Editor, .detail, #main-content, body")
        if container is None:
            container = fragment_soup
        return self._extract_node_text(container)

    def _extract_node_text(self, node) -> str:
        paragraphs = [p.get_text(" ", strip=True) for p in node.select("p") if p.get_text(strip=True)]
        if paragraphs:
            return "\n\n".join(paragraphs)
        return node.get_text("\n\n", strip=True)

    def _cleanup_text(self, text: str) -> str:
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        lines = []
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if any(token in stripped for token in ("责任编辑", "编辑：", "免责声明", "版权所有", "推荐阅读", "相关阅读")):
                continue
            lines.append(stripped)
        return "\n\n".join(lines).strip()
