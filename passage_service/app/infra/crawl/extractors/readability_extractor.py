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
