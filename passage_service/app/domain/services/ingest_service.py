import json
import re
from urllib.parse import urlparse

from app.core.config import get_config_bundle
from app.core.enums import ArticleStatus
from app.core.logging import get_logger
from app.domain.services._common import ServiceBase
from app.domain.services.process_service import ProcessService
from app.infra.crawl.discovery import discover_article_urls
from app.infra.crawl.extractors.readability_extractor import ReadabilityLikeExtractor
from app.infra.crawl.fetchers.http_fetcher import HttpCrawlerFetcher
from app.infra.ingest.cleaners.basic_cleaner import BasicCleaner
from app.infra.ingest.dedupe.content_hash import build_content_hash


logger = get_logger(__name__)


class IngestService(ServiceBase):
    def ingest(self, payload: dict) -> object:
        cleaner = BasicCleaner()
        clean_text = cleaner.clean(payload["raw_text"])
        content_hash = build_content_hash(clean_text)
        existing = self.article_repo.get_by_hash(content_hash)
        if existing is not None:
            return existing
        existing_url = self.article_repo.get_by_source_url(payload["source_url"])
        if existing_url is not None:
            article = self.article_repo.update(
                existing_url.id,
                source=payload["source"],
                title=payload.get("title"),
                raw_text=payload["raw_text"],
                clean_text=clean_text,
                language=payload.get("language", "zh"),
                domain=payload.get("domain"),
                status=ArticleStatus.CLEANED.value,
                hash=content_hash,
            )
            self.audit_repo.log("article", article.id, "ingest_update", {"source_url": article.source_url})
            return article
        article = self.article_repo.create(
            source=payload["source"],
            source_url=payload["source_url"],
            title=payload.get("title"),
            raw_text=payload["raw_text"],
            clean_text=clean_text,
            language=payload.get("language", "zh"),
            domain=payload.get("domain"),
            status=ArticleStatus.CLEANED.value,
            hash=content_hash,
        )
        self.audit_repo.log("article", article.id, "ingest", {"source_url": article.source_url})
        return article


class CrawlService(ServiceBase):
    def run_all_sources(self) -> dict:
        job = self.job_repo.create("crawl_run_all", {"mode": "all_sources"})
        result = {"sources": []}
        sources = get_config_bundle().sources.get("sources", [])
        for source in sources:
            if not source.get("enabled", True):
                continue
            result["sources"].append(run_crawl_for_source(self.session, source["id"]))
        self.job_repo.mark_finished(job.id, "finished", result)
        return {"job_id": job.id, "status": "finished", "result": result}

    def run_source(self, source_id: str) -> dict:
        job = self.job_repo.create("crawl_run_source", {"source_id": source_id})
        result = run_crawl_for_source(self.session, source_id)
        self.job_repo.mark_finished(job.id, "finished", result)
        return {"job_id": job.id, "status": "finished", "source_id": source_id, "result": result}

    def get_job(self, job_id: str) -> dict:
        job = self.job_repo.get(job_id)
        if job is None:
            return {"job_id": job_id, "status": "not_found"}
        return {"job_id": job.id, "status": job.status, "result": job.result}


def run_crawl_for_source(session, source_id: str) -> dict:
    service = _SourceCrawler(session)
    return service.run(source_id)


class _SourceCrawler(ServiceBase):
    def __init__(self, session) -> None:
        super().__init__(session)
        self.fetcher = HttpCrawlerFetcher()
        self.extractor = ReadabilityLikeExtractor()

    def run(self, source_id: str) -> dict:
        source = _find_source_config(source_id)
        if source is None:
            return {"source_id": source_id, "status": "not_found"}

        list_urls = source.get("entry_urls") or [source.get("base_url")]
        article_limit = int(source.get("article_limit", 50))
        discovery_limit = max(article_limit * 5, article_limit)
        discovered_urls = self._discover_urls(source_id=source_id, source=source, list_urls=list_urls, discovery_limit=discovery_limit)

        seen: set[str] = set()
        candidates = []
        for url in discovered_urls:
            if url not in seen:
                seen.add(url)
                candidates.append(url)
        existing_urls = self.article_repo.get_existing_source_urls(candidates)
        fresh_candidates = [url for url in candidates if url not in existing_urls]
        skipped_existing = len(candidates) - len(fresh_candidates)
        candidates = fresh_candidates[:article_limit]

        ingested = 0
        processed = 0
        failures = 0
        processed_article_ids: list[str] = []
        for article_url in candidates:
            try:
                parsed = self._extract_article(source_id=source_id, source=source, article_url=article_url)
                raw_text = parsed.get("raw_text", "").strip()
                if len(raw_text) < int(source.get("min_body_length", 180)):
                    self.audit_repo.log("crawl_article", article_url, "crawl_skip_short_body", {"source_id": source_id})
                    continue
                article = IngestService(self.session).ingest(
                    {
                        "source": source["site_name"],
                        "source_url": article_url,
                        "title": parsed.get("title"),
                        "raw_text": raw_text,
                        "language": source.get("language", "zh"),
                        "domain": source.get("domain"),
                    }
                )
                if source.get("auto_process_after_ingest", True):
                    ProcessService(self.session).process_article(article.id, mode=source.get("process_mode", "full"))
                    processed += 1
                    processed_article_ids.append(article.id)
                self.audit_repo.log(
                    "crawl_article",
                    article_url,
                    "crawl_article_ingested",
                    {"source_id": source_id, "title": parsed.get("title"), "published_at": parsed.get("published_at")},
                )
                ingested += 1
            except Exception as exc:  # noqa: BLE001
                failures += 1
                logger.warning("crawl article failed: %s %s", article_url, exc)
                self.session.rollback()
                self.audit_repo.log("crawl_article", article_url, "crawl_article_failed", {"source_id": source_id, "error": str(exc)})

        return {
            "source_id": source_id,
            "site_name": source["site_name"],
            "discovered_count": len(discovered_urls),
            "unique_candidate_count": len(seen),
            "skipped_existing_count": skipped_existing,
            "candidate_count": len(candidates),
            "ingested_count": ingested,
            "processed_count": processed,
            "processed_article_ids": processed_article_ids,
            "failed_count": failures,
            "status": "finished",
        }

    def _discover_urls(self, *, source_id: str, source: dict, list_urls: list[str], discovery_limit: int) -> list[str]:
        discovered_urls: list[str] = []
        for list_url in list_urls:
            try:
                html = self.fetcher.fetch_text(list_url)
                discovered_urls.extend(discover_article_urls(html, list_url, source, limit=discovery_limit))
            except Exception as exc:  # noqa: BLE001
                logger.warning("crawl list page failed: %s %s", list_url, exc)
                self.audit_repo.log("crawl_source", source_id, "crawl_list_failed", {"url": list_url, "error": str(exc)})

        if source_id == "lifeweek" and len(discovered_urls) < max(10, discovery_limit // 5):
            discovered_urls.extend(self._discover_lifeweek_article_urls(limit=discovery_limit))

        if source_id == "whb" and len(discovered_urls) < max(10, discovery_limit // 5):
            discovered_urls.extend(self._discover_whb_article_urls(limit=discovery_limit))

        return discovered_urls

    def _discover_lifeweek_article_urls(self, *, limit: int) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        page_size = min(50, max(20, limit // 2))
        max_pages = 3
        for page in range(1, max_pages + 1):
            api_url = f"https://www.lifeweek.com.cn/api/article?currPage={page}&rowsPerPage={page_size}"
            try:
                payload = json.loads(self.fetcher.fetch_text(api_url))
            except Exception:  # noqa: BLE001
                continue
            model = payload.get("model") or {}
            items = model.get("list") or []
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                article_id = item.get("id")
                if not article_id:
                    continue
                article_url = f"https://www.lifeweek.com.cn/article/{article_id}"
                if article_url in seen:
                    continue
                seen.add(article_url)
                urls.append(article_url)
                if len(urls) >= limit:
                    return urls
        return urls

    def _discover_whb_article_urls(self, *, limit: int) -> list[str]:
        entry_url = "https://www.whb.cn/channel/1008"
        try:
            html = self.fetcher.fetch_text(entry_url)
        except Exception:  # noqa: BLE001
            return []

        build_id_match = re.search(r'"buildId"\s*:\s*"([^"]+)"', html)
        if not build_id_match:
            return []
        build_id = build_id_match.group(1)
        index_api = f"https://www.whb.cn/_next/data/{build_id}/index.json"
        try:
            payload = json.loads(self.fetcher.fetch_text(index_api))
        except Exception:  # noqa: BLE001
            return []

        page_info = (payload.get("pageProps") or {}).get("data", {}).get("pageInfo", {})
        cards = page_info.get("list") or []
        urls: list[str] = []
        seen: set[str] = set()
        for card in cards:
            entries = []
            if isinstance(card, dict):
                entries.append(card)
                entries.extend(card.get("childList") or [])
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                share_url = ((entry.get("shareInfo") or {}).get("shareUrl") or "").strip()
                cont_id = entry.get("contId")
                candidate = share_url or (f"https://www.whb.cn/commonDetail/{cont_id}" if cont_id else "")
                if not candidate:
                    continue
                parsed = urlparse(candidate)
                if parsed.netloc.lower() == "m.whb.cn":
                    candidate = candidate.replace("https://m.whb.cn", "https://www.whb.cn").replace("http://m.whb.cn", "https://www.whb.cn")
                if candidate in seen:
                    continue
                seen.add(candidate)
                urls.append(candidate)
                if len(urls) >= limit:
                    return urls
        return urls

    def _extract_article(self, *, source_id: str, source: dict, article_url: str) -> dict:
        if source_id == "lifeweek":
            parsed = self._extract_lifeweek_article(article_url=article_url, source=source)
            if parsed is not None:
                return parsed
        html = self.fetcher.fetch_text(article_url)
        return self.extractor.extract(html, article_url, source)

    def _extract_lifeweek_article(self, *, article_url: str, source: dict) -> dict | None:
        match = re.search(r"/article/(\d+)", article_url)
        if not match:
            return None
        article_id = match.group(1)
        api_url = f"https://www.lifeweek.com.cn/api/article/{article_id}"
        try:
            payload = json.loads(self.fetcher.fetch_text(api_url))
        except Exception:  # noqa: BLE001
            return None
        model = payload.get("model") or {}
        content_html = (model.get("content") or "").strip()
        if not content_html:
            return None
        title = (model.get("title") or "").strip()
        published_at = (model.get("pubTime") or "").strip() or None
        if not published_at:
            read_time = str(model.get("readTime") or "").strip()
            if re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", read_time):
                published_at = read_time
        synthetic_html = f"<html><head><title>{title}</title></head><body><div class='article-content'>{content_html}</div></body></html>"
        parsed = self.extractor.extract(synthetic_html, article_url, source)
        if title:
            parsed["title"] = title
        if published_at:
            parsed["published_at"] = published_at
        return parsed


def _find_source_config(source_id: str) -> dict | None:
    sources = get_config_bundle().sources.get("sources", [])
    for source in sources:
        if source.get("id") == source_id:
            return source
    return None
