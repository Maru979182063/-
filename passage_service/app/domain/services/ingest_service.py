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
        discovered_urls: list[str] = []
        for list_url in list_urls:
            try:
                html = self.fetcher.fetch_text(list_url)
                discovered_urls.extend(discover_article_urls(html, list_url, source, limit=discovery_limit))
            except Exception as exc:  # noqa: BLE001
                logger.warning("crawl list page failed: %s %s", list_url, exc)
                self.audit_repo.log("crawl_source", source_id, "crawl_list_failed", {"url": list_url, "error": str(exc)})

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
                html = self.fetcher.fetch_text(article_url)
                parsed = self.extractor.extract(html, article_url, source)
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


def _find_source_config(source_id: str) -> dict | None:
    sources = get_config_bundle().sources.get("sources", [])
    for source in sources:
        if source.get("id") == source_id:
            return source
    return None
