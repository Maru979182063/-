# passage-service

MVP passage material service for:

- article ingest
- segmentation into candidate spans
- plugin-based tagging
- material pool search/promotion
- feedback loop and gray release
- fixed-source crawling with manual and scheduled triggers

## Run

Install dependencies from `pyproject.toml`, then start:

```bash
uvicorn app.main:app --reload
```

Run from:

```bash
cd passage_service
```

## Quick Start

1. Run bootstrap:

```powershell
.\scripts\bootstrap.ps1
```

2. Set your API key in the environment:

```powershell
$env:PASSAGE_OPENAI_API_KEY="your_key"
```

3. Enable LLM tagging in `app/config/llm.yaml`.

4. Start the app:

```powershell
.\.venv\Scripts\Activate.ps1
uvicorn app.main:app --reload
```

5. Run one end-to-end processing test:

```powershell
.\scripts\test_process_article.ps1
```

## Key APIs

- `POST /articles/ingest`
- `POST /articles/{article_id}/segment`
- `POST /articles/{article_id}/tag`
- `POST /articles/{article_id}/process`
- `POST /articles/{article_id}/review-export`
- `POST /materials/search`
- `POST /materials/promote`
- `POST /materials/reprocess`
- `POST /materials/feedback`
- `POST /crawl/run`
- `POST /crawl/source/{source_id}/run`
- `GET /crawl/jobs/{job_id}`

## Crawl Notes

- Current crawler targets static pages first.
- Dedup uses cleaned body hash; same URL with changed content updates the existing article record.
- Source definitions live in `app/config/sources.yaml`.
- Scheduler reads per-source cron expressions from `sources.yaml`.
- A quick manual verification script for the five core sites is available at `scripts/sample_core_sites.ps1`.
- The sample script writes review files into `review_samples/core_sites/` for manual inspection.
- Processed article review exports are written into `review_samples/processed/<article_id>/`.

## LLM Tagging

- LLM settings live in `app/config/llm.yaml`.
- Universal coarse tagging is configured to use a cheaper model.
- Family-specific fine tagging is configured to use a stronger mini model.
- If no `PASSAGE_OPENAI_API_KEY` is set, the service falls back to heuristic tagging.
