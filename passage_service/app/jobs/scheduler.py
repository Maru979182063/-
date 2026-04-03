from apscheduler.schedulers.background import BackgroundScheduler

from app.core.config import get_config_bundle
from app.core.logging import get_logger


logger = get_logger(__name__)
scheduler = BackgroundScheduler(timezone="Asia/Shanghai")


def setup_scheduler(run_crawl_callback) -> None:
    sources = get_config_bundle().sources.get("sources", [])
    for source in sources:
        if not source.get("enabled", True):
            continue
        cron = source.get("schedule", {}).get("cron")
        if not cron:
            continue
        minute, hour, day, month, day_of_week = cron.split()
        scheduler.add_job(
            run_crawl_callback,
            "cron",
            args=[source["id"]],
            id=f"crawl_{source['id']}",
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
            replace_existing=True,
        )
    if not scheduler.running:
        scheduler.start()
        logger.info("scheduler started with %s jobs", len(scheduler.get_jobs()))


def shutdown_scheduler() -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)
