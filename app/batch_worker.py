import asyncio
from datetime import datetime, timedelta, timezone

from app.observability import configure_logging, get_logger
from app.settings import get_settings

SERVICE_NAME = "fraud-batch-worker"
logger = get_logger(__name__)


def _seconds_until_next_run(run_hour: int, run_minute: int) -> int:
    """Return the number of seconds until the next configured batch run time."""
    current_time = datetime.now(timezone.utc)
    target = current_time.replace(hour=run_hour % 24, minute=run_minute % 60, second=0, microsecond=0)
    if target <= current_time:
        target += timedelta(days=1)
    return int((target - current_time).total_seconds())


async def process_batch_job() -> None:
    """Run the scheduled deferred-processing job.

    This is intentionally separate from the live transaction path. The batch worker
    is time-bound and designed for heavier deferred maintenance tasks such as
    recalculation, historical review, or summarization rather than low-latency
    request handling.
    """
    logger.info("batch_job_started", service=SERVICE_NAME)


async def main() -> None:
    """Run the scheduled batch worker at a configured time of day."""
    settings = get_settings()
    if not settings.batch_job_enabled:
        logger.info(
            "batch_worker_disabled",
            enabled=False,
            run_hour=settings.batch_run_hour,
            run_minute=settings.batch_run_minute,
        )
        return

    configure_logging(SERVICE_NAME, settings.observability_log_level)

    logger.info(
        "batch_worker_started",
        run_hour=settings.batch_run_hour,
        run_minute=settings.batch_run_minute,
    )

    while True:
        delay_seconds = _seconds_until_next_run(settings.batch_run_hour, settings.batch_run_minute)
        logger.info("batch_worker_waiting_until_next_run", delay_seconds=delay_seconds)
        await asyncio.sleep(delay_seconds)
        await process_batch_job()


if __name__ == "__main__":
    asyncio.run(main())
