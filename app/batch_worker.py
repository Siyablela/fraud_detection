"""Daily fraud-activity summary, run off the live path at a configured UTC time."""

import asyncio
import signal
import sys
from datetime import UTC, datetime, timedelta

from app.database import database_session_factory, ping_database, summarize_transactions
from app.observability import configure_logging, get_logger
from app.settings import Settings, get_settings

SERVICE_NAME = "fraud-batch-worker"
logger = get_logger(__name__)


def seconds_until_next_run(now: datetime, run_hour: int, run_minute: int) -> float:
    """Seconds from ``now`` until the next ``run_hour:run_minute`` UTC (always > 0)."""
    target = now.replace(hour=run_hour, minute=run_minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


async def run_summary(settings: Settings) -> dict:
    since = datetime.now(UTC) - timedelta(hours=settings.batch_summary_window_hours)
    async with database_session_factory(settings) as session_factory:
        await ping_database(session_factory, SERVICE_NAME)
        summary = await summarize_transactions(session_factory, since)
    logger.info(
        "batch_summary",
        window_hours=settings.batch_summary_window_hours,
        since=since.isoformat(),
        total_transactions=summary["total"],
        total_fraudulent=summary["fraud"],
        category_breakdown=summary["by_category"],
    )
    return summary


async def main(argv: list[str]) -> int:
    """``--once``: run the summary now and exit (for a Kubernetes CronJob).
    Otherwise stay resident and run daily at ``BATCH_RUN_HOUR:BATCH_RUN_MINUTE`` UTC."""
    settings = get_settings()
    configure_logging(SERVICE_NAME, settings.observability_log_level, settings.logs_redact_fields)
    if "--once" in argv:
        try:
            await run_summary(settings)
        except Exception:
            logger.exception("batch_summary_failed")
            return 1
        return 0
    if not settings.batch_job_enabled:
        logger.info("batch_worker_disabled")
        return 0

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    logger.info("batch_worker_started", run_hour=settings.batch_run_hour, run_minute=settings.batch_run_minute)
    while not stop.is_set():
        delay = seconds_until_next_run(datetime.now(UTC), settings.batch_run_hour, settings.batch_run_minute)
        logger.info("batch_worker_waiting", delay_seconds=round(delay))
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
            break
        except TimeoutError:
            pass
        try:
            await run_summary(settings)
        except Exception:
            # Keep the schedule alive; tomorrow's run may succeed. The failure is
            # visible in logs and in the fraud_database_up gauge.
            logger.exception("batch_summary_failed")
    logger.info("batch_worker_stopped")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1:])))
