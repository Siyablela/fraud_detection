"""Per-caller API rate limiting backed by Redis.

Keyed on the *verified* token subject, so a caller cannot dodge the limit by
minting tokens with fresh ``sub`` claims, and counters live in Redis so the limit
holds across API replicas and never grows process memory.

Fixed-window counters: one ``INCR`` + ``EXPIRE`` round trip per request. A
caller can briefly reach 2x the limit across a window boundary; that is accepted
for an abuse guard (it is not a billing meter).

If Redis is unavailable the check fails open: rejecting every read because the
limiter's own store is down would turn a cache outage into an API outage. Each
fail-open is counted in ``fraud_rate_limiter_errors_total`` for alerting.
"""

import hashlib
import math
import time
from collections.abc import Callable

import redis.asyncio as aioredis
from fastapi import HTTPException, status

from app.observability import get_logger, record_rate_limiter_error

logger = get_logger(__name__)


class RateLimiter:
    def __init__(
        self,
        redis_client: aioredis.Redis,
        max_requests: int,
        window_seconds: int,
        clock: Callable[[], float] = time.time,
    ):
        self._redis = redis_client
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._clock = clock

    def _bucket_key(self, subject: str, window: int) -> str:
        # Hash the subject: keys stay fixed-length and never carry raw identifiers.
        digest = hashlib.sha256(subject.encode("utf-8")).hexdigest()[:32]
        return f"fraud:ratelimit:{digest}:{window}"

    async def enforce(self, subject: str) -> None:
        """Count one request for ``subject``.

        Raises:
            HTTPException: 429 with ``Retry-After`` when the window budget is spent.
        """
        now = self._clock()
        window = int(now // self._window_seconds)
        key = self._bucket_key(subject, window)
        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.incr(key)
                pipe.expire(key, self._window_seconds + 1)
                count, _ = await pipe.execute()
        except Exception as exc:
            record_rate_limiter_error()
            logger.warning("rate_limiter_unavailable_failing_open", error_type=type(exc).__name__)
            return
        if count > self._max_requests:
            retry_after = max(1, math.ceil((window + 1) * self._window_seconds - now))
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded. Retry after the indicated number of seconds.",
                headers={"Retry-After": str(retry_after)},
            )
