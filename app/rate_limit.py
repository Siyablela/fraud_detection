
"""Per-principal token-bucket rate limiting for FastAPI."""

import asyncio
import base64
import json
import math
import time
from dataclasses import dataclass
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send


ANONYMOUS_PRINCIPAL = "anonymous"


@dataclass(slots=True)
class TokenBucket:
    """Mutable rate-limit state for one principal."""

    tokens: float
    last_refill: float
    last_seen: float


class PrincipalRateLimitMiddleware:
    """Apply an in-memory token bucket to a selected API path.

    The JWT is decoded without signature verification only to derive a
    bucket key. Authentication and authorization remain the responsibility
    of the application's existing security dependencies.

    This implementation coordinates requests within one middleware instance.
    It does not coordinate separate worker processes or application replicas.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        max_requests: int = 100,
        window_seconds: float = 60.0,
        burst_capacity: int | None = None,
        path_prefix: str = "/api/query",
        cleanup_interval: int = 1024,
        bucket_ttl_seconds: float = 3600.0,
    ) -> None:
        """Configure the token bucket and the paths it protects.

        Args:
            app: The downstream ASGI application.
            max_requests: Sustained refill allowance per time window.
            window_seconds: Duration used to calculate the refill rate.
            burst_capacity: Maximum tokens available to a principal.
                Defaults to max_requests.
            path_prefix: API path prefix to protect.
            cleanup_interval: Number of bucket checks between cleanup passes.
            bucket_ttl_seconds: Remove buckets not seen within this period.
        """
        if max_requests <= 0:
            raise ValueError("max_requests must be positive")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if burst_capacity is not None and burst_capacity <= 0:
            raise ValueError("burst_capacity must be positive")
        if cleanup_interval <= 0:
            raise ValueError("cleanup_interval must be positive")
        if bucket_ttl_seconds <= 0:
            raise ValueError("bucket_ttl_seconds must be positive")
        if not path_prefix.startswith("/"):
            raise ValueError("path_prefix must start with '/'")

        self.app = app
        self.capacity = float(
            burst_capacity if burst_capacity is not None else max_requests
        )
        self.refill_rate = max_requests / window_seconds
        self.path_prefix = path_prefix.rstrip("/") or "/"
        self.cleanup_interval = cleanup_interval
        self.bucket_ttl_seconds = bucket_ttl_seconds

        self._buckets: dict[str, TokenBucket] = {}
        self._lock = asyncio.Lock()
        self._checks_since_cleanup = 0

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        """Limit matching HTTP requests before passing them downstream."""
        if scope["type"] != "http" or not self._is_protected_path(
            scope.get("path", "")
        ):
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        principal = self._get_principal(request)

        allowed, retry_after = await self._consume_token(principal)

        if not allowed:
            response = JSONResponse(
                status_code=429,
                content={
                    "detail": "Rate limit exceeded. Try again later."
                },
                headers={
                    "Retry-After": str(retry_after),
                    "Cache-Control": "no-store",
                },
            )
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)

    def _is_protected_path(self, path: str) -> bool:
        """Match the configured prefix without matching similarly named paths."""
        if self.path_prefix == "/":
            return True
        return (
            path == self.path_prefix
            or path.startswith(self.path_prefix + "/")
        )

    @staticmethod
    def _get_principal(request: Request) -> str:
        """Derive a bucket key from unverified JWT claims.

        Prefer sub, then azp, then client_id. Missing, malformed, or
        unsupported credentials share the anonymous bucket.

        These claims are untrusted here. Never use this method to authenticate
        a request or grant access to a protected resource.
        """
        authorization = request.headers.get("authorization", "")
        scheme, separator, token = authorization.partition(" ")

        if not separator or scheme.lower() != "bearer" or not token.strip():
            return ANONYMOUS_PRINCIPAL

        claims = PrincipalRateLimitMiddleware._decode_unverified_jwt(
            token.strip()
        )
        if claims is None:
            return ANONYMOUS_PRINCIPAL

        for claim_name in ("sub", "azp", "client_id"):
            value = claims.get(claim_name)

            # Bound the key size and accept only non-empty string claims.
            if isinstance(value, str) and 0 < len(value) <= 256:
                return f"{claim_name}:{value}"

        return ANONYMOUS_PRINCIPAL

    @staticmethod
    def _decode_unverified_jwt(token: str) -> dict[str, Any] | None:
        """Decode a JWT payload without verifying its signature.

        This is deliberately not an authentication function. The normal
        authentication dependency must independently validate the token.
        """
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return None

            payload = parts[1]
            # Reject unusually large payloads before allocating decoded data.
            if len(payload) > 16_384:
                return None

            padded_payload = payload + "=" * (-len(payload) % 4)
            decoded = base64.b64decode(
                padded_payload,
                altchars=b"-_",
                validate=True,
            )
            claims = json.loads(decoded)

            return claims if isinstance(claims, dict) else None
        except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError):
            return None

    async def _consume_token(self, principal: str) -> tuple[bool, int]:
        """Atomically refill a principal's bucket and consume one token.

        Returns:
            A tuple containing whether the request is allowed and the
            Retry-After value in whole seconds. The latter is zero when
            the request is allowed.
        """
        now = time.monotonic()

        async with self._lock:
            self._checks_since_cleanup += 1

            bucket = self._buckets.get(principal)
            if bucket is None:
                bucket = TokenBucket(
                    tokens=self.capacity,
                    last_refill=now,
                    last_seen=now,
                )
                self._buckets[principal] = bucket
            else:
                elapsed = max(0.0, now - bucket.last_refill)
                bucket.tokens = min(
                    self.capacity,
                    bucket.tokens + elapsed * self.refill_rate,
                )
                bucket.last_refill = now
                bucket.last_seen = now

            if self._checks_since_cleanup >= self.cleanup_interval:
                self._cleanup_stale_buckets(now)
                self._checks_since_cleanup = 0

            if bucket.tokens >= 1.0:
                bucket.tokens -= 1.0
                return True, 0

            missing_tokens = 1.0 - bucket.tokens
            retry_after = math.ceil(missing_tokens / self.refill_rate)
            return False, max(1, retry_after)

    def _cleanup_stale_buckets(self, now: float) -> None:
        """Remove inactive buckets to bound retained state over time.

        Must be called while holding self._lock.
        """
        cutoff = now - self.bucket_ttl_seconds
        stale_principals = [
            principal
            for principal, bucket in self._buckets.items()
            if bucket.last_seen < cutoff
        ]

        for principal in stale_principals:
            del self._buckets[principal]
