from __future__ import annotations
import math
import time
from collections.abc import Awaitable, Callable, Iterable

import jwt 
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

# Paths that must never be rate limited (health/observability probes).
DEFAULT_EXEMPT_PATHS = frozenset(("/live", "/ready", "/health", "/metrics"))

def _unverified_principal(token: str) -> str | None:
    """Read a stable principal id from a bearer token without verifying it. 
    Used only for rate-limit bucketing, never for authentication. 
    Prefers the 'sub' claim and falls back to the authorized-party/client 
    identifiers used by client-credentials tokens.
    """
    try:
        claims = jwt.decode(token, options={"verify_signature": False})
    except jwt.PyJWTError:
        return None
        
    for claim in ("sub", "azp", "client_id"):
        value = claims.get(claim)
        if value:
            return str(value)
    return None

#: Shared bucket for all requests that carry no resolvable principal.
ANONYMOUS_KEY = "anon"

def principal_key(request: Request) -> str:
    """Resolve a rate-limit bucket key from the caller's identity (never the IP). 
    Returns "sub:<principal>" for a bearer-token request, or a single shared 
    'anon' bucket for tokenless requests. The "sub:" namespace prevents an 
    authenticated principal from ever colliding with the anonymous bucket.
    """
    authorization = request.headers.get("authorization", "")
    if authorization.lower().startswith("bearer "):
        principal = _unverified_principal(authorization[7:].strip())
        if principal:
            return f"sub:{principal}"
    return ANONYMOUS_KEY

class RateLimitMiddleware(BaseHTTPMiddleware):
    """Token-bucket rate limiter keyed by caller identity.
    
    Args:
        app: The wrapped ASGI application.
        max_requests: Sustained request budget per "window_seconds" (sets the continuous refill rate).
        window_seconds: Window the sustained rate is expressed over.
        burst: Bucket capacity - the largest instantaneous burst allowed. Defaults to "max_requests" when not set ("None" or "0").
        exempt_paths: Request paths that bypass rate limiting entirely.
        key_func: Resolves the bucket key from a request (defaults to principal keying, with a shared anonymous bucket for tokenless calls).
        clock: Injectable monotonic clock (seconds) for deterministic testing.
    """
    def __init__(
        self, 
        app, 
        max_requests: int, 
        window_seconds: int, 
        burst: int | None = None, 
        exempt_paths: Iterable[str] = DEFAULT_EXEMPT_PATHS, 
        key_func: Callable[[Request], str] = principal_key, 
        clock: Callable[[], float] = time.monotonic
    ) -> None:
        super().__init__(app)
        self._refill_rate = max_requests / window_seconds  # tokens per second
        self._capacity = float(burst) if burst and burst > 0 else float(max_requests)
        self._exempt_paths = frozenset(exempt_paths)
        self._key_func = key_func
        self._clock = clock
        
        # key -> (available_tokens, last_refill_time)
        self._buckets: dict[str, tuple[float, float]] = {}

    def _consume(self, key: str) -> tuple[bool, int]:
        """Refill the bucket for "key" and try to take one token. 
        Returns "(allowed, retry_after_seconds)". 
        "retry_after_seconds" is the whole-second wait until a token is available when the request is denied.
        """
        now = self._clock()
        tokens, last_refill = self._buckets.get(key, (self._capacity, now))
        
        tokens = min(self._capacity, tokens + (now - last_refill) * self._refill_rate)
        
        if tokens >= 1.0:
            self._buckets[key] = (tokens - 1.0, now)
            return True, 0
            
        self._buckets[key] = (tokens, now)
        deficit = 1.0 - tokens
        retry_after = math.ceil(deficit / self._refill_rate) if self._refill_rate > 0 else 1
        return False, max(1, retry_after)

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """Enforce the per-principal token budget, exempting probe endpoints."""
        if request.url.path in self._exempt_paths:
            return await call_next(request)
            
        allowed, retry_after = self._consume(self._key_func(request))
        if not allowed:
            return JSONResponse(
                status_code=429, 
                content={"detail": "Rate limit exceeded. Try again later."}, 
                headers={"Retry-After": str(retry_after)}
            )
            
        return await call_next(request)