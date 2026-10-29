"""Bearer-token authentication and scope-based authorisation.

Authorisation model: callers are back-office services and fraud analysts, not the
cardholders whose transactions are stored, so access is granted per capability
(OAuth scope) rather than per record owner. See README "Security model".
"""

import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import httpx
import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.concurrency import run_in_threadpool

from app.settings import Settings, get_settings

bearer_scheme = HTTPBearer(auto_error=False, description="OAuth 2.0 access token issued by Keycloak")

_REQUIRED_CLAIMS = ["exp", "iat", "sub", "iss", "aud"]


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    subject: str
    scopes: frozenset[str]
    claims: dict[str, Any]


class _TokenVerifier(ABC):
    def __init__(self, settings: Settings):
        self._settings = settings

    @abstractmethod
    def _signing_key(self, token: str) -> Any: ...

    def decode(self, token: str) -> dict[str, Any]:
        """Verify signature, issuer, audience and expiry; return the claims.

        Raises:
            jwt.InvalidTokenError: For any verification failure.
        """
        return jwt.decode(
            token,
            self._signing_key(token),
            algorithms=self._settings.jwt_algorithms,
            audience=self._settings.jwt_audience,
            issuer=self._settings.jwt_issuer,
            leeway=self._settings.jwt_leeway_seconds,
            options={"require": _REQUIRED_CLAIMS},
        )


class _JwksTokenVerifier(_TokenVerifier):
    """Resolve signing keys from the issuer's JWKS endpoint.

    The key set is fetched once and re-fetched when a token names an unknown key
    id (key rotation), but at most once per ``JWT_JWKS_MIN_REFRESH_INTERVAL_SECONDS``.
    Without that limit, a stream of tokens with random ``kid`` values would turn
    every request into a call to the identity provider.
    """

    def __init__(self, settings: Settings, fetch: Callable[[str], dict[str, Any]] | None = None):
        super().__init__(settings)
        self._url = settings.jwt_jwks_url
        self._min_interval = settings.jwt_jwks_min_refresh_interval_seconds
        self._fetch = fetch or self._http_fetch
        self._keys: dict[str, Any] = {}
        self._last_fetch = float("-inf")
        self._lock = threading.Lock()

    @staticmethod
    def _http_fetch(url: str) -> dict[str, Any]:
        response = httpx.get(url, timeout=5.0)
        response.raise_for_status()
        return response.json()

    def _refresh(self) -> None:
        jwk_set = jwt.PyJWKSet.from_dict(self._fetch(self._url))
        self._keys = {key.key_id: key.key for key in jwk_set.keys if key.key_id}
        self._last_fetch = time.monotonic()

    def _signing_key(self, token: str) -> Any:
        kid = jwt.get_unverified_header(token).get("kid")
        if not kid:
            raise jwt.InvalidTokenError("token header has no 'kid'")
        key = self._keys.get(kid)
        if key is not None:
            return key
        with self._lock:
            key = self._keys.get(kid)
            if key is None and time.monotonic() - self._last_fetch >= self._min_interval:
                try:
                    self._refresh()
                except (httpx.HTTPError, ValueError, jwt.PyJWKError) as exc:
                    raise jwt.InvalidTokenError("signing keys are unavailable") from exc
                key = self._keys.get(kid)
        if key is None:
            raise jwt.InvalidTokenError("token is signed with an unknown key")
        return key


class _PublicKeyTokenVerifier(_TokenVerifier):
    def __init__(self, settings: Settings):
        super().__init__(settings)
        self._public_key = Path(settings.jwt_public_key_path).read_text(encoding="utf-8")

    def _signing_key(self, token: str) -> Any:
        return self._public_key


@lru_cache(maxsize=1)
def get_token_verifier() -> _TokenVerifier:
    """Return the process-wide verifier configured by ``JWT_JWKS_URL`` or ``JWT_PUBLIC_KEY_PATH``.

    Raises:
        RuntimeError: If neither is configured; the API refuses to start.
    """
    settings = get_settings()
    if settings.jwt_jwks_url:
        return _JwksTokenVerifier(settings)
    if settings.jwt_public_key_path:
        return _PublicKeyTokenVerifier(settings)
    raise RuntimeError("JWT verification is not configured. Set JWT_JWKS_URL or JWT_PUBLIC_KEY_PATH.")


def _parse_scopes(claims: dict[str, Any]) -> frozenset[str]:
    scope_value = claims.get("scope") or claims.get("scp") or ""
    if isinstance(scope_value, str):
        return frozenset(part for part in scope_value.split() if part)
    if isinstance(scope_value, list):
        return frozenset(str(part) for part in scope_value if str(part))
    return frozenset()


def _unauthorized(detail: str, error: str | None = None) -> HTTPException:
    challenge = 'Bearer realm="fraud-detection"' + (f', error="{error}"' if error else "")
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail=detail, headers={"WWW-Authenticate": challenge}
    )


async def get_current_principal(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> AuthenticatedPrincipal:
    """Authenticate the caller from the ``Authorization: Bearer`` header.

    Raises:
        HTTPException: 401 when the token is missing, invalid, or expired.
    """
    if credentials is None or credentials.scheme.lower() != "bearer" or not credentials.credentials.strip():
        raise _unauthorized("Missing bearer access token.")
    token = credentials.credentials.strip()
    try:
        # Verification may fetch the JWKS over HTTP; keep that off the event loop.
        claims = await run_in_threadpool(get_token_verifier().decode, token)
    except jwt.ExpiredSignatureError as exc:
        raise _unauthorized("Access token has expired.", "invalid_token") from exc
    except jwt.InvalidTokenError as exc:
        raise _unauthorized("Access token is invalid.", "invalid_token") from exc

    subject = str(claims.get("sub") or "").strip()
    if not subject:
        raise _unauthorized("Access token is missing the subject claim.", "invalid_token")
    return AuthenticatedPrincipal(subject=subject, scopes=_parse_scopes(claims), claims=claims)


def require_scope(scope_name: str) -> Callable[..., Any]:
    """Build a dependency that authenticates, checks one scope, and applies rate limiting.

    Args:
        scope_name: Name of a ``Settings`` attribute holding the scope, e.g.
            ``"scope_transactions_read"``; resolved per request so tests and
            deployments can rename scopes through configuration.
    """

    async def dependency(
        request: Request, principal: AuthenticatedPrincipal = Depends(get_current_principal)
    ) -> AuthenticatedPrincipal:
        required = getattr(get_settings(), scope_name)
        if required not in principal.scopes:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access token does not include the required scope '{required}'.",
                headers={"WWW-Authenticate": f'Bearer error="insufficient_scope", scope="{required}"'},
            )
        limiter = getattr(request.app.state, "rate_limiter", None)
        if limiter is not None:
            await limiter.enforce(principal.subject)
        return principal

    return dependency
