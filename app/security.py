from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWKClient

from app.settings import get_settings


bearer_scheme = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    subject: str
    scopes: frozenset[str]
    claims: dict[str, Any]


class _TokenVerifier:
    def decode(self, token: str) -> dict[str, Any]:
        """Decode and validate a JWT using the configured verifier strategy.

        Args:
            token: The bearer token string to validate.

        Returns:
            dict[str, Any]: The decoded JWT payload.
        """
        raise NotImplementedError


class _JwksTokenVerifier(_TokenVerifier):
    def __init__(self, jwks_url: str):
        """Create a verifier backed by a JWKS endpoint.

        Args:
            jwks_url: The remote JWKS URL used to resolve signing keys.
        """
        self._jwks_client = PyJWKClient(jwks_url)

    def decode(self, token: str) -> dict[str, Any]:
        """Validate the token against the configured issuer, audience, and signing key.

        Args:
            token: A JWT string supplied in the Authorization header.

        Returns:
            dict[str, Any]: The verified payload from the JWT.
        """
        settings = get_settings()
        signing_key = self._jwks_client.get_signing_key_from_jwt(token)
        return jwt.decode(
            token,
            signing_key.key,
            algorithms=settings.jwt_algorithms,
            audience=settings.jwt_audience,
            issuer=settings.jwt_issuer,
            options={"require": ["exp", "sub", "iss", "aud"]},
        )


class _PublicKeyTokenVerifier(_TokenVerifier):
    def __init__(self, public_key_path: str):
        """Create a verifier backed by a PEM-formatted public key file.

        Args:
            public_key_path: Path to the PEM public key used to validate tokens.
        """
        self._public_key = Path(public_key_path).read_text(encoding="utf-8")

    def decode(self, token: str) -> dict[str, Any]:
        """Validate the token using the fixed public key from disk.

        Args:
            token: A JWT string supplied by the caller.

        Returns:
            dict[str, Any]: The validated JWT payload.
        """
        settings = get_settings()
        return jwt.decode(
            token,
            self._public_key,
            algorithms=settings.jwt_algorithms,
            audience=settings.jwt_audience,
            issuer=settings.jwt_issuer,
            options={"require": ["exp", "sub", "iss", "aud"]},
        )


@lru_cache(maxsize=1)
def _get_token_verifier() -> _TokenVerifier:
    """Return a cached token verifier matching the application's configured JWT source.

    Returns:
        _TokenVerifier: A verifier instance backed by a JWKS URL or PEM public key.

    Raises:
        RuntimeError: If no JWT verification configuration is available.
    """
    settings = get_settings()
    if settings.jwt_jwks_url:
        return _JwksTokenVerifier(settings.jwt_jwks_url)
    if settings.jwt_public_key_path:
        return _PublicKeyTokenVerifier(settings.jwt_public_key_path)
    raise RuntimeError(
        "JWT verification is not configured. Set JWT_JWKS_URL or JWT_PUBLIC_KEY_PATH."
    )


def _parse_scopes(claims: dict[str, Any]) -> frozenset[str]:
    """Normalize JWT scope data from string or list-shaped claims into a set.

    Args:
        claims: The token claims dictionary from the token verifier.

    Returns:
        frozenset[str]: A normalized set of granted scope values.
    """
    scope_value = claims.get("scope") or claims.get("scp") or ""
    if isinstance(scope_value, str):
        return frozenset(part for part in scope_value.split() if part)
    if isinstance(scope_value, list):
        return frozenset(str(part) for part in scope_value if str(part))
    return frozenset()


async def get_current_principal(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> AuthenticatedPrincipal:
    """Validate a bearer token and return the authenticated principal metadata.

    Args:
        credentials: The parsed bearer authentication details from FastAPI dependency injection.

    Returns:
        AuthenticatedPrincipal: The verified principal with subject, scopes, and raw claims.

    Raises:
        HTTPException: If the token is missing, invalid, expired, or lacks a subject claim.
    """
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer access token.",
            headers={"WWW-Authenticate": 'Bearer realm="fraud-detection"'},
        )

    token = credentials.credentials.strip()
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer access token.",
            headers={"WWW-Authenticate": 'Bearer realm="fraud-detection"'},
        )

    try:
        claims = _get_token_verifier().decode(token)
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token has expired.",
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        ) from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token is invalid.",
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        ) from exc

    subject = str(claims.get("sub") or "").strip()
    if not subject:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token is missing the subject claim.",
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        )

    return AuthenticatedPrincipal(
        subject=subject,
        scopes=_parse_scopes(claims),
        claims=claims,
    )
