from __future__ import annotations

import os
from dataclasses import dataclass

import httpx

from app.observability import get_logger
from app.settings import get_settings

logger = get_logger(__name__)


class TokenExchangeError(Exception):
    """Base error for token exchange failures."""


class InvalidCredentialsError(TokenExchangeError):
    """Raised when the identity provider rejects username/password credentials."""


class InvalidClientCredentialsError(TokenExchangeError):
    """Raised when the identity provider rejects client credentials."""


class IdentityProviderUnavailableError(TokenExchangeError):
    """Raised when the identity provider cannot be reached."""


class IdentityProviderRejectedError(TokenExchangeError):
    """Raised when the identity provider rejects a token request."""


@dataclass(frozen=True)
class TokenExchangeResult:
    access_token: str
    token_type: str
    expires_in: int | None = None
    refresh_token: str | None = None
    refresh_expires_in: int | None = None
    scope: str | None = None


class KeycloakTokenService:
    def __init__(self, timeout_seconds: float = 10.0):
        """Create a Keycloak token service client.

        Args:
            timeout_seconds: HTTP timeout used for token requests to the identity provider.
        """
        self._timeout_seconds = timeout_seconds

    @staticmethod
    def _token_url() -> str:
        """Build the Keycloak token endpoint URL based on the configured issuer.

        Returns:
            str: The full Keycloak token endpoint URL.
        """
        issuer = get_settings().jwt_issuer.rstrip("/")
        return f"{issuer}/protocol/openid-connect/token"

    @staticmethod
    def _request_body(
        username: str,
        password: str,
        client_id: str | None,
        scope: str | None,
    ) -> dict[str, str]:
        """Build the OAuth form body for a password grant token exchange.

        Args:
            username: Username for the user credentials grant.
            password: Password for the user credentials grant.
            client_id: Optional override for the client ID used in the grant.
            scope: Optional scope string to request from Keycloak.

        Returns:
            dict[str, str]: OAuth form parameters for the password grant request.
        """
        settings = get_settings()
        body = {
            "grant_type": "password",
            "client_id": (client_id or settings.keycloak_token_client_id).strip(),
            "username": username,
            "password": password,
        }
        if scope and scope.strip():
            body["scope"] = scope.strip()
        client_secret = os.getenv("KEYCLOAK_TOKEN_CLIENT_SECRET")
        if client_secret is None:
            client_secret = settings.keycloak_token_client_secret
        if isinstance(client_secret, str) and client_secret.strip():
            body["client_secret"] = client_secret.strip()
        return body

    async def exchange_token(
        self,
        *,
        username: str,
        password: str,
        client_id: str | None,
        scope: str | None,
    ) -> TokenExchangeResult:
        """Exchange a username and password for a user access token.

        Args:
            username: The user’s Keycloak username.
            password: The user’s Keycloak password.
            client_id: Optional override of the OAuth client ID.
            scope: Optional requested token scope.

        Returns:
            TokenExchangeResult: The normalized access token response payload.
        """
        body = self._request_body(username, password, client_id, scope)

        return await self._exchange_with_body(body, invalid_error="invalid_grant")

    async def exchange_service_token(
        self,
        *,
        client_id: str | None,
        client_secret: str | None,
        scope: str | None,
    ) -> TokenExchangeResult:
        """Exchange service account credentials for a client-credentials access token.

        Args:
            client_id: Optional client ID override for the service account.
            client_secret: Optional client secret override for the service account.
            scope: Optional requested scope for the exchanged token.

        Returns:
            TokenExchangeResult: The normalized token response for the service account.
        """
        settings = get_settings()
        resolved_client_id = (client_id or settings.keycloak_service_client_id).strip()
        env_client_secret = os.getenv("KEYCLOAK_SERVICE_CLIENT_SECRET")
        resolved_client_secret = (client_secret or env_client_secret or settings.keycloak_service_client_secret).strip()

        body = {
            "grant_type": "client_credentials",
            "client_id": resolved_client_id,
            "client_secret": resolved_client_secret,
        }
        if scope and scope.strip():
            body["scope"] = scope.strip()

        return await self._exchange_with_body(body, invalid_error="invalid_client")

    async def _exchange_with_body(
        self,
        body: dict[str, str],
        *,
        invalid_error: str,
    ) -> TokenExchangeResult:
        """Handle the common Keycloak token exchange flow and normalize the result.

        Args:
            body: OAuth form payload for the token request.
            invalid_error: Expected error string used to distinguish invalid-grant and invalid-client failures.

        Returns:
            TokenExchangeResult: Parsed token response returned by Keycloak.

        Raises:
            IdentityProviderUnavailableError: If the Keycloak endpoint is unreachable.
            InvalidCredentialsError: If the credentials grant is rejected.
            InvalidClientCredentialsError: If the client credentials flow is rejected.
            IdentityProviderRejectedError: If the provider rejects the token request for another reason.
        """
        token_url = self._token_url()

        try:
            async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
                response = await client.post(
                    token_url,
                    data=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
        except httpx.HTTPError as exc:
            logger.exception("token_exchange_unavailable", token_url=token_url)
            raise IdentityProviderUnavailableError() from exc

        if response.status_code >= 400:
            error_payload: dict[str, str] = {}
            try:
                raw = response.json()
                if isinstance(raw, dict):
                    error_payload = {str(k): str(v) for k, v in raw.items()}
            except ValueError:
                error_payload = {}

            if error_payload.get("error") == "invalid_grant":
                raise InvalidCredentialsError()
            if error_payload.get("error") == invalid_error:
                raise InvalidClientCredentialsError()

            logger.warning(
                "token_exchange_failed",
                status_code=response.status_code,
                detail=error_payload or response.text,
            )
            raise IdentityProviderRejectedError()

        payload = response.json()
        return TokenExchangeResult(
            access_token=payload["access_token"],
            token_type=str(payload.get("token_type", "Bearer")),
            expires_in=payload.get("expires_in"),
            refresh_token=payload.get("refresh_token"),
            refresh_expires_in=payload.get("refresh_expires_in"),
            scope=payload.get("scope"),
        )
