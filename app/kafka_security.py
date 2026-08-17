from __future__ import annotations

from typing import Any

import httpx

from app.settings import get_settings


def _build_oauthbearer_config(security_protocol: str) -> dict[str, Any]:
    """Construct the SASL/OAUTHBEARER Kafka configuration for Keycloak-based auth.

    Args:
        security_protocol: The resolved SASL security protocol to use, either
            ``SASL_PLAINTEXT`` or ``SASL_SSL``.

    Returns:
        dict[str, Any]: Kafka client configuration parameters for OAUTHBEARER authentication.
    """
    settings = get_settings()
    issuer = settings.jwt_issuer.rstrip("/")

    def oauth_cb(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        """Fetch a service-account access token for SASL/OAUTHBEARER Kafka auth.

        Args:
            *_args: Ignored callback arguments supplied by the Kafka client.
            **_kwargs: Ignored keyword arguments supplied by the Kafka client.

        Returns:
            dict[str, Any]: A token payload with the access token, type, and expiry
                metadata required by the Kafka OAUTHBEARER callback.
        """
        response = httpx.post(
            f"{issuer}/protocol/openid-connect/token",
            data={
                "grant_type": "client_credentials",
                "client_id": settings.keycloak_service_client_id,
                "client_secret": settings.keycloak_service_client_secret,
                "scope": settings.keycloak_service_token_scope or "fraud:transactions:read",
            },
            timeout=10.0,
        )
        response.raise_for_status()
        payload = response.json()
        return {
            "token": payload.get("access_token", ""),
            "token_type": payload.get("token_type", "Bearer"),
            "expires_in": payload.get("expires_in", 300),
        }

    return {
        "security.protocol": security_protocol,
        "sasl.mechanisms": "OAUTHBEARER",
        "sasl.oauthbearer.token.endpoint.url": f"{issuer}/protocol/openid-connect/token",
        "sasl.oauthbearer.client.id": settings.keycloak_service_client_id,
        "sasl.oauthbearer.client.secret": settings.keycloak_service_client_secret,
        "sasl.oauthbearer.scope": settings.keycloak_service_token_scope or "fraud:transactions:read",
        "sasl.oauthbearer.extensions": "protocol=oauth2",
        "oauth_cb": oauth_cb,
    }


def build_kafka_client_config() -> dict[str, Any]:
    """Build the Kafka client configuration for the active security protocol.

    Returns:
        dict[str, Any]: Kafka configuration values for the active SASL/OAUTHBEARER or PLAINTEXT mode.
    """
    settings = get_settings()
    protocol = settings.kafka_security_protocol.upper()

    if protocol in {"SASL_PLAINTEXT", "SASL_SSL"}:
        return _build_oauthbearer_config(protocol)

    return {"security.protocol": "PLAINTEXT"}