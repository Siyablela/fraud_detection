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
                "scope": settings.keycloak_service_token_scope
                or "fraud:transactions:read",
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
        "sasl.oauthbearer.scope": settings.keycloak_service_token_scope
        or "fraud:transactions:read",
        "sasl.oauthbearer.extensions": "protocol=oauth2",
        "oauth_cb": oauth_cb,
    }


def _build_tls_config() -> dict[str, Any]:
    """Build librdkafka TLS/mTLS options from any configured certificate material.

    Only keys for values that are actually set are returned, so an operator can
    enale one-way TLS (CA Only) or mutual TLS (CA + Client cert/key) without the
    unset optiond intefering.

    Returns:
        dict[str, Any]: ``ssl.*`` configuration entries for the kafka client.
    """
    settings = get_settings()
    tls_config: dict[str, Any] = {}
    if settings.kafka_ssl_ca_location:
        tls_config["ssl.ca.location"] = settings.kafka_ssl_ca_location
    if settings.kafka_ssl_certificate_location:
        tls_config["ssl.certificate.location"] = settings.kafka_ssl_certificate_location
    if settings.kafka_ssl_key_location:
        tls_config["ssl.key.location"] = settings.kafka_ssl_key_location
    if settings.kafka_ssl_key_password:
        tls_config["ssl.key.password"] = settings.kafka_ssl_key_password
    return tls_config


def build_kafka_client_config() -> dict[str, Any]:
    """Build the Kafka client configuration for the active security protocol.

    Supports ``PLAINTEXT``, ``SSL`` (mutual TLS, no SASL), and the SASL variants
    ```SASL_PLAINTEXT`` and ``SASL_SSL`` with OAUTHBEARER authentication. TLS material layered
    is whenever an SSL-bearing protocol is selected abd certificate paths are set.

    Returns:
        dict[str, Any]: Kafka configuration values for the security mode.
    """
    settings = get_settings()
    protocol = settings.kafka_security_protocol.upper()

    if protocol in {"SASL_PLAINTEXT", "SASL_SSL"}:
        config = _build_oauthbearer_config(protocol)
        if protocol == "SASL_SSL":
            config.update(_build_tls_config())
        return config

    if protocol == "SSL":
        return {"security.protocol": "SSL", **_build_tls_config()}

    return {"security.protocol": "PLAINTEXT"}
