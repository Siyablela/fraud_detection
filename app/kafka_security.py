from __future__ import annotations

import ssl
from functools import lru_cache
from typing import Any

import httpx

from app.settings import get_settings


@lru_cache(maxsize=1)
def build_kafka_ssl_context() -> ssl.SSLContext:
    """Create and validate the Kafka SSL context using the configured keystore and truststore.

    Returns:
        ssl.SSLContext: An SSL context configured for Kafka client authentication.

    Raises:
        RuntimeError: If the Kafka SSL protocol is not enabled or required certificate files are missing.
    """
    settings = get_settings()
    if settings.kafka_security_protocol.upper() != "SSL":
        raise RuntimeError("Kafka SSL context requested but KAFKA_SECURITY_PROTOCOL is not SSL.")

    if not settings.kafka_ssl_truststore_path:
        raise RuntimeError("KAFKA_SSL_TRUSTSTORE_PATH is required when Kafka SSL is enabled.")
    if not settings.kafka_ssl_keystore_cert_path:
        raise RuntimeError("KAFKA_SSL_KEYSTORE_CERT_PATH is required when Kafka SSL is enabled.")
    if not settings.kafka_ssl_keystore_key_path:
        raise RuntimeError("KAFKA_SSL_KEYSTORE_KEY_PATH is required when Kafka SSL is enabled.")

    context = ssl.create_default_context(cafile=settings.kafka_ssl_truststore_path)
    context.load_cert_chain(
        certfile=settings.kafka_ssl_keystore_cert_path,
        keyfile=settings.kafka_ssl_keystore_key_path,
        password=settings.kafka_ssl_keystore_password or None,
    )
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    return context


def _build_oauthbearer_config() -> dict[str, Any]:
    """Construct the SASL/OAUTHBEARER Kafka configuration for Keycloak-based auth.

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
        "security.protocol": "SASL_PLAINTEXT",
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
        dict[str, Any]: Kafka configuration values for the configured TLS or SASL mode.
    """
    settings = get_settings()
    protocol = settings.kafka_security_protocol.upper()
    if protocol == "SSL":
        return {
            "security.protocol": "SSL",
            "ssl.ca.location": settings.kafka_ssl_truststore_path,
            "ssl.certificate.location": settings.kafka_ssl_keystore_cert_path,
            "ssl.key.location": settings.kafka_ssl_keystore_key_path,
            "ssl.key.password": settings.kafka_ssl_keystore_password,
        }

    if protocol in {"SASL_PLAINTEXT", "SASL_SSL"}:
        return _build_oauthbearer_config()

    return {"security.protocol": "PLAINTEXT"}