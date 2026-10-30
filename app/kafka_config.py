"""librdkafka client configuration for the worker's consumer and DLQ producer.

Delivery semantics: at-least-once. Offsets are stored manually only after a
message is fully handled (decision committed to PostgreSQL, or dead-lettered and
acknowledged) and librdkafka auto-commits the *stored* offsets. Downstream writes
are idempotent (see :mod:`app.database`), so redelivery after a crash is harmless.

Security: ``PLAINTEXT`` (local only), ``SSL`` (TLS / mutual TLS), or
``SASL_PLAINTEXT``/``SASL_SSL`` with OAUTHBEARER client credentials, where
librdkafka fetches and refreshes tokens from ``KAFKA_OAUTH_TOKEN_URL`` itself.
"""

from typing import Any

from app.settings import Settings

_SUPPORTED_PROTOCOLS = frozenset({"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"})


def _security_config(settings: Settings) -> dict[str, Any]:
    protocol = settings.kafka_security_protocol.upper()
    if protocol not in _SUPPORTED_PROTOCOLS:
        raise RuntimeError(f"KAFKA_SECURITY_PROTOCOL must be one of {sorted(_SUPPORTED_PROTOCOLS)}")
    config: dict[str, Any] = {"security.protocol": protocol}

    if protocol in {"SSL", "SASL_SSL"}:
        tls_settings = {
            "ssl.ca.location": settings.kafka_ssl_ca_location,
            "ssl.certificate.location": settings.kafka_ssl_certificate_location,
            "ssl.key.location": settings.kafka_ssl_key_location,
            "ssl.key.password": settings.kafka_ssl_key_password,
        }
        config.update({name: value for name, value in tls_settings.items() if value})

    if protocol.startswith("SASL_"):
        missing = [
            name
            for name, value in (
                ("KAFKA_OAUTH_TOKEN_URL", settings.kafka_oauth_token_url),
                ("KAFKA_OAUTH_CLIENT_ID", settings.kafka_oauth_client_id),
                ("KAFKA_OAUTH_CLIENT_SECRET", settings.kafka_oauth_client_secret),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(f"{protocol} requires: {', '.join(missing)}")
        config.update(
            {
                "sasl.mechanisms": "OAUTHBEARER",
                "sasl.oauthbearer.method": "oidc",
                "sasl.oauthbearer.token.endpoint.url": settings.kafka_oauth_token_url,
                "sasl.oauthbearer.client.id": settings.kafka_oauth_client_id,
                "sasl.oauthbearer.client.secret": settings.kafka_oauth_client_secret,
            }
        )
        if settings.kafka_oauth_scope:
            config["sasl.oauthbearer.scope"] = settings.kafka_oauth_scope
    return config


def build_consumer_config(settings: Settings) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.kafka_consumer_group_id,
        "client.id": "fraud-worker",
        "enable.auto.commit": True,
        "enable.auto.offset.store": False,
        "auto.commit.interval.ms": 1000,
        "auto.offset.reset": "earliest",
        # Cooperative rebalancing: partitions that stay with this consumer keep
        # processing while others move.
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms": 45000,
        "max.poll.interval.ms": 300000,
        **_security_config(settings),
    }


def build_producer_config(settings: Settings) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": "fraud-worker-dlq",
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        "delivery.timeout.ms": 30000,
        **_security_config(settings),
    }
