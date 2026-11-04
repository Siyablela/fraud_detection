"""librdkafka client configuration for the worker's consumer and DLQ producer.

Delivery semantics: at-least-once. Offsets are stored manually only after a
message is fully handled (decision committed to PostgreSQL, or dead-lettered and
acknowledged) and librdkafka auto-commits the *stored* offsets. Downstream writes
are idempotent (see :mod:`app.database`), so redelivery after a crash is harmless.

Security: ``PLAINTEXT`` (local only), ``SSL`` (TLS / mutual TLS), or ``SASL_SSL``
with OAUTHBEARER clien credentials, where librdkafka fetches and refreshes tokens
from ``KAFKA_OAUTH_TOKEN_URL`` itself. ``SASL_PLAINTEXT`` is deliberately not
supported: it would send those credentials over an unencrypted connection.
"""

from typing import Any

from app.settings import Settings

_SUPPORTED_PROTOCOLS = frozenset({"PLAINTEXT", "SSL", "SASL_SSL"})


def _security_config(settings: Settings) -> dict[str, Any]:
    """Connection security shared by the consumer and the DLQ producer (same brokers).

    Raises:
        RuntimeError: Unknown protocol, or SASL_SSL without its OAuth settings.
    """
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

    if protocol == "SASL_SSL":
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
    """librdkafka settings for the worker's consumer (at-least-once, manual offset store)."""
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.kafka_consumer_group_id,
        "client.id": "fraud-worker",
        # The pair that gives at-least-once: librdkafka commits in the background, but
        # only offsets the worker has explicitly stored, which it does after a message
        # is fully handled. A polled-but-unfinished message is never committed.
        "enable.auto.commit": True,
        "enable.auto.offset.store": False,
        "auto.commit.interval.ms": 1000,
        # A brand-new consumer group starts from the oldest retained event, not the
        # newest, so nothing published before the first deploy is skipped.
        "auto.offset.reset": "earliest",
        # Cooperative rebalancing: partitions that stay with this consumer keep
        # processing while others move.
        "partition.assignment.strategy": "cooperative-sticky",
        # No heartbeat for 45 s: the broker treats this consumer as dead and moves its partitions.
        "session.timeout.ms": 45000,
        # poll() must be called at least this often or the consumer is evicted; the
        # worker's backpressure pauses partitions instead of ever blocking the poll loop.
        "max.poll.interval.ms": 300000,
        **_security_config(settings),
    }


def build_producer_config(settings: Settings) -> dict[str, Any]:
    """librdkafka settings for the dead-letter producer."""
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": "fraud-worker-dlq",
        # A dead letter is acknowledged only once every in-sync replica has it, and
        # librdkafka's internal retries cannot write it twice.
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        "delivery.timeout.ms": 30000,  # matches DeadLetterPublisher's wait for the acknowledgement
        **_security_config(settings),
    }
