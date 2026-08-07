from __future__ import annotations

import ssl
from functools import lru_cache
from typing import Any

from app.settings import get_settings


@lru_cache(maxsize=1)
def build_kafka_ssl_context() -> ssl.SSLContext:
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


def kafka_client_security_kwargs() -> dict[str, Any]:
    settings = get_settings()
    if settings.kafka_security_protocol.upper() != "SSL":
        return {"security_protocol": "PLAINTEXT"}

    return {
        "security_protocol": "SSL",
        "ssl_context": build_kafka_ssl_context(),
    }