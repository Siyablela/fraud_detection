from __future__ import annotations

import ssl
from functools import lru_cache
from typing import Any

from app.settings import (
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SSL_KEYSTORE_CERT_PATH,
    KAFKA_SSL_KEYSTORE_KEY_PATH,
    KAFKA_SSL_KEYSTORE_PASSWORD,
    KAFKA_SSL_TRUSTSTORE_PATH,
)


@lru_cache(maxsize=1)
def build_kafka_ssl_context() -> ssl.SSLContext:
    if KAFKA_SECURITY_PROTOCOL.upper() != "SSL":
        raise RuntimeError("Kafka SSL context requested but KAFKA_SECURITY_PROTOCOL is not SSL.")

    if not KAFKA_SSL_TRUSTSTORE_PATH:
        raise RuntimeError("KAFKA_SSL_TRUSTSTORE_PATH is required when Kafka SSL is enabled.")
    if not KAFKA_SSL_KEYSTORE_CERT_PATH:
        raise RuntimeError("KAFKA_SSL_KEYSTORE_CERT_PATH is required when Kafka SSL is enabled.")
    if not KAFKA_SSL_KEYSTORE_KEY_PATH:
        raise RuntimeError("KAFKA_SSL_KEYSTORE_KEY_PATH is required when Kafka SSL is enabled.")

    context = ssl.create_default_context(cafile=KAFKA_SSL_TRUSTSTORE_PATH)
    context.load_cert_chain(
        certfile=KAFKA_SSL_KEYSTORE_CERT_PATH,
        keyfile=KAFKA_SSL_KEYSTORE_KEY_PATH,
        password=KAFKA_SSL_KEYSTORE_PASSWORD or None,
    )
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    return context


def kafka_client_security_kwargs() -> dict[str, Any]:
    if KAFKA_SECURITY_PROTOCOL.upper() != "SSL":
        return {"security_protocol": "PLAINTEXT"}

    return {
        "security_protocol": "SSL",
        "ssl_context": build_kafka_ssl_context(),
    }