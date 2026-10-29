import asyncio
import json
import logging
from datetime import UTC, datetime

import pytest
import structlog

from app.dlq import DeadLetter, DeadLetterPublisher, build_dlq_headers, describe_error
from app.errors import FailureClass, InvalidEventError, TransientError
from app.observability import configure_logging, get_logger, sanitise_correlation_id


def letter(**overrides) -> DeadLetter:
    values = dict(
        source_topic="transactions",
        source_partition=2,
        source_offset=41,
        key=b"user-key",
        value=b'{"bad": true}',
        error=InvalidEventError("Event failed schema validation", [{"field": "amount", "message": "Field required"}]),
        failure_class=FailureClass.PERMANENT,
        attempts=1,
        correlation_id="corr-1",
    )
    values.update(overrides)
    return DeadLetter(**values)


def test_headers_describe_the_failure_and_source():
    headers = dict(build_dlq_headers(letter(), failed_at=datetime(2026, 10, 29, tzinfo=UTC)))
    assert headers["dlq.error.class"] == b"permanent"
    assert headers["dlq.error.type"] == b"InvalidEventError"
    assert headers["dlq.source.partition"] == b"2"
    assert headers["dlq.source.offset"] == b"41"
    assert headers["dlq.failed_at"] == b"2026-10-29T00:00:00+00:00"
    assert headers["correlation_id"] == b"corr-1"
    assert b"amount: Field required" in headers["dlq.error.message"]


def test_error_description_is_truncated():
    assert len(describe_error(RuntimeError("x" * 5000))) == 1024


class FakeProducer:
    def __init__(self, delivery_error=None, raise_on_produce=None):
        self.delivery_error = delivery_error
        self.raise_on_produce = raise_on_produce
        self.produced = []
        self._callbacks = []

    def produce(self, topic, value=None, key=None, headers=None, on_delivery=None):
        if self.raise_on_produce:
            raise self.raise_on_produce
        self.produced.append((topic, key, value, headers))
        self._callbacks.append(on_delivery)

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        for callback in self._callbacks:
            callback(self.delivery_error, None)
        self._callbacks.clear()
        return 0


def test_publish_forwards_original_bytes_and_waits_for_ack():
    producer = FakeProducer()
    asyncio.run(DeadLetterPublisher(producer, "transactions.dlq").publish(letter()))
    topic, key, value, _ = producer.produced[0]
    assert (topic, key, value) == ("transactions.dlq", b"user-key", b'{"bad": true}')


@pytest.mark.parametrize(
    "producer", [FakeProducer(delivery_error="broker down"), FakeProducer(raise_on_produce=BufferError())]
)
def test_unacknowledged_publish_is_transient(producer):
    with pytest.raises(TransientError):
        asyncio.run(DeadLetterPublisher(producer, "transactions.dlq").publish(letter()))


@pytest.mark.parametrize(
    ("value", "expected"),
    [("abc-123", "abc-123"), ("a" * 129, None), ("bad value\n", None), (None, None), (42, None)],
)
def test_correlation_ids_are_sanitised(value, expected):
    assert sanitise_correlation_id(value) == expected


def test_logs_are_json_and_redact_sensitive_keys(capsys):
    configure_logging("test-service", "INFO", ["password", "authorization"])
    try:
        get_logger("test").info(
            "login", password="hunter2", nested={"Authorization": "Bearer abc", "ok": 1}, user="analyst"
        )
        record = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    finally:
        structlog.reset_defaults()
        logging.getLogger().handlers = []
    assert record["password"] == "[REDACTED]"
    assert record["nested"] == {"Authorization": "[REDACTED]", "ok": 1}
    assert record["service"] == "test-service"
    assert record["level"] == "info"
