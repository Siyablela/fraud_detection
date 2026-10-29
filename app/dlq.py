"""Dead-letter publishing.

The DLQ record keeps the original key and value byte-for-byte, so replay is "copy
value back to the source topic". Why it failed travels in headers (the Kafka
Connect convention), which keeps tooling able to read the payload unchanged.

Headers (all UTF-8 strings): ``dlq.schema_version``, ``dlq.error.class``
(``permanent`` | ``unexpected``), ``dlq.error.type``, ``dlq.error.message``
(truncated, never contains payload values), ``dlq.source.topic``,
``dlq.source.partition``, ``dlq.source.offset``, ``dlq.attempts``,
``dlq.failed_at`` (ISO-8601 UTC) and ``correlation_id`` when known.
"""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from app.errors import FailureClass, InvalidEventError, TransientError

DLQ_SCHEMA_VERSION = "1"
_MAX_ERROR_MESSAGE_CHARS = 1024


@dataclass(frozen=True)
class DeadLetter:
    source_topic: str
    source_partition: int
    source_offset: int
    key: bytes | None
    value: bytes | None
    error: BaseException
    failure_class: FailureClass
    attempts: int
    correlation_id: str | None = None


def describe_error(error: BaseException) -> str:
    """A log- and header-safe description: messages and field names, no payload values."""
    message = str(error)
    if isinstance(error, InvalidEventError) and error.details:
        message += ": " + "; ".join(f"{item['field']}: {item['message']}" for item in error.details)
    return message[:_MAX_ERROR_MESSAGE_CHARS]


def build_dlq_headers(letter: DeadLetter, failed_at: datetime | None = None) -> list[tuple[str, bytes]]:
    failed_at = failed_at or datetime.now(UTC)
    headers = {
        "dlq.schema_version": DLQ_SCHEMA_VERSION,
        "dlq.error.class": letter.failure_class.value,
        "dlq.error.type": type(letter.error).__name__,
        "dlq.error.message": describe_error(letter.error),
        "dlq.source.topic": letter.source_topic,
        "dlq.source.partition": str(letter.source_partition),
        "dlq.source.offset": str(letter.source_offset),
        "dlq.attempts": str(letter.attempts),
        "dlq.failed_at": failed_at.isoformat(),
    }
    if letter.correlation_id:
        headers["correlation_id"] = letter.correlation_id
    return [(name, value.encode("utf-8")) for name, value in headers.items()]


class _Producer(Protocol):
    def produce(
        self, topic: str, value: Any = ..., key: Any = ..., headers: Any = ..., on_delivery: Any = ...
    ) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float) -> int: ...


class DeadLetterPublisher:
    """Publish to the DLQ and wait for the broker's acknowledgement.

    The source offset is only committed after this returns, so a crash between
    failure and acknowledgement redelivers the source message instead of losing it.
    """

    def __init__(self, producer: _Producer, topic: str, delivery_timeout_seconds: float = 30.0):
        self._producer = producer
        self._topic = topic
        self._timeout = delivery_timeout_seconds

    async def publish(self, letter: DeadLetter) -> None:
        """Raises :class:`TransientError` when the broker does not acknowledge in time."""
        loop = asyncio.get_running_loop()
        delivered: asyncio.Future[None] = loop.create_future()

        def on_delivery(err: Any, _msg: Any) -> None:
            def settle() -> None:
                if delivered.done():
                    return
                if err is not None:
                    delivered.set_exception(TransientError(f"DLQ delivery failed: {err}"))
                else:
                    delivered.set_result(None)

            loop.call_soon_threadsafe(settle)

        try:
            self._producer.produce(
                self._topic,
                value=letter.value,
                key=letter.key,
                headers=build_dlq_headers(letter),
                on_delivery=on_delivery,
            )
        except BufferError as exc:
            raise TransientError("DLQ producer queue is full") from exc
        # flush() serves the delivery callback; run it off the event loop.
        await asyncio.to_thread(self._producer.flush, self._timeout)
        try:
            await asyncio.wait_for(delivered, timeout=1.0)
        except TimeoutError as exc:
            raise TransientError("DLQ delivery was not acknowledged in time") from exc
