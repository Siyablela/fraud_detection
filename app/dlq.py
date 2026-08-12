import json
from typing import Any


# DLQ payloads are kept small and explicit so failures can be replayed or inspected later.
def build_dlq_payload(
    source_topic: str,
    source_partition: int,
    source_offset: int,
    source_timestamp: int | None,
    raw_payload: str,
    error: Exception,
) -> dict[str, Any]:
    """Build a JSON-serializable dead-letter payload for a processing failure.

    Args:
        source_topic: Kafka topic where the failed message was consumed.
        source_partition: Kafka partition producing the failed message.
        source_offset: Kafka offset of the failed message.
        source_timestamp: Source message timestamp, if present.
        raw_payload: Original serialized event payload as text.
        error: Exception raised while processing the message.

    Returns:
        dict[str, Any]: A dead-letter payload containing source details and the failure context.
    """
    return {
        "source_topic": source_topic,
        "source_partition": source_partition,
        "source_offset": source_offset,
        "source_timestamp": source_timestamp,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "raw_payload": raw_payload,
    }


def encode_dlq_payload(payload: dict[str, Any]) -> bytes:
    """Encode a DLQ payload as UTF-8 JSON bytes for Kafka delivery.

    Args:
        payload: A serializable dead-letter payload dictionary.

    Returns:
        bytes: UTF-8 JSON encoded payload suitable for Kafka message values.
    """
    return json.dumps(payload).encode("utf-8")
