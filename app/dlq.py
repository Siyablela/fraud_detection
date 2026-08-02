import json
from typing import Any


def build_dlq_payload(
    source_topic: str,
    source_partition: int,
    source_offset: int,
    source_timestamp: int | None,
    raw_payload: str,
    error: Exception,
) -> dict[str, Any]:
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
    return json.dumps(payload).encode("utf-8")
