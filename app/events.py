"""The ``TransactionCreated`` event contract consumed from Kafka.

Contract (schema_version 1), produced with the message key set to ``user_id`` so all
of a user's events land on one partition and are processed in order - the velocity
rules depend on that ordering:

.. code-block:: json

    {
      "schema_version": 1,
      "event_type": "TransactionCreated",
      "transaction_id": "0192f3c4-...",
      "user_id": "0192f3c4-...",
      "amount": "125.50",
      "category": "GROCERIES",
      "occurred_at": "2026-10-29T14:03:11Z",
      "correlation_id": "optional-trace-id"
    }

Evolution: unknown fields are ignored, so producers may add optional fields
without coordination. Renaming, removing, or changing the meaning of a field
requires a new ``schema_version``; this consumer dead-letters versions it does not
know rather than guessing.
"""

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, ValidationError, field_validator

from app.errors import InvalidEventError, UnsupportedSchemaVersionError

SUPPORTED_SCHEMA_VERSIONS = frozenset({1})
MAX_EVENT_BYTES = 64 * 1024

Category = Annotated[
    str,
    StringConstraints(strip_whitespace=True, to_upper=True, min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_\-]+$"),
]
CorrelationId = Annotated[str, StringConstraints(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:\-]+$")]
Money = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]


class TransactionCreatedV1(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    schema_version: Literal[1]
    event_type: Literal["TransactionCreated"]
    transaction_id: UUID
    user_id: UUID
    amount: Money
    category: Category
    occurred_at: AwareDatetime
    correlation_id: CorrelationId | None = None

    @field_validator("amount", mode="before")
    @classmethod
    def _reject_binary_floats(cls, value: object) -> object:
        # The parser below turns JSON numbers into Decimal; a float here would mean
        # precision was already lost upstream.
        if isinstance(value, float):
            raise ValueError("amount must be a decimal string or JSON number, not a float")
        return value

    @field_validator("occurred_at")
    @classmethod
    def _normalise_timezone(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)


def _field_errors(exc: ValidationError) -> list[dict[str, str]]:
    return [
        {"field": ".".join(str(part) for part in error["loc"]) or "event", "message": error["msg"]}
        for error in exc.errors(include_input=False, include_url=False)
    ]


def parse_transaction_event(raw: bytes | str | None) -> TransactionCreatedV1:
    """Decode and validate one Kafka message value.

    Raises:
        InvalidEventError: The payload is empty, oversized, not JSON, or fails validation.
        UnsupportedSchemaVersionError: The payload declares an unknown schema version.
    """
    if raw is None or len(raw) == 0:
        raise InvalidEventError("Event payload is empty")
    if len(raw) > MAX_EVENT_BYTES:
        raise InvalidEventError(f"Event payload exceeds {MAX_EVENT_BYTES} bytes")
    try:
        document = json.loads(raw, parse_float=Decimal)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidEventError(f"Event payload is not valid JSON: {exc.__class__.__name__}") from None
    if not isinstance(document, dict):
        raise InvalidEventError("Event payload must be a JSON object")

    version = document.get("schema_version")
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        raise UnsupportedSchemaVersionError(
            "Unsupported event schema version",
            [{"field": "schema_version", "message": f"supported versions: {sorted(SUPPORTED_SCHEMA_VERSIONS)}"}],
        )
    try:
        return TransactionCreatedV1.model_validate(document)
    except ValidationError as exc:
        raise InvalidEventError("Event failed schema validation", _field_errors(exc)) from None
