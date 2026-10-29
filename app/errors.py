"""Typed error hierarchy and failure classification.

Every failure the worker sees is sorted into one of three classes, because each
needs a different response:

* ``transient``  - the message is fine but a dependency is not (database down,
  Redis timeout, no active rule set yet). Retry with backoff, never dead-letter:
  every other message would fail the same way, so dead-lettering would only move
  good traffic into the DLQ.
* ``permanent``  - the message itself can never succeed (bad schema, a duplicate
  transaction id carrying different data). Dead-letter immediately.
* ``unexpected`` - anything unclassified, most likely a bug triggered by this
  particular message. Retry a bounded number of times, then dead-letter so one
  message cannot block its partition forever.
"""

from enum import StrEnum
from typing import Any

import asyncpg
import redis.exceptions
import sqlalchemy.exc
from pydantic import ValidationError


class FailureClass(StrEnum):
    TRANSIENT = "transient"
    PERMANENT = "permanent"
    UNEXPECTED = "unexpected"


class FraudDetectionError(Exception):
    """Base class for errors raised by this service."""


class TransientError(FraudDetectionError):
    """A dependency failed in a way that is expected to recover on retry."""


class PermanentError(FraudDetectionError):
    """The input can never be processed successfully; retrying is pointless."""


class InvalidEventError(PermanentError):
    """A Kafka message is not a valid transaction event.

    Args:
        message: Human-readable summary.
        details: Field-level problems as ``{"field": ..., "message": ...}`` dicts.
            Values are deliberately excluded so the error is safe to log.
    """

    def __init__(self, message: str, details: list[dict[str, str]] | None = None):
        super().__init__(message)
        self.details = details or []


class UnsupportedSchemaVersionError(InvalidEventError):
    """The event declares a schema version this consumer does not understand."""


class ConflictingDuplicateError(PermanentError):
    """A transaction id was seen before with different transaction data."""


class InvalidRuleSetError(PermanentError):
    """A rule-set definition failed validation."""

    def __init__(self, message: str, details: list[dict[str, str]] | None = None):
        super().__init__(message)
        self.details = details or []


class RuleEvaluationError(FraudDetectionError):
    """A validated rule could not be evaluated against a transaction context."""


class RuleSetNotFoundError(FraudDetectionError):
    """No rule set (or requested version) exists for the given key."""


class RuleSetVersionConflictError(FraudDetectionError):
    """A rule-set write lost an optimistic-concurrency race."""


class TransactionNotFoundError(FraudDetectionError):
    """No transaction exists with the requested id."""


_TRANSIENT_LIBRARY_ERRORS: tuple[type[BaseException], ...] = (
    TimeoutError,
    ConnectionError,
    OSError,
    asyncpg.PostgresConnectionError,
    asyncpg.exceptions.CannotConnectNowError,
    asyncpg.exceptions.TooManyConnectionsError,
    asyncpg.exceptions.QueryCanceledError,  # statement_timeout
    redis.exceptions.ConnectionError,
    redis.exceptions.TimeoutError,
    sqlalchemy.exc.TimeoutError,  # connection-pool checkout timeout
)


def classify_failure(exc: BaseException) -> FailureClass:
    """Decide how the worker should react to ``exc``.

    SQLAlchemy wraps driver errors, so the original driver exception is inspected
    as well as the wrapper.
    """
    if isinstance(exc, (TransientError, RuleSetNotFoundError)):
        return FailureClass.TRANSIENT
    if isinstance(exc, (PermanentError, ValidationError)):
        return FailureClass.PERMANENT

    candidates: list[Any] = [exc]
    if isinstance(exc, sqlalchemy.exc.DBAPIError):
        if exc.connection_invalidated:
            return FailureClass.TRANSIENT
        candidates.append(exc.orig)
        candidates.append(getattr(exc.orig, "__cause__", None))
    for candidate in candidates:
        if isinstance(candidate, _TRANSIENT_LIBRARY_ERRORS):
            return FailureClass.TRANSIENT
    if isinstance(exc, (sqlalchemy.exc.OperationalError, sqlalchemy.exc.InterfaceError)):
        return FailureClass.TRANSIENT
    return FailureClass.UNEXPECTED
