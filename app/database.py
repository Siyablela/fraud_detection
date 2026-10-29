"""PostgreSQL schema and data access.

Write-path idempotency (Kafka delivers at least once):

* ``transactions`` is keyed by ``transaction_id`` and the first decision wins.
  A redelivered or re-published event never re-evaluates, because its velocity
  context has changed since the first evaluation and the decision would drift.
* ``transaction_history`` is an append-only processing audit. The unique
  ``(source_topic, source_partition, source_offset)`` key makes redelivery of the
  same Kafka message a no-op, while a re-publish (new offset) is recorded with
  ``outcome = 'duplicate'``.
* A re-published event whose data differs from the stored transaction raises
  :class:`ConflictingDuplicateError` and the whole unit of work rolls back.
"""

import base64
import binascii
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    func,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.errors import ConflictingDuplicateError, RuleSetNotFoundError, RuleSetVersionConflictError
from app.observability import set_database_health
from app.rules import RuleSet, RuleSetDefinition
from app.settings import Settings

SessionFactory = async_sessionmaker[AsyncSession]
_UTC_NOW = text("timezone('UTC', now())")


class Base(DeclarativeBase):
    pass


class TransactionRecord(Base):
    __tablename__ = "transactions"

    transaction_id: Mapped[UUID] = mapped_column(sa.Uuid, primary_key=True)
    user_id: Mapped[UUID] = mapped_column(sa.Uuid, nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    category: Mapped[str] = mapped_column(String(64), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_fraud: Mapped[bool] = mapped_column(Boolean, nullable=False)
    triggered_rules: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    rules_version: Mapped[str] = mapped_column(String(128), nullable=False)
    rules_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=_UTC_NOW)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=_UTC_NOW)

    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_transactions_amount_positive"),
        Index("ix_transactions_user_occurred", "user_id", "occurred_at", "transaction_id"),
        Index("ix_transactions_category_occurred", "category", "occurred_at", "transaction_id"),
        Index("ix_transactions_occurred", "occurred_at", "transaction_id"),
        Index(
            "ix_transactions_fraud_occurred",
            "occurred_at",
            "transaction_id",
            postgresql_where=text("is_fraud"),
        ),
    )


class TransactionHistoryRecord(Base):
    __tablename__ = "transaction_history"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    transaction_id: Mapped[UUID] = mapped_column(
        sa.Uuid,
        sa.ForeignKey("transactions.transaction_id", ondelete="CASCADE", name="fk_transaction_history_transaction"),
        nullable=False,
    )
    outcome: Mapped[str] = mapped_column(String(16), nullable=False)
    user_id: Mapped[UUID] = mapped_column(sa.Uuid, nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    category: Mapped[str] = mapped_column(String(64), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_fraud: Mapped[bool] = mapped_column(Boolean, nullable=False)
    triggered_rules: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    rules_version: Mapped[str] = mapped_column(String(128), nullable=False)
    rules_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    source_topic: Mapped[str] = mapped_column(String(255), nullable=False)
    source_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    source_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_timestamp: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=_UTC_NOW)

    __table_args__ = (
        CheckConstraint("outcome IN ('evaluated', 'duplicate')", name="ck_transaction_history_outcome"),
        sa.UniqueConstraint("source_topic", "source_partition", "source_offset", name="uq_transaction_history_source"),
        Index("ix_transaction_history_transaction_processed_at", "transaction_id", "processed_at"),
    )


class RuleSetVersionRecord(Base):
    __tablename__ = "rule_set_versions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    rule_set_key: Mapped[str] = mapped_column(String(64), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    definition: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    description: Mapped[str] = mapped_column(String(500), nullable=False)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=_UTC_NOW)

    __table_args__ = (
        CheckConstraint("version >= 1", name="ck_rule_set_versions_version_positive"),
        sa.UniqueConstraint("rule_set_key", "version", name="uq_rule_set_versions_key_version"),
    )


# --- Engine lifecycle ---------------------------------------------------------


def _async_url(url: str) -> str:
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


def create_engine(settings: Settings) -> AsyncEngine:
    """Create the shared connection pool. One per process, reused for its lifetime."""
    return create_async_engine(
        _async_url(settings.database_url),
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_pool_max_overflow,
        pool_timeout=settings.db_pool_timeout_seconds,
        pool_pre_ping=True,
        connect_args={
            "server_settings": {"statement_timeout": str(settings.db_statement_timeout_ms)},
            "timeout": settings.db_pool_timeout_seconds,
        },
    )


def create_session_factory(engine: AsyncEngine) -> SessionFactory:
    return async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def database_session_factory(settings: Settings) -> AsyncIterator[SessionFactory]:
    """Own an engine for the duration of the block and dispose it afterwards."""
    engine = create_engine(settings)
    try:
        yield create_session_factory(engine)
    finally:
        await engine.dispose()


async def ping_database(session_factory: SessionFactory, service_name: str) -> None:
    """Run ``SELECT 1`` and publish the result as the ``fraud_database_up`` gauge."""
    try:
        async with session_factory() as session:
            await session.execute(text("SELECT 1"))
    except Exception:
        set_database_health(service_name, False)
        raise
    set_database_health(service_name, True)


# --- Transactions -------------------------------------------------------------


class RecordOutcome(StrEnum):
    EVALUATED = "evaluated"
    DUPLICATE = "duplicate"


@dataclass(frozen=True)
class TransactionDecision:
    transaction_id: UUID
    user_id: UUID
    amount: Decimal
    category: str
    occurred_at: datetime
    is_fraud: bool
    triggered_rules: list[str]
    rules_version: str
    rules_hash: str
    correlation_id: str | None


@dataclass(frozen=True)
class MessageProvenance:
    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None


@dataclass(frozen=True)
class RecordResult:
    outcome: RecordOutcome
    decision: TransactionDecision


def _decision_from_record(record: TransactionRecord) -> TransactionDecision:
    return TransactionDecision(
        transaction_id=record.transaction_id,
        user_id=record.user_id,
        amount=record.amount,
        category=record.category,
        occurred_at=record.occurred_at,
        is_fraud=record.is_fraud,
        triggered_rules=list(record.triggered_rules),
        rules_version=record.rules_version,
        rules_hash=record.rules_hash,
        correlation_id=record.correlation_id,
    )


def _same_transaction(left: TransactionDecision, right: TransactionDecision) -> bool:
    return (
        left.user_id == right.user_id
        and left.amount == right.amount
        and left.category == right.category
        and left.occurred_at == right.occurred_at
    )


def _history_values(
    decision: TransactionDecision, provenance: MessageProvenance, outcome: RecordOutcome, correlation_id: str | None
) -> dict[str, Any]:
    return {
        "transaction_id": decision.transaction_id,
        "outcome": outcome.value,
        "user_id": decision.user_id,
        "amount": decision.amount,
        "category": decision.category,
        "occurred_at": decision.occurred_at,
        "is_fraud": decision.is_fraud,
        "triggered_rules": decision.triggered_rules,
        "rules_version": decision.rules_version,
        "rules_hash": decision.rules_hash,
        "source_topic": provenance.topic,
        "source_partition": provenance.partition,
        "source_offset": provenance.offset,
        "source_timestamp": provenance.timestamp_ms,
        "correlation_id": correlation_id,
    }


async def record_decision(
    session_factory: SessionFactory, decision: TransactionDecision, provenance: MessageProvenance
) -> RecordResult:
    """Persist a decision and its audit row in one database transaction.

    Returns:
        RecordResult: ``EVALUATED`` with ``decision`` when this call stored it, or
        ``DUPLICATE`` with the previously stored decision.

    Raises:
        ConflictingDuplicateError: The id exists with different transaction data.
    """
    async with session_factory() as session, session.begin():
        inserted = await session.scalar(
            insert(TransactionRecord)
            .values(
                transaction_id=decision.transaction_id,
                user_id=decision.user_id,
                amount=decision.amount,
                category=decision.category,
                occurred_at=decision.occurred_at,
                is_fraud=decision.is_fraud,
                triggered_rules=decision.triggered_rules,
                rules_version=decision.rules_version,
                rules_hash=decision.rules_hash,
                correlation_id=decision.correlation_id,
            )
            .on_conflict_do_nothing(index_elements=[TransactionRecord.transaction_id])
            .returning(TransactionRecord.transaction_id)
        )
        if inserted is not None:
            outcome, stored = RecordOutcome.EVALUATED, decision
        else:
            existing = await session.scalar(
                select(TransactionRecord).where(TransactionRecord.transaction_id == decision.transaction_id)
            )
            stored = _decision_from_record(existing)
            if not _same_transaction(stored, decision):
                raise ConflictingDuplicateError(
                    "transaction_id already recorded with different user, amount, category or occurred_at"
                )
            outcome = RecordOutcome.DUPLICATE

        await session.execute(
            insert(TransactionHistoryRecord)
            .values(_history_values(stored, provenance, outcome, decision.correlation_id))
            .on_conflict_do_nothing(constraint="uq_transaction_history_source")
        )
    return RecordResult(outcome=outcome, decision=stored)


async def get_velocity_aggregates(
    session_factory: SessionFactory,
    user_id: UUID,
    window_start: datetime,
    window_end: datetime,
    exclude_transaction_id: UUID,
) -> tuple[int, Decimal]:
    """Count and sum a user's other transactions with ``window_start <= occurred_at <= window_end``.

    Excluding the transaction being evaluated keeps a redelivered message from
    counting itself. Served by ``ix_transactions_user_occurred``.
    """
    async with session_factory() as session:
        row = (
            await session.execute(
                select(func.count(), func.coalesce(func.sum(TransactionRecord.amount), 0)).where(
                    TransactionRecord.user_id == user_id,
                    TransactionRecord.occurred_at >= window_start,
                    TransactionRecord.occurred_at <= window_end,
                    TransactionRecord.transaction_id != exclude_transaction_id,
                )
            )
        ).one()
    return int(row[0]), Decimal(row[1])


async def get_transaction(session_factory: SessionFactory, transaction_id: UUID) -> TransactionRecord | None:
    async with session_factory() as session:
        return await session.get(TransactionRecord, transaction_id)


@dataclass(frozen=True)
class TransactionFilters:
    user_id: UUID | None = None
    category: str | None = None
    is_fraud: bool | None = None
    occurred_from: datetime | None = None
    occurred_to: datetime | None = None


@dataclass(frozen=True)
class PageCursor:
    """Keyset position: the sort key of the last row on the previous page."""

    occurred_at: datetime
    transaction_id: UUID

    def encode(self) -> str:
        raw = json.dumps({"o": self.occurred_at.isoformat(), "t": str(self.transaction_id)})
        return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")

    @classmethod
    def decode(cls, token: str) -> PageCursor:
        """Parse a token produced by :meth:`encode`; raises ``ValueError`` if malformed."""
        try:
            padded = token + "=" * (-len(token) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))
            return cls(occurred_at=datetime.fromisoformat(payload["o"]), transaction_id=UUID(payload["t"]))
        except (binascii.Error, ValueError, KeyError, TypeError, UnicodeError) as exc:
            raise ValueError("cursor is malformed") from exc


async def list_transactions(
    session_factory: SessionFactory, filters: TransactionFilters, limit: int, cursor: PageCursor | None
) -> tuple[list[TransactionRecord], PageCursor | None]:
    """Return one page, newest ``occurred_at`` first, plus the cursor for the next page.

    Keyset pagination keeps every page an index range scan; OFFSET pagination
    re-reads and discards all earlier rows, so deep pages get slower as data grows.
    """
    query = select(TransactionRecord)
    if filters.user_id is not None:
        query = query.where(TransactionRecord.user_id == filters.user_id)
    if filters.category is not None:
        query = query.where(TransactionRecord.category == filters.category)
    if filters.is_fraud is not None:
        query = query.where(TransactionRecord.is_fraud.is_(filters.is_fraud))
    if filters.occurred_from is not None:
        query = query.where(TransactionRecord.occurred_at >= filters.occurred_from)
    if filters.occurred_to is not None:
        query = query.where(TransactionRecord.occurred_at < filters.occurred_to)
    if cursor is not None:
        query = query.where(
            sa.tuple_(TransactionRecord.occurred_at, TransactionRecord.transaction_id)
            < sa.tuple_(sa.literal(cursor.occurred_at), sa.literal(cursor.transaction_id))
        )
    query = query.order_by(TransactionRecord.occurred_at.desc(), TransactionRecord.transaction_id.desc()).limit(
        limit + 1
    )

    async with session_factory() as session:
        rows = list((await session.scalars(query)).all())
    next_cursor = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor = PageCursor(occurred_at=rows[-1].occurred_at, transaction_id=rows[-1].transaction_id)
    return rows, next_cursor


async def summarize_transactions(session_factory: SessionFactory, since: datetime) -> dict[str, Any]:
    """Totals for the batch summary job: all, fraudulent, and per category since ``since``."""
    async with session_factory() as session:
        totals = (
            await session.execute(
                select(func.count(), func.count().filter(TransactionRecord.is_fraud.is_(True))).where(
                    TransactionRecord.created_at >= since
                )
            )
        ).one()
        category_rows = await session.execute(
            select(TransactionRecord.category, func.count())
            .where(TransactionRecord.created_at >= since)
            .group_by(TransactionRecord.category)
        )
        by_category = {category: int(count) for category, count in category_rows.all()}
    return {"total": int(totals[0]), "fraud": int(totals[1]), "by_category": by_category}


# --- Rule sets ----------------------------------------------------------------


def _rule_set_from_record(record: RuleSetVersionRecord) -> RuleSet:
    return RuleSet(
        key=record.rule_set_key,
        version=record.version,
        definition=RuleSetDefinition.model_validate(record.definition),
        content_hash=record.content_hash,
        description=record.description,
        created_by=record.created_by,
        created_at=record.created_at,
    )


async def get_latest_rule_set(session_factory: SessionFactory, key: str) -> RuleSet:
    """Return the highest version for ``key``; the latest version is the active one.

    Raises:
        RuleSetNotFoundError: No version exists for ``key``.
    """
    async with session_factory() as session:
        record = await session.scalar(
            select(RuleSetVersionRecord)
            .where(RuleSetVersionRecord.rule_set_key == key)
            .order_by(RuleSetVersionRecord.version.desc())
            .limit(1)
        )
    if record is None:
        raise RuleSetNotFoundError(f"No rule set exists for key '{key}'")
    return _rule_set_from_record(record)


async def get_rule_set_version(session_factory: SessionFactory, key: str, version: int) -> RuleSet:
    async with session_factory() as session:
        record = await session.scalar(
            select(RuleSetVersionRecord).where(
                RuleSetVersionRecord.rule_set_key == key, RuleSetVersionRecord.version == version
            )
        )
    if record is None:
        raise RuleSetNotFoundError(f"Rule set '{key}' has no version {version}")
    return _rule_set_from_record(record)


async def list_rule_set_versions(
    session_factory: SessionFactory, key: str, limit: int, before_version: int | None
) -> list[RuleSet]:
    query = select(RuleSetVersionRecord).where(RuleSetVersionRecord.rule_set_key == key)
    if before_version is not None:
        query = query.where(RuleSetVersionRecord.version < before_version)
    query = query.order_by(RuleSetVersionRecord.version.desc()).limit(limit)
    async with session_factory() as session:
        records = (await session.scalars(query)).all()
    return [_rule_set_from_record(record) for record in records]


async def create_rule_set_version(
    session_factory: SessionFactory,
    key: str,
    definition: RuleSetDefinition,
    definition_hash: str,
    description: str,
    created_by: str,
    expected_current_version: int | None,
) -> RuleSet:
    """Append a new version; versions are immutable once written.

    ``expected_current_version`` is an optimistic-concurrency guard: ``0`` means
    "the key must not exist yet". Two concurrent writers that both pass the check
    still cannot both win, because ``uq_rule_set_versions_key_version`` rejects the
    second insert.

    Raises:
        RuleSetVersionConflictError: The current version is not the expected one, or
            a concurrent write took the next version number first.
    """
    async with session_factory() as session:
        try:
            async with session.begin():
                current = await session.scalar(
                    select(func.coalesce(func.max(RuleSetVersionRecord.version), 0)).where(
                        RuleSetVersionRecord.rule_set_key == key
                    )
                )
                if expected_current_version is not None and current != expected_current_version:
                    raise RuleSetVersionConflictError(
                        f"Current version of '{key}' is {current}, expected {expected_current_version}"
                    )
                record = RuleSetVersionRecord(
                    rule_set_key=key,
                    version=int(current) + 1,
                    definition=definition.model_dump(mode="json"),
                    content_hash=definition_hash,
                    description=description,
                    created_by=created_by,
                )
                session.add(record)
                await session.flush()
                await session.refresh(record)
        except IntegrityError as exc:
            raise RuleSetVersionConflictError(
                f"Another version of '{key}' was created concurrently; retry with the new current version"
            ) from exc
    return _rule_set_from_record(record)
