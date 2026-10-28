import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional
from uuid import uuid7 as uuid

import sqlalchemy as sa
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Integer,
    String,
    desc,
    func,
    select,
    text,
)

from app.observability import get_logger

logger = get_logger(__name__)

from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.observability import set_database_health
from app.settings import get_settings
from app.time_utils import normalize_utc_datetime, utc_now


def _as_async_sqlalchemy_url(url: str) -> str:
    """Convert a PostgreSQL URL to the asyncpg-backed SQLAlchemy dialect when needed."""
    if url.startswith("postgresql+asyncpg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


class Base(DeclarativeBase):
    pass


class TransactionRecord(Base):
    __tablename__ = "transactions"

    transaction_id: Mapped[uuid] = mapped_column(uuid, primary_key=True)
    user_id: Mapped[uuid] = mapped_column(uuid, nullable=False)
    amount: Mapped[Decimal] = mapped_column(Float, nullable=False)
    category: Mapped[str] = mapped_column(String, nullable=False)
    timestamp: Mapped[float] = mapped_column(Float, nullable=False)
    is_fraud: Mapped[bool] = mapped_column(Boolean, nullable=False)
    triggered_rules: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    rules_version: Mapped[str] = mapped_column(String, nullable=False)
    rules_hash: Mapped[str] = mapped_column(String, nullable=False)
    correlation_id: Mapped[uuid | None] = mapped_column(uuid, nullable=True)
    created_at: Mapped[Any] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("timezone('UTC', now())"),
    )
    updated_at: Mapped[Any] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("timezone('UTC', now())"),
        onupdate=sa.text("timezone('UTC', now())"),
    )


class RuleModel(Base):
    """SQLAlchemy model for storing versioned rules."""

    __tablename__ = "application_rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rule_key: Mapped[str] = mapped_column(String(255), index=True)
    version: Mapped[int] = mapped_column(Integer, index=True)
    rules_json: Mapped[dict[str, Any]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class TransactionHistoryRecord(Base):
    __tablename__ = "transaction_history"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    transaction_id: Mapped[uuid] = mapped_column(uuid, nullable=False)
    user_id: Mapped[uuid] = mapped_column(uuid, nullable=False)
    amount: Mapped[Decimal] = mapped_column(Float, nullable=False)
    category: Mapped[str] = mapped_column(String, nullable=False)
    timestamp: Mapped[float] = mapped_column(Float, nullable=False)
    is_fraud: Mapped[bool] = mapped_column(Boolean, nullable=False)
    triggered_rules: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    rules_version: Mapped[str] = mapped_column(String, nullable=False)
    rules_hash: Mapped[str] = mapped_column(String, nullable=False)
    source_topic: Mapped[str | None] = mapped_column(String, nullable=True)
    source_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_offset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    source_timestamp: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    correlation_id: Mapped[uuid | None] = mapped_column(uuid, nullable=True)
    processed_at: Mapped[Any] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("timezone('UTC', now())"),
    )


def create_pool() -> tuple[Any, async_sessionmaker[AsyncSession]]:
    """Create the application's asynchronous database engine and session factory.

    Configures a SQLAlchemy asynchronous engine using the configured database
    URL and connection pool settings. The engine maintains a pool of reusable
    database connections, performs connection health checks before each use,
    and supports temporary overflow connections during periods of increased
    demand.

    An ``async_sessionmaker`` is also created to provide ``AsyncSession``
    instances for interacting with the database.

    Returns:
        tuple[Any, async_sessionmaker[AsyncSession]]:
            A tuple containing the configured SQLAlchemy async engine and an
            ``async_sessionmaker`` configured with ``expire_on_commit=False``.
    """

    settings = get_settings()
    engine = create_async_engine(
        _as_async_sqlalchemy_url(settings.database_url),
        pool_size=settings.db_pool_min_size,
        max_overflow=max(settings.db_pool_max_size - settings.db_pool_min_size, 0),
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    return engine, session_factory


@asynccontextmanager
async def database_pool():
    """Provide a managed asynchronous database session factory.

    Creates a SQLAlchemy async engine and session factory, yields the
    session factory to the caller, and ensures that the engine and all
    pooled database connections are cleanly disposed of when the context
    exits.

    This context manager is intended to be used during application
    startup and shutdown to manage the lifecycle of the database
    connection pool.

    Yields:
        async_sessionmaker[AsyncSession]: A session factory for creating
            ``AsyncSession`` instances.

    """
    engine, session_factory = create_pool()
    try:
        yield session_factory
    finally:
        await engine.dispose()


async def _retry_async(
    operation,
    *,
    retries: int = 3,
    delay_seconds: float = 0.5,
    retry_exceptions: tuple[type[BaseException], ...] = (
        asyncio.TimeoutError,
        TimeoutError,
        ConnectionError,
        OSError,
    ),
):
    """Retry only a narrow, explicitly transient set of network and timeout failures."""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return await operation()
        except Exception as exc:  # pragma: no cover - exercised via runtime behavior
            message = str(exc).lower()
            is_sqlalchemy_transient = isinstance(exc, sa.exc.OperationalError) and any(
                marker in message
                for marker in (
                    "timeout",
                    "could not connect",
                    "connection refused",
                    "connection reset",
                )
            )
            if not isinstance(exc, retry_exceptions) and not is_sqlalchemy_transient:
                raise
            last_error = exc
            if attempt == retries:
                raise
            await asyncio.sleep(delay_seconds * attempt)
    if last_error is not None:
        raise last_error
    raise RuntimeError("Retry helper did not return a result.")


async def ping_database(session_factory: async_sessionmaker[AsyncSession]) -> None:
    """Verify database connectivity.

    Executes a lightweight query against the database to confirm that a
    connection can be established and SQL statements can be executed
    successfully.

    Args:
        session_factory: A SQLAlchemy ``async_sessionmaker`` used to create
            an ``AsyncSession``.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If a connection cannot be established
            or the query execution fails.
    """

    async def _execute_ping() -> None:
        async with session_factory() as session:
            await session.execute(text("SELECT 1"))

    try:
        await _retry_async(_execute_ping, retries=3, delay_seconds=0.5)
        set_database_health("fraud-service", True)
    except Exception:
        set_database_health("fraud-service", False)
        raise


async def save_transaction(
    session_factory: async_sessionmaker[AsyncSession],
    result: dict[str, Any],
    source_metadata: dict[str, Any] | None = None,
    correlation_id: str | None = None,
) -> None:
    """Insert or update a transaction record in the database.

    Persists the supplied transaction by performing an upsert based on the
    transaction identifier. If a record with the same ``transaction_id``
    already exists, its fields are updated with the latest values;
    otherwise, a new record is inserted.

    Args:
        session_factory: A SQLAlchemy ``async_sessionmaker`` used to create
            an ``AsyncSession``.
        result: A dictionary containing the transaction details. Expected
            keys are ``transaction_id``, ``user_id``, ``amount``,
            ``category``, ``timestamp``, ``is_fraud``, and
            ``triggered_rules``.
        source_metadata: Optional Kafka source metadata for appending to the
            immutable transaction history record.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If the database operation or commit
            fails.
    """
    resolved_correlation_id = correlation_id or str(uuid())
    now_utc = utc_now()
    stmt = insert(TransactionRecord).values(
        transaction_id=result["transaction_id"],
        user_id=result["user_id"],
        amount=result["amount"],
        category=result["category"],
        timestamp=result["timestamp"],
        is_fraud=result["is_fraud"],
        triggered_rules=result["triggered_rules"],
        rules_version=result["rules_version"],
        rules_hash=result["rules_hash"],
        correlation_id=resolved_correlation_id,
        created_at=now_utc,
        updated_at=now_utc,
    )
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=[TransactionRecord.transaction_id],
        set_={
            "user_id": stmt.excluded.user_id,
            "amount": stmt.excluded.amount,
            "category": stmt.excluded.category,
            "timestamp": stmt.excluded.timestamp,
            "is_fraud": stmt.excluded.is_fraud,
            "triggered_rules": stmt.excluded.triggered_rules,
            "correlation_id": stmt.excluded.correlation_id,
            "updated_at": utc_now(),
        },
    )

    async def _persist_transaction() -> None:
        async with session_factory() as session:
            await session.execute(upsert_stmt)
            await session.execute(
                insert(TransactionHistoryRecord).values(
                    _build_history_values(
                        result, source_metadata, correlation_id=resolved_correlation_id
                    )
                )
            )
            await session.commit()

    await _retry_async(_persist_transaction, retries=3, delay_seconds=0.5)


def _build_history_values(
    result: dict[str, Any],
    source_metadata: dict[str, Any] | None = None,
    correlation_id: uuid | None = None,
) -> dict[str, Any]:
    """Build the immutable transaction history row payload, including Kafka source data.

    Args:
        result: A transaction evaluation result dictionary containing the core
            transaction fields.
        source_metadata: Optional Kafka metadata describing the originating topic,
            partition, offset, and timestamp.
        correlation_id: The request or message correlation ID to persist with the
            history record.

    Returns:
        dict[str, Any]: A row dictionary suitable for insertion into the
            ``transaction_history`` table.
    """
    source_metadata = source_metadata or {}
    resolved_correlation_id = correlation_id or str(uuid())
    processed_at = utc_now()
    return {
        "transaction_id": result["transaction_id"],
        "user_id": result["user_id"],
        "amount": result["amount"],
        "category": result["category"],
        "timestamp": result["timestamp"],
        "is_fraud": result["is_fraud"],
        "triggered_rules": result["triggered_rules"],
        "rules_version": result["rules_version"],
        "rules_hash": result["rules_hash"],
        "source_topic": source_metadata.get("source_topic"),
        "source_partition": source_metadata.get("source_partition"),
        "source_offset": source_metadata.get("source_offset"),
        "source_timestamp": source_metadata.get("source_timestamp"),
        "correlation_id": resolved_correlation_id,
        "processed_at": processed_at,
    }


async def get_transaction(
    session_factory: async_sessionmaker[AsyncSession], transaction_id: str
) -> dict[str, Any] | None:
    """
    Retrieve a transaction record by its identifier.

    Args:
        session_factory: A SQLAlchemy ``async_sessionmaker`` used to create
            an ``AsyncSession``.
        transaction_id: The unique identifier of the transaction to retrieve.

    Returns:
        dict[str, Any] | None: A dictionary containing the transaction details
        if found, otherwise ``None``.
    """
    async with session_factory() as session:
        row = await session.scalar(
            select(TransactionRecord).where(
                TransactionRecord.transaction_id == transaction_id
            )
        )
    return _row_to_result(row) if row else None


async def get_transactions_by_category(
    session_factory: async_sessionmaker[AsyncSession],
    category: str,
    offset: int,
    limit: int,
) -> tuple[list[dict[str, Any]], int]:
    """
    Retrieve a paginated list of transaction records filtered by category.

    Args:
        session_factory: A SQLAlchemy ``async_sessionmaker`` used to create
            an ``AsyncSession``.
        category: The category to filter transactions by.
        offset: The number of rows to skip.
        limit: The maximum number of records to retrieve.

    Returns:
        tuple[list[dict[str, Any]], int]: A tuple containing the page of
        transaction details and the total number of matching rows.
    """
    async with session_factory() as session:
        base_query = select(TransactionRecord).where(
            func.lower(TransactionRecord.category) == func.lower(category)
        )
        total_count = await session.scalar(
            select(func.count()).select_from(base_query.subquery())
        )
        rows = await session.scalars(
            base_query.order_by(TransactionRecord.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        records = rows.all()
    return [_row_to_result(row) for row in records], int(total_count or 0)


def _row_to_result(row: TransactionRecord) -> dict[str, Any]:
    """Map a SQLAlchemy transaction row to a serializable dictionary payload.

    Args:
        row: A ``TransactionRecord`` instance loaded from the database.

    Returns:
        dict[str, Any]: A JSON-friendly transaction representation without ORM
            metadata.
    """
    return {
        "transaction_id": row.transaction_id,
        "user_id": row.user_id,
        "amount": row.amount,
        "category": row.category,
        "timestamp": row.timestamp,
        "is_fraud": row.is_fraud,
        "triggered_rules": row.triggered_rules,
        "rules_version": row.rules_version,
        "rules_hash": row.rules_hash,
        "correlation_id": row.correlation_id,
        "created_at": normalize_utc_datetime(row.created_at),
        "updated_at": normalize_utc_datetime(row.updated_at),
    }


async def get_recent_user_activity(
    session_factory: async_sessionmaker[AsyncSession],
    user_id: uuid,
    window_start_epoch: float,
    window_end_epoch: float,
) -> list[dict[str, Any]]:
    """Return a user's transaction count and amount total in an event-time window.

    Used by the worker to feed the stateful velocity fraud rule. Only transacyions
    with a business ``timestamp`` between ```window_start_epoch`` and
    ``window_end_epoch``(inclusive) are considered.

    Args:
        session_factory: Async session favtory for the database.
        user_id: The user whose recent activity is being summed.
        window_start_epoch: Inclusive lower bound for event-time lookup.
        window_end_epoch: Inclusive upper bound for event-time lookup.

    Returns:
        tuple[int, Decimal]: the number of matching transactions and sum of their amounts.
    """
    async with session_factory() as session:
        count = await session.scalar(
            select(func.count())
            .select_from(TransactionRecord)
            .where(
                TransactionRecord.user_id == user_id,
                TransactionRecord.timestamp >= window_start_epoch,
                TransactionRecord.timestamp >= window_end_epoch,
            )
        )
        total = await session.scalar(
            select(func.coalesce(func.sum(TransactionRecord.amount), 0)).where(
                TransactionRecord.user_id == user_id,
                TransactionRecord.timestamp >= window_start_epoch,
                TransactionRecord.timestamp <= window_end_epoch,
            )
        )
    return int(count or 0), Decimal(str(total or 0))


async def summarize_transactions(
    session_factory: async_sessionmaker[AsyncSession], since: datetime
) -> dict[str, Any]:
    """Summarize transaction activity since a cutoff time, for the deffered batch job.

    Args:
        session_factory: Async session factory for the database.
        since: The inclusion lower bound for the transaction ``timestamp``.

    Returns:
        dict[str, Any]:  total and per category breakdown of processed transactions.
    """
    async with session_factory() as session:
        total = await session.scalar(
            select(func.count())
            .select_from(TransactionRecord)
            .where(TransactionRecord.created_at >= since)
        )
        fraud_total = await session.scalar(
            select(func.count())
            .select_from(TransactionRecord)
            .where(
                TransactionRecord.created_at >= since,
                TransactionRecord.is_fraud.is_(True),
            )
        )
        category_rows = await session.execute(
            select(TransactionRecord.category, func.count())
            .where(TransactionRecord.created_at >= since)
            .group_by(TransactionRecord.category)
        )
        by_category = {category: int(count) for category, count in category_rows.all()}
    return {
        "total": int(total or 0),
        "fraud": int(fraud_total or 0),
        "by_category": by_category,
    }

# Match this helper function with the database module layer inside app.rule or app.database
async def get_active_rules_config(db_pool: Any, rule_key: str) -> Optional[Dict[str, Any]]:
    """
    Fetches the latest version of a JSON-Logic ruleset from PostgreSQL using SQLAlchemy 2.0.
    
    Args:
        db_pool: The async session maker factory (AsyncSessionLocal).
        rule_key: The string identifier mapping the rule context target (e.g. 'fraud_default_ruleset').
        
    Returns:
        Optional[Dict[str, Any]]: Rehydrated application rules dictionary, or None if no entry exists.
    """
    # Safeguard initialization if db_pool behaves as a callable factory session
    async_session_factory = db_pool
    
    async with async_session_factory() as session:
        try:
            # Construct the declarative selection AST representation
            query = (
                select(RuleModel)
                .where(RuleModel.rule_key == rule_key)
                .order_by(desc(RuleModel.version))
                .limit(1)
            )
            
            result = await session.execute(query)
            db_rule = result.scalar_one_or_none()
            
            if not db_rule:
                logger.warning("ruleset_not_found_in_database", rule_key=rule_key)
                return None
            
            # Reconstruct the structured payload tracking formatting expectations
            # Extraction matches values parsed by our RuleValidationSchema model validation constraints
            return {
                "version": db_rule.version,
                "rule_name": db_rule.rules_json["rule_name"],
                "root_condition": db_rule.rules_json["root_condition"],
                "actions": db_rule.rules_json["actions"]
            }
            
        except Exception:
            logger.exception("database_fetch_active_rules_failed", rule_key=rule_key)
            raise
        
    async def find_latest_rule_in_db(
        session_factory: async_sessionmaker[AsyncSession], 
        rule_key: str
    ) -> Optional[RuleModel]:
        """Retrieve the latest version of a rule by its rule_key.

        Args:
            session_factory: A SQLAlchemy async_sessionmaker instance.
            rule_key: The string key identifying the rule.

        Returns:
            Optional[RuleModel]: The RuleModel instance with the highest version, 
                                or None if no rule matches the key.
        """
        stmt = (
            select(RuleModel)
            .where(RuleModel.rule_key == rule_key)
            .order_by(RuleModel.version.desc())
            .limit(1)
        )

        async with session_factory() as session:
            result = await session.execute(stmt)
            return result.scalar_one_or_none()