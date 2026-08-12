from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

from sqlalchemy import BigInteger, Boolean, DateTime, Float, Integer, String, func, select, text
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.settings import get_settings

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

    transaction_id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    category: Mapped[str] = mapped_column(String, nullable=False)
    timestamp: Mapped[float] = mapped_column(Float, nullable=False)
    is_fraud: Mapped[bool] = mapped_column(Boolean, nullable=False)
    triggered_rules: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[Any] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class TransactionHistoryRecord(Base):
    __tablename__ = "transaction_history"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    transaction_id: Mapped[str] = mapped_column(String, nullable=False)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    category: Mapped[str] = mapped_column(String, nullable=False)
    timestamp: Mapped[float] = mapped_column(Float, nullable=False)
    is_fraud: Mapped[bool] = mapped_column(Boolean, nullable=False)
    triggered_rules: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    source_topic: Mapped[str | None] = mapped_column(String, nullable=True)
    source_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_offset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    source_timestamp: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(String, nullable=True)
    processed_at: Mapped[Any] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
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
    async with session_factory() as session:
        await session.execute(text("SELECT 1"))


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
    stmt = insert(TransactionRecord).values(
        transaction_id=result["transaction_id"],
        user_id=result["user_id"],
        amount=result["amount"],
        category=result["category"],
        timestamp=result["timestamp"],
        is_fraud=result["is_fraud"],
        triggered_rules=result["triggered_rules"],
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
        },
    )

    async with session_factory() as session:
        await session.execute(upsert_stmt)
        await session.execute(
            insert(TransactionHistoryRecord).values(
                _build_history_values(result, source_metadata, correlation_id=correlation_id)
            )
        )
        await session.commit()


def _build_history_values(
    result: dict[str, Any],
    source_metadata: dict[str, Any] | None = None,
    correlation_id: str | None = None,
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
    resolved_correlation_id = correlation_id or str(uuid4())
    return {
        "transaction_id": result["transaction_id"],
        "user_id": result["user_id"],
        "amount": result["amount"],
        "category": result["category"],
        "timestamp": result["timestamp"],
        "is_fraud": result["is_fraud"],
        "triggered_rules": result["triggered_rules"],
        "source_topic": source_metadata.get("source_topic"),
        "source_partition": source_metadata.get("source_partition"),
        "source_offset": source_metadata.get("source_offset"),
        "source_timestamp": source_metadata.get("source_timestamp"),
        "correlation_id": resolved_correlation_id,
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
    }
