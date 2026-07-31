from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy import Boolean, DateTime, Float, String, func, select, text
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.settings import DATABASE_URL, DB_POOL_MAX_SIZE, DB_POOL_MIN_SIZE

def _as_async_sqlalchemy_url(url: str) -> str:
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


def create_pool() -> tuple[Any, async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(
        _as_async_sqlalchemy_url(DATABASE_URL),
        pool_size=DB_POOL_MIN_SIZE,
        max_overflow=max(DB_POOL_MAX_SIZE - DB_POOL_MIN_SIZE, 0),
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    return engine, session_factory


@asynccontextmanager
async def database_pool():
    engine, session_factory = create_pool()
    try:
        yield session_factory
    finally:
        await engine.dispose()


async def ping_database(session_factory: async_sessionmaker[AsyncSession]) -> None:
    async with session_factory() as session:
        await session.execute(text("SELECT 1"))


async def save_transaction(
    session_factory: async_sessionmaker[AsyncSession], result: dict[str, Any]
) -> None:
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
        await session.commit()


async def get_transaction(
    session_factory: async_sessionmaker[AsyncSession], transaction_id: str
) -> dict[str, Any] | None:
    async with session_factory() as session:
        row = await session.scalar(
            select(TransactionRecord).where(
                TransactionRecord.transaction_id == transaction_id
            )
        )
    return _row_to_result(row) if row else None


async def get_transactions_by_category(
    session_factory: async_sessionmaker[AsyncSession], category: str, limit: int
) -> list[dict[str, Any]]:
    async with session_factory() as session:
        rows = await session.scalars(
            select(TransactionRecord)
            .where(func.lower(TransactionRecord.category) == func.lower(category))
            .order_by(TransactionRecord.created_at.desc())
            .limit(limit)
        )
        records = rows.all()
    return [_row_to_result(row) for row in records]


def _row_to_result(row: TransactionRecord) -> dict[str, Any]:
    return {
        "transaction_id": row.transaction_id,
        "user_id": row.user_id,
        "amount": row.amount,
        "category": row.category,
        "timestamp": row.timestamp,
        "is_fraud": row.is_fraud,
        "triggered_rules": row.triggered_rules,
    }
