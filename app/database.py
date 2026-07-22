import json
import os
from contextlib import asynccontextmanager
from typing import Any

import asyncpg

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://fraud_user:change-me@localhost:5432/fraud_detection",
)

CREATE_TRANSACTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    category TEXT NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    is_fraud BOOLEAN NOT NULL,
    triggered_rules JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


async def create_pool() -> asyncpg.Pool:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    async with pool.acquire() as connection:
        await connection.execute(CREATE_TRANSACTIONS_TABLE)
    return pool


@asynccontextmanager
async def database_pool():
    pool = await create_pool()
    try:
        yield pool
    finally:
        await pool.close()


async def save_transaction(pool: asyncpg.Pool, result: dict[str, Any]) -> None:
    await pool.execute(
        """
        INSERT INTO transactions (
            transaction_id, user_id, amount, category, timestamp,
            is_fraud, triggered_rules
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        ON CONFLICT (transaction_id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            amount = EXCLUDED.amount,
            category = EXCLUDED.category,
            timestamp = EXCLUDED.timestamp,
            is_fraud = EXCLUDED.is_fraud,
            triggered_rules = EXCLUDED.triggered_rules
        """,
        result["transaction_id"],
        result["user_id"],
        result["amount"],
        result["category"],
        result["timestamp"],
        result["is_fraud"],
        json.dumps(result["triggered_rules"]),
    )


async def get_transaction(pool: asyncpg.Pool, transaction_id: str) -> dict[str, Any] | None:
    row = await pool.fetchrow(
        """
        SELECT transaction_id, user_id, amount, category, timestamp,
               is_fraud, triggered_rules
        FROM transactions
        WHERE transaction_id = $1
        """,
        transaction_id,
    )
    return _row_to_result(row) if row else None


async def get_transactions_by_category(
    pool: asyncpg.Pool, category: str, limit: int
) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        """
        SELECT transaction_id, user_id, amount, category, timestamp,
               is_fraud, triggered_rules
        FROM transactions
        WHERE LOWER(category) = LOWER($1)
        ORDER BY created_at DESC
        LIMIT $2
        """,
        category,
        limit,
    )
    return [_row_to_result(row) for row in rows]


def _row_to_result(row: asyncpg.Record) -> dict[str, Any]:
    triggered_rules = row["triggered_rules"]
    if isinstance(triggered_rules, str):
        triggered_rules = json.loads(triggered_rules)

    return {
        "transaction_id": row["transaction_id"],
        "user_id": row["user_id"],
        "amount": row["amount"],
        "category": row["category"],
        "timestamp": row["timestamp"],
        "is_fraud": row["is_fraud"],
        "triggered_rules": triggered_rules,
    }
