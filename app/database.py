import json
import hashlib
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from app.settings import AUDIT_HASH_SECRET, DATABASE_URL, DB_POOL_MAX_SIZE, DB_POOL_MIN_SIZE

CREATE_TRANSACTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS transactions (
    audit_id TEXT PRIMARY KEY,
    correlation_id TEXT,
    user_id TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    category TEXT NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    actor_id TEXT,
    actor_type TEXT,
    source_ip TEXT,
    user_agent TEXT,
    request_id TEXT,
    ingest_path TEXT,
    ruleset_hash TEXT NOT NULL,
    is_fraud BOOLEAN NOT NULL,
    triggered_rules JSONB NOT NULL,
    created_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_date TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

CREATE_AUDIT_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS audit_events (
    id BIGSERIAL PRIMARY KEY,
    audit_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actor_id TEXT,
    actor_type TEXT,
    source_ip TEXT,
    user_agent TEXT,
    request_id TEXT,
    service_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    prev_hash TEXT,
    event_hash TEXT NOT NULL
)
"""

CREATE_AUDIT_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_correlation_id ON transactions(correlation_id);
CREATE INDEX IF NOT EXISTS idx_transactions_created_date ON transactions(created_date DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_audit_id ON audit_events(audit_id, id DESC);
"""


async def create_pool() -> asyncpg.Pool:
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=DB_POOL_MIN_SIZE,
        max_size=DB_POOL_MAX_SIZE,
    )
    async with pool.acquire() as connection:
        await connection.execute(CREATE_TRANSACTIONS_TABLE)
        await connection.execute(CREATE_AUDIT_EVENTS_TABLE)
        await connection.execute(CREATE_AUDIT_INDEXES)
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
            audit_id, correlation_id, user_id, amount, category, timestamp,
            actor_id, actor_type, source_ip, user_agent, request_id, ingest_path,
            ruleset_hash, is_fraud, triggered_rules, created_date, updated_date
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::jsonb, NOW(), NOW())
        ON CONFLICT (audit_id) DO UPDATE SET
            correlation_id = EXCLUDED.correlation_id,
            user_id = EXCLUDED.user_id,
            amount = EXCLUDED.amount,
            category = EXCLUDED.category,
            timestamp = EXCLUDED.timestamp,
            actor_id = EXCLUDED.actor_id,
            actor_type = EXCLUDED.actor_type,
            source_ip = EXCLUDED.source_ip,
            user_agent = EXCLUDED.user_agent,
            request_id = EXCLUDED.request_id,
            ingest_path = EXCLUDED.ingest_path,
            ruleset_hash = EXCLUDED.ruleset_hash,
            is_fraud = EXCLUDED.is_fraud,
            triggered_rules = EXCLUDED.triggered_rules,
            updated_date = NOW()
        """,
        result["audit_id"],
        result["correlation_id"],
        result["user_id"],
        result["amount"],
        result["category"],
        result["timestamp"],
        result.get("actor_id"),
        result.get("actor_type"),
        result.get("source_ip"),
        result.get("user_agent"),
        result.get("request_id"),
        result.get("ingest_path"),
        result["ruleset_hash"],
        result["is_fraud"],
        json.dumps(result["triggered_rules"]),
    )


async def create_audit_event(
    pool: asyncpg.Pool,
    *,
    audit_id: str,
    event_type: str,
    service_name: str,
    payload: dict[str, Any],
    actor_id: str | None = None,
    actor_type: str | None = None,
    source_ip: str | None = None,
    user_agent: str | None = None,
    request_id: str | None = None,
) -> str:
    previous_hash = await pool.fetchval(
        """
        SELECT event_hash
        FROM audit_events
        WHERE audit_id = $1
        ORDER BY id DESC
        LIMIT 1
        """,
        audit_id,
    )

    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    event_hash = hashlib.sha256(
        f"{AUDIT_HASH_SECRET}|{previous_hash or ''}|{event_type}|{payload_json}".encode("utf-8")
    ).hexdigest()

    await pool.execute(
        """
        INSERT INTO audit_events (
            audit_id, event_type, actor_id, actor_type, source_ip,
            user_agent, request_id, service_name, payload, prev_hash, event_hash
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11)
        """,
        audit_id,
        event_type,
        actor_id,
        actor_type,
        source_ip,
        user_agent,
        request_id,
        service_name,
        payload_json,
        previous_hash,
        event_hash,
    )
    return event_hash


async def get_audit_events(
    pool: asyncpg.Pool, audit_id: str, limit: int
) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        """
        SELECT id, audit_id, event_type, event_time, actor_id, actor_type,
               source_ip, user_agent, request_id, service_name, payload,
               prev_hash, event_hash
        FROM audit_events
        WHERE audit_id = $1
        ORDER BY id DESC
        LIMIT $2
        """,
        audit_id,
        limit,
    )
    return [_row_to_audit_event(row) for row in rows]


async def get_transaction(pool: asyncpg.Pool, audit_id: str) -> dict[str, Any] | None:
    row = await pool.fetchrow(
        """
     SELECT audit_id, correlation_id, user_id, amount, category, timestamp,
             actor_id, actor_type, source_ip, user_agent, request_id,
             ingest_path, ruleset_hash, is_fraud, triggered_rules,
             created_date, updated_date
        FROM transactions
     WHERE audit_id = $1
        """,
     audit_id,
    )
    return _row_to_result(row) if row else None


async def get_transactions_by_category(
    pool: asyncpg.Pool, category: str, limit: int
) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        """
         SELECT audit_id, correlation_id, user_id, amount, category, timestamp,
               actor_id, actor_type, source_ip, user_agent, request_id,
               ingest_path, ruleset_hash, is_fraud, triggered_rules,
               created_date, updated_date
        FROM transactions
        WHERE LOWER(category) = LOWER($1)
        ORDER BY created_date DESC
        LIMIT $2
        """,
        category,
        limit,
    )
    return [_row_to_result(row) for row in rows]


async def get_transactions_by_user(
    pool: asyncpg.Pool, user_id: str, limit: int
) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        """
         SELECT audit_id, correlation_id, user_id, amount, category, timestamp,
             actor_id, actor_type, source_ip, user_agent, request_id,
             ingest_path, ruleset_hash, is_fraud, triggered_rules,
             created_date, updated_date
        FROM transactions
        WHERE user_id = $1
         ORDER BY created_date DESC
        LIMIT $2
        """,
        user_id,
        limit,
    )
    return [_row_to_result(row) for row in rows]


def _row_to_result(row: asyncpg.Record) -> dict[str, Any]:
    triggered_rules = row["triggered_rules"]
    if isinstance(triggered_rules, str):
        triggered_rules = json.loads(triggered_rules)

    return {
        "audit_id": row["audit_id"],
        "correlation_id": row["correlation_id"],
        "user_id": row["user_id"],
        "amount": row["amount"],
        "category": row["category"],
        "timestamp": row["timestamp"],
        "actor_id": row["actor_id"],
        "actor_type": row["actor_type"],
        "source_ip": row["source_ip"],
        "user_agent": row["user_agent"],
        "request_id": row["request_id"],
        "ingest_path": row["ingest_path"],
        "ruleset_hash": row["ruleset_hash"],
        "is_fraud": row["is_fraud"],
        "triggered_rules": triggered_rules,
        "created_date": row["created_date"].isoformat(),
        "updated_date": row["updated_date"].isoformat(),
    }


def _row_to_audit_event(row: asyncpg.Record) -> dict[str, Any]:
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)

    return {
        "id": row["id"],
        "audit_id": row["audit_id"],
        "event_type": row["event_type"],
        "event_time": row["event_time"].isoformat(),
        "actor_id": row["actor_id"],
        "actor_type": row["actor_type"],
        "source_ip": row["source_ip"],
        "user_agent": row["user_agent"],
        "request_id": row["request_id"],
        "service_name": row["service_name"],
        "payload": payload,
        "prev_hash": row["prev_hash"],
        "event_hash": row["event_hash"],
    }
