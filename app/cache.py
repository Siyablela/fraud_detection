"""Redis client factory and the active rule-set provider.

Lookup order for the active rule set: in-process copy (``RULE_LOCAL_CACHE_TTL_SECONDS``)
-> Redis (``RULE_REDIS_CACHE_TTL_SECONDS``) -> PostgreSQL. Redis shields the
database from every pod refreshing every few seconds; the in-process copy keeps
Redis off the per-message hot path.

Staleness bound: publishing a version writes PostgreSQL, then overwrites the Redis
entry, so pods normally switch within the local TTL. If that Redis write fails,
pods may keep the previous version for up to local TTL + Redis TTL.
"""

import json
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

import redis.asyncio as aioredis

from app.database import SessionFactory, get_latest_rule_set
from app.observability import get_logger, record_rule_cache_lookup
from app.rules import RuleSet, RuleSetDefinition
from app.settings import Settings

logger = get_logger(__name__)

_CACHE_ENTRY_SCHEMA = 1


def create_redis_client(settings: Settings) -> aioredis.Redis:
    """Create a pooled client. The pool is shared for the life of the process."""
    return aioredis.Redis.from_url(
        settings.redis_url,
        max_connections=settings.redis_max_connections,
        socket_timeout=settings.redis_socket_timeout_seconds,
        socket_connect_timeout=settings.redis_socket_timeout_seconds,
        health_check_interval=30,
        decode_responses=True,
    )


async def ping_redis(client: aioredis.Redis) -> None:
    await client.ping()


def rule_set_cache_key(key: str) -> str:
    return f"fraud:rule-sets:{key}:active"


def _serialise(rule_set: RuleSet) -> str:
    return json.dumps(
        {
            "schema": _CACHE_ENTRY_SCHEMA,
            "key": rule_set.key,
            "version": rule_set.version,
            "definition": rule_set.definition.model_dump(mode="json"),
            "content_hash": rule_set.content_hash,
            "description": rule_set.description,
            "created_by": rule_set.created_by,
            "created_at": rule_set.created_at.isoformat(),
        }
    )


def _deserialise(payload: str) -> RuleSet | None:
    """Return ``None`` for entries written by an incompatible version of this code."""
    entry: dict[str, Any] = json.loads(payload)
    if entry.get("schema") != _CACHE_ENTRY_SCHEMA:
        return None
    return RuleSet(
        key=entry["key"],
        version=int(entry["version"]),
        definition=RuleSetDefinition.model_validate(entry["definition"]),
        content_hash=entry["content_hash"],
        description=entry["description"],
        created_by=entry["created_by"],
        created_at=datetime.fromisoformat(entry["created_at"]),
    )


class RuleSetProvider:
    """Resolve the active rule set for one key, with layered caching."""

    def __init__(
        self,
        session_factory: SessionFactory,
        redis_client: aioredis.Redis,
        key: str,
        local_ttl_seconds: float,
        redis_ttl_seconds: int,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._session_factory = session_factory
        self._redis = redis_client
        self._key = key
        self._local_ttl = local_ttl_seconds
        self._redis_ttl = redis_ttl_seconds
        self._clock = clock
        self._cached: RuleSet | None = None
        self._cached_at = float("-inf")

    @property
    def key(self) -> str:
        return self._key

    async def get_active(self) -> RuleSet:
        """Return the active rule set.

        Raises:
            RuleSetNotFoundError: No rule set exists and none was cached before.
            Exception: Database errors when there is no previously loaded copy.
        """
        if self._cached is not None and self._clock() - self._cached_at < self._local_ttl:
            record_rule_cache_lookup("local")
            return self._cached

        rule_set = await self._read_redis()
        if rule_set is not None:
            record_rule_cache_lookup("redis")
        else:
            try:
                rule_set = await get_latest_rule_set(self._session_factory, self._key)
            except Exception as exc:
                if self._cached is None:
                    raise
                # Keep scoring with the last known rules rather than failing every
                # message; the database outage itself is surfaced by its own alert.
                logger.warning(
                    "rule_set_refresh_failed_serving_stale",
                    rule_set=self._cached.label,
                    error_type=type(exc).__name__,
                )
                self._cached_at = self._clock()
                return self._cached
            record_rule_cache_lookup("database")
            await self._write_redis(rule_set)

        if self._cached is None or self._cached.version != rule_set.version:
            logger.info("rule_set_activated", rule_set=rule_set.label, content_hash=rule_set.content_hash)
        self._cached, self._cached_at = rule_set, self._clock()
        return rule_set

    async def _read_redis(self) -> RuleSet | None:
        try:
            payload = await self._redis.get(rule_set_cache_key(self._key))
            return _deserialise(payload) if payload else None
        except Exception as exc:
            # Redis is an optimisation here; fall through to PostgreSQL.
            logger.warning("rule_cache_read_failed", rule_set_key=self._key, error_type=type(exc).__name__)
            return None

    async def _write_redis(self, rule_set: RuleSet) -> None:
        await write_rule_set_cache(self._redis, rule_set, self._redis_ttl)


async def write_rule_set_cache(client: aioredis.Redis, rule_set: RuleSet, ttl_seconds: int) -> bool:
    """Overwrite the shared active-version entry; returns ``False`` if Redis failed.

    Failure is logged, not raised: PostgreSQL is the source of truth and the entry
    expires on its own, which bounds how long a missed update can persist.
    """
    try:
        await client.set(rule_set_cache_key(rule_set.key), _serialise(rule_set), ex=ttl_seconds)
    except Exception as exc:
        logger.warning("rule_cache_write_failed", rule_set_key=rule_set.key, error_type=type(exc).__name__)
        return False
    return True
