import asyncio
import json

import fakeredis
import pytest

from app import cache
from app.cache import RuleSetProvider, rule_set_cache_key, write_rule_set_cache
from app.errors import RuleSetNotFoundError
from tests.conftest import default_rule_set


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


@pytest.fixture
def database(monkeypatch):
    """Replace the PostgreSQL lookup; ``state['result']`` is returned or raised."""
    state = {"result": default_rule_set(1), "calls": 0}

    async def fake_latest(_session_factory, key):
        state["calls"] += 1
        if isinstance(state["result"], BaseException):
            raise state["result"]
        return state["result"]

    monkeypatch.setattr(cache, "get_latest_rule_set", fake_latest)
    return state


def provider(redis_client, clock):
    return RuleSetProvider(
        session_factory=None,
        redis_client=redis_client,
        key="fraud-default",
        local_ttl_seconds=5,
        redis_ttl_seconds=60,
        clock=clock,
    )


def test_database_result_is_cached_locally_and_in_redis(database):
    redis_client, clock = fakeredis.FakeAsyncRedis(decode_responses=True), Clock()
    rule_sets = provider(redis_client, clock)

    async def run():
        first = await rule_sets.get_active()
        second = await rule_sets.get_active()
        stored = json.loads(await redis_client.get(rule_set_cache_key("fraud-default")))
        ttl = await redis_client.ttl(rule_set_cache_key("fraud-default"))
        return first, second, stored, ttl

    first, second, stored, ttl = asyncio.run(run())
    assert first is second
    assert database["calls"] == 1
    assert stored["version"] == 1 and 0 < ttl <= 60


def test_new_version_in_redis_is_picked_up_after_local_ttl(database):
    redis_client, clock = fakeredis.FakeAsyncRedis(decode_responses=True), Clock()
    rule_sets = provider(redis_client, clock)

    async def run():
        await rule_sets.get_active()
        await write_rule_set_cache(redis_client, default_rule_set(2), 60)
        before_ttl = await rule_sets.get_active()
        clock.now = 6
        after_ttl = await rule_sets.get_active()
        return before_ttl, after_ttl

    before_ttl, after_ttl = asyncio.run(run())
    assert (before_ttl.version, after_ttl.version) == (1, 2)
    assert database["calls"] == 1


def test_redis_outage_falls_back_to_database(database):
    class BrokenRedis:
        async def get(self, key):
            raise ConnectionError("down")

        async def set(self, *args, **kwargs):
            raise ConnectionError("down")

    rule_set = asyncio.run(provider(BrokenRedis(), Clock()).get_active())
    assert rule_set.version == 1


def test_database_outage_serves_last_known_rule_set(database):
    redis_client, clock = fakeredis.FakeAsyncRedis(decode_responses=True), Clock()
    rule_sets = provider(redis_client, clock)

    async def run():
        await rule_sets.get_active()
        await redis_client.flushall()
        database["result"] = ConnectionError("db down")
        clock.now = 10
        return await rule_sets.get_active()

    assert asyncio.run(run()).version == 1


def test_database_outage_without_cached_copy_raises(database):
    database["result"] = ConnectionError("db down")
    with pytest.raises(ConnectionError):
        asyncio.run(provider(fakeredis.FakeAsyncRedis(decode_responses=True), Clock()).get_active())


def test_missing_rule_set_raises_not_found(database):
    database["result"] = RuleSetNotFoundError("none")
    with pytest.raises(RuleSetNotFoundError):
        asyncio.run(provider(fakeredis.FakeAsyncRedis(decode_responses=True), Clock()).get_active())


def test_cache_entries_from_an_incompatible_schema_are_ignored(database):
    redis_client = fakeredis.FakeAsyncRedis(decode_responses=True)

    async def run():
        await redis_client.set(rule_set_cache_key("fraud-default"), json.dumps({"schema": 99}))
        return await provider(redis_client, Clock()).get_active()

    assert asyncio.run(run()).version == 1
    assert database["calls"] == 1


def test_cache_write_failure_is_reported_not_raised():
    class BrokenRedis:
        async def set(self, *args, **kwargs):
            raise TimeoutError()

    assert asyncio.run(write_rule_set_cache(BrokenRedis(), default_rule_set(), 60)) is False
