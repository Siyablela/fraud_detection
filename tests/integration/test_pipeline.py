"""End to end: Kafka -> worker -> PostgreSQL -> API, plus DLQ and redelivery."""

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
import sqlalchemy as sa
from confluent_kafka import Consumer, Producer, TopicPartition
from prometheus_client import REGISTRY

from app.api import create_app
from app.cache import RuleSetProvider, create_redis_client, rule_set_cache_key
from app.database import create_rule_set_version
from app.dlq import DeadLetterPublisher
from app.kafka_config import build_consumer_config, build_producer_config
from app.processing import TransactionProcessor
from app.rules import DEFAULT_RULE_SET_DEFINITION, content_hash, parse_rule_set_definition
from app.settings import Settings, clear_settings_cache
from app.worker import FraudWorker
from tests.conftest import encode_event, make_event
from tests.integration.conftest import DLQ_TOPIC, TOPIC
from tests.integration.test_repository import RESTORE_V1, TAMPER_V1

pytestmark = [pytest.mark.integration, pytest.mark.anyio]

T0 = datetime(2026, 10, 29, 9, tzinfo=UTC)


@pytest.fixture
def pipeline_settings(integration_env, kafka_bootstrap, tmp_path):
    os.environ.update(
        {
            "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap,
            "KAFKA_CONSUMER_GROUP_ID": f"it-{uuid4()}",
            "WORKER_HEARTBEAT_PATH": str(tmp_path / "heartbeat"),
            "WORKER_RETRY_INITIAL_BACKOFF_SECONDS": "0.05",
            "WORKER_RETRY_MAX_BACKOFF_SECONDS": "0.1",
        }
    )
    clear_settings_cache()
    return Settings.from_env()


def produce(settings: Settings, events: list[tuple[bytes, bytes]]) -> None:
    producer = Producer(build_producer_config(settings))
    for key, value in events:
        producer.produce(TOPIC, key=key, value=value)
    assert producer.flush(10) == 0


class RunningWorker:
    def __init__(self, settings, session_factory):
        self.settings = settings
        self.consumer = Consumer(build_consumer_config(settings))
        self.producer = Producer(build_producer_config(settings))
        self.redis = create_redis_client(settings)
        rule_sets = RuleSetProvider(session_factory, self.redis, "fraud-default", 0, 60)
        self.worker = FraudWorker(
            settings,
            self.consumer,
            TransactionProcessor(session_factory, rule_sets),
            DeadLetterPublisher(self.producer, DLQ_TOPIC),
        )

    async def __aenter__(self):
        self.task = asyncio.create_task(self.worker.run())
        return self

    async def __aexit__(self, *exc):
        self.worker.stop()
        await asyncio.wait_for(self.task, timeout=60)
        await self.redis.aclose()


async def wait_for(predicate, timeout=60):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.25)
    raise AssertionError("condition not met in time")


async def count(session_factory, sql, **params) -> int:
    async with session_factory() as session:
        return int(await session.scalar(sa.text(sql), params))


def read_dlq(settings: Settings, expected: int) -> list:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"dlq-{uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([DLQ_TOPIC])
    messages = []
    for _ in range(60):
        message = consumer.poll(1.0)
        if message is not None and message.error() is None:
            messages.append(message)
        if len(messages) >= expected:
            break
    consumer.close()
    return messages


async def test_events_are_scored_stored_dead_lettered_and_served(pipeline_settings, session_factory, make_token):
    velocity_user = str(uuid4())
    velocity_events = [
        make_event(user_id=velocity_user, amount="10.00", occurred_at=(T0 + timedelta(seconds=i)).isoformat())
        for i in range(6)
    ]
    high_value = make_event(amount="15000.00", category="crypto")
    duplicate = make_event(amount="20.00")
    poison = b'{"schema_version": 1, "event_type": "TransactionCreated", "amount": "oops"}'

    events = [(velocity_user.encode(), encode_event(event)) for event in velocity_events]
    events += [
        (high_value["user_id"].encode(), encode_event(high_value)),
        (duplicate["user_id"].encode(), encode_event(duplicate)),
        (duplicate["user_id"].encode(), encode_event(duplicate)),  # re-published
        (b"poison", poison),
    ]
    produce(pipeline_settings, events)

    async with RunningWorker(pipeline_settings, session_factory):
        await wait_for(lambda: _done(session_factory, expected_transactions=8, expected_history=9))
    letters = read_dlq(pipeline_settings, expected=1)

    # Velocity: only the sixth event within 60s trips the count rule.
    async with session_factory() as session:
        flags = [
            row[0]
            for row in await session.execute(
                sa.text("SELECT triggered_rules FROM transactions WHERE user_id = :u ORDER BY occurred_at"),
                {"u": velocity_user},
            )
        ]
    assert flags == [[], [], [], [], [], ["VELOCITY_COUNT_EXCEEDED"]]

    # Duplicate publish: one decision, two audit rows.
    assert (
        await count(
            session_factory,
            "SELECT count(*) FROM transaction_history WHERE transaction_id = :t",
            t=duplicate["transaction_id"],
        )
        == 2
    )

    # Poison message: original bytes in the DLQ with failure headers.
    (letter,) = letters
    headers = dict(letter.headers())
    assert letter.value() == poison
    assert headers["dlq.error.class"] == b"permanent"
    assert headers["dlq.source.topic"] == TOPIC.encode()

    # Offsets were committed for every partition that had traffic.
    probe = Consumer(
        {
            "bootstrap.servers": pipeline_settings.kafka_bootstrap_servers,
            "group.id": pipeline_settings.kafka_consumer_group_id,
        }
    )
    committed = probe.committed([TopicPartition(TOPIC, p) for p in range(3)], timeout=10)
    probe.close()
    assert sum(tp.offset for tp in committed if tp.offset > 0) == len(events)

    # And the API serves the stored decision.
    os.environ["JWT_AUDIENCE"] = "fraud-api"
    app = create_app()
    async with app.router.lifespan_context(app):
        app.state.rate_limiter = None
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                f"/api/v1/transactions/{high_value['transaction_id']}",
                headers={"Authorization": f"Bearer {make_token()}"},
            )
            page = await client.get(
                "/api/v1/transactions",
                params={"user_id": velocity_user, "limit": 4},
                headers={"Authorization": f"Bearer {make_token()}"},
            )
    assert response.status_code == 200
    body = response.json()
    assert (body["is_fraud"], body["verdict"], body["risk_score"]) == (True, "REJECTED", 90)
    assert body["triggered_rules"] == ["HIGH_VALUE_THRESHOLD", "RISKY_CATEGORY_LIMIT"]
    assert body["amount"] == "15000.00" and body["category"] == "CRYPTO"
    assert len(page.json()["data"]) == 4 and page.json()["next_cursor"]


async def _done(session_factory, expected_transactions: int, expected_history: int) -> bool:
    transactions = await count(session_factory, "SELECT count(*) FROM transactions")
    history = await count(session_factory, "SELECT count(*) FROM transaction_history")
    return transactions >= expected_transactions and history >= expected_history


async def test_worker_resumes_after_restart_without_reprocessing(pipeline_settings, session_factory):
    # The topic is shared with other tests, so only this test's transactions are counted.
    first, second = make_event(), make_event()
    ids = (first["transaction_id"], second["transaction_id"])
    audit_rows = "SELECT count(*) FROM transaction_history WHERE transaction_id IN (:a, :b)"

    async def audited(expected: int) -> bool:
        return await count(session_factory, audit_rows, a=ids[0], b=ids[1]) >= expected

    produce(pipeline_settings, [(first["user_id"].encode(), encode_event(first))])
    async with RunningWorker(pipeline_settings, session_factory):
        await wait_for(lambda: audited(1))

    # Same consumer group: the restarted worker must continue after the committed offset.
    produce(pipeline_settings, [(second["user_id"].encode(), encode_event(second))])
    async with RunningWorker(pipeline_settings, session_factory):
        await wait_for(lambda: audited(2))
        await asyncio.sleep(2)  # give any (incorrect) redelivery time to show up
    assert await count(session_factory, audit_rows, a=ids[0], b=ids[1]) == 2


async def test_api_rule_publish_updates_shared_cache(integration_env, session_factory, make_token):
    from app.rules import DEFAULT_RULE_SET_DEFINITION

    app = create_app()
    definition = json.loads(json.dumps(DEFAULT_RULE_SET_DEFINITION))
    definition["rules"][0]["condition"] = {">=": [{"var": "amount"}, 8000]}
    async with app.router.lifespan_context(app):
        app.state.rate_limiter = None
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            created = await client.post(
                "/api/v1/rule-sets/fraud-default/versions",
                json={"description": "lower threshold", "definition": definition, "expected_current_version": 1},
                headers={"Authorization": f"Bearer {make_token(scopes='fraud:rules:write', sub='rules-admin')}"},
            )
            dry_run = await client.post(
                "/api/v1/rule-sets/fraud-default/evaluate",
                json={"user_id": str(uuid4()), "amount": "8000.00", "currency": "ZAR", "category": "GROCERIES"},
                headers={"Authorization": f"Bearer {make_token(scopes='fraud:rules:read')}"},
            )
            ready = await client.get("/ready")
        cached = json.loads(await app.state.redis.get("fraud:rule-sets:fraud-default:active"))
    assert created.status_code == 201, created.text
    assert created.json()["created_by"] == "rules-admin"
    assert cached["version"] == 2
    assert dry_run.json()["triggered_rules"] == ["HIGH_VALUE_THRESHOLD"]
    assert dry_run.json()["rule_set"] == "fraud-default@v2"
    assert ready.status_code == 200


def metric(name: str, **labels) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


async def test_tampered_rule_set_stops_scoring_without_dead_lettering(pipeline_settings, session_factory):
    rejected_before = metric("fraud_rule_set_rejected_total", rule_set_key="fraud-default")
    async with session_factory() as session, session.begin():
        await session.execute(sa.text(TAMPER_V1))
    redis = create_redis_client(pipeline_settings)
    await redis.delete(rule_set_cache_key("fraud-default"))  # no valid copy to hide behind
    try:
        async with RunningWorker(pipeline_settings, session_factory):
            event = make_event(occurred_at=T0.isoformat())
            produce(pipeline_settings, [(event["user_id"].encode(), encode_event(event))])
            stored = "SELECT count(*) FROM transactions WHERE transaction_id = :t"

            # The worker keeps retrying the message: refused and not scored.
            async def refused_repeatedly():
                return metric("fraud_rule_set_rejected_total", rule_set_key="fraud-default") >= rejected_before + 3

            await wait_for(refused_repeatedly)
            assert await count(session_factory, stored, t=event["transaction_id"]) == 0

            # Publishing a valid version is the fix; the held message is then scored with it.
            # That it can still be scored proves it was held, not dead-lettered: a
            # dead-lettered message's offset is committed and it is never seen again.
            definition = parse_rule_set_definition(DEFAULT_RULE_SET_DEFINITION)
            await create_rule_set_version(
                session_factory,
                key="fraud-default",
                definition=definition,
                definition_hash=content_hash(definition),
                description="repair after tampering",
                created_by="admin",
                expected_current_version=1,
            )

            async def scored():
                return await count(session_factory, stored, t=event["transaction_id"]) == 1

            await wait_for(scored)
            version = "SELECT rules_version FROM transactions WHERE transaction_id = :t"
            async with session_factory() as session:
                assert await session.scalar(sa.text(version), {"t": event["transaction_id"]}) == "fraud-default@v2"
    finally:
        async with session_factory() as session, session.begin():
            await session.execute(sa.text(RESTORE_V1))
        await redis.delete(rule_set_cache_key("fraud-default"))
        await redis.aclose()
