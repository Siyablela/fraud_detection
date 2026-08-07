import asyncio
import json
import os
from types import SimpleNamespace
import unittest

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost/db")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("KAFKA_TOPIC_NAME", "transactions")
os.environ.setdefault("KAFKA_CONSUMER_GROUP_ID", "fraud-worker")
os.environ.setdefault("FRAUD_RULES_CONFIG_PATH", "rules.json")
os.environ.setdefault("JWT_ISSUER", "issuer")
os.environ.setdefault("JWT_AUDIENCE", "aud")
os.environ.setdefault("DB_POOL_MIN_SIZE", "1")
os.environ.setdefault("DB_POOL_MAX_SIZE", "5")
os.environ.setdefault("DEFAULT_HIGH_VALUE_THRESHOLD", "100.0")
os.environ.setdefault("DEFAULT_RESTRICTED_CATEGORIES", '{"GAMBLING": 100.0}')

from aiokafka.structs import TopicPartition

from app.worker import commit_processed_message, route_message_to_dlq


class WorkerCommitTests(unittest.TestCase):
    def test_commit_processed_message_commits_next_offset_for_partition(self):
        async def run_test() -> None:
            captured = {}

            class FakeConsumer:
                async def commit(self, offsets):
                    captured.update(offsets)

            msg = SimpleNamespace(topic="transactions", partition=2, offset=41)

            await commit_processed_message(FakeConsumer(), msg)

            self.assertEqual(captured, {TopicPartition("transactions", 2): 42})

        asyncio.run(run_test())

    def test_route_message_to_dlq_publishes_payload_and_commits_offset(self):
        async def run_test() -> None:
            committed = {}
            published = {}

            class FakeConsumer:
                async def commit(self, offsets):
                    committed.update(offsets)

            class FakeProducer:
                async def send_and_wait(self, topic, key, value):
                    published["topic"] = topic
                    published["key"] = key
                    published["value"] = value

            msg = SimpleNamespace(
                topic="transactions",
                partition=1,
                offset=9,
                timestamp=1722435000,
                key=b"tx-key",
            )

            await route_message_to_dlq(
                consumer=FakeConsumer(),
                dlq_producer=FakeProducer(),
                msg=msg,
                raw_event='{"bad":true}',
                error=ValueError("invalid event"),
                topic_name="transactions",
                dlq_topic_name="transactions.dlq",
            )

            payload = json.loads(published["value"].decode("utf-8"))

            self.assertEqual(published["topic"], "transactions.dlq")
            self.assertEqual(published["key"], b"tx-key")
            self.assertEqual(payload["source_topic"], "transactions")
            self.assertEqual(payload["source_offset"], 9)
            self.assertEqual(payload["error_type"], "ValueError")
            self.assertEqual(payload["error_message"], "invalid event")
            self.assertEqual(committed, {TopicPartition("transactions", 1): 10})

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()