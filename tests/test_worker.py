import asyncio
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

from app.worker import commit_processed_message


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


if __name__ == "__main__":
    unittest.main()