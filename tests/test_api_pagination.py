import asyncio
import os
import unittest
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost/db")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("KAFKA_TOPIC_NAME", "transactions")
os.environ.setdefault("KAFKA_CONSUMER_GROUP_ID", "fraud-worker")
os.environ.setdefault("FRAUD_RULES_CONFIG_PATH", "rules.json")
os.environ.setdefault("JWT_ISSUER", "issuer")
os.environ.setdefault("JWT_AUDIENCE", "aud")
os.environ.setdefault("DB_POOL_MIN_SIZE", "1")
os.environ.setdefault("DB_POOL_MAX_SIZE", "5")
os.environ.setdefault("DEFAULT_RESTRICTED_CATEGORIES", '{"GAMBLING": 100.0}')

from app import api


class PaginationTests(unittest.TestCase):
    def test_get_transactions_by_category_returns_paginated_payload(self):
        async def run_test() -> None:
            captured: dict[str, int] = {}

            async def fake_find_by_category(session_factory, category, offset, limit):
                captured["offset"] = offset
                captured["limit"] = limit
                return [{"transaction_id": "tx-1"}], 1

            original = api.find_by_category
            api.find_by_category = fake_find_by_category
            try:
                request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(database=object())))
                result = await api.get_transactions_by_category(
                    "travel", request=request, page=2, page_size=10, _principal=None
                )
            finally:
                api.find_by_category = original

            self.assertEqual(captured["offset"], 10)
            self.assertEqual(captured["limit"], 10)
            self.assertEqual(result["page"], 2)
            self.assertEqual(result["page_size"], 10)
            self.assertEqual(result["count"], 1)
            self.assertEqual(result["total_count"], 1)
            self.assertEqual(result["total_pages"], 1)
            self.assertEqual(result["has_previous"], True)
            self.assertEqual(result["has_more"], False)
            self.assertEqual(result["previous_page"], 1)
            self.assertEqual(result["next_page"], None)
            self.assertEqual(result["data"], [{"transaction_id": "tx-1"}])

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
