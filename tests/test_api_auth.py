import asyncio
import os
import unittest

from fastapi import HTTPException

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

from app.api import require_transaction_read_scope
from app.security import AuthenticatedPrincipal


class ApiAuthTests(unittest.TestCase):
    def test_require_transaction_read_scope_accepts_matching_scope(self):
        async def run_test() -> None:
            principal = AuthenticatedPrincipal(
                subject="user-1",
                scopes=frozenset({"fraud:transactions:read"}),
                claims={"sub": "user-1"},
            )

            result = await require_transaction_read_scope(principal)

            self.assertIs(result, principal)

        asyncio.run(run_test())

    def test_require_transaction_read_scope_rejects_missing_scope(self):
        async def run_test() -> None:
            principal = AuthenticatedPrincipal(
                subject="user-1",
                scopes=frozenset({"other:scope"}),
                claims={"sub": "user-1"},
            )

            with self.assertRaises(HTTPException) as context:
                await require_transaction_read_scope(principal)

            self.assertEqual(context.exception.status_code, 403)
            self.assertEqual(
                context.exception.headers["WWW-Authenticate"],
                'Bearer error="insufficient_scope", scope="fraud:transactions:read"',
            )

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()