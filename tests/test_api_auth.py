import asyncio
import os
import unittest

import jwt
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

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
from app import security
from app.security import AuthenticatedPrincipal


class ApiAuthTests(unittest.TestCase):
    def tearDown(self) -> None:
        security._get_token_verifier.cache_clear()

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

    def test_get_current_principal_rejects_missing_bearer_token(self):
        async def run_test() -> None:
            with self.assertRaises(HTTPException) as context:
                await security.get_current_principal(None)

            self.assertEqual(context.exception.status_code, 401)
            self.assertEqual(context.exception.detail, "Missing bearer access token.")

        asyncio.run(run_test())

    def test_get_current_principal_rejects_expired_token(self):
        async def run_test() -> None:
            class ExpiredVerifier:
                def decode(self, token: str):
                    raise jwt.ExpiredSignatureError("expired")

            original = security._get_token_verifier
            security._get_token_verifier = lambda: ExpiredVerifier()
            try:
                credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials="expired-token")
                with self.assertRaises(HTTPException) as context:
                    await security.get_current_principal(credentials)

                self.assertEqual(context.exception.status_code, 401)
                self.assertEqual(context.exception.detail, "Access token has expired.")
            finally:
                security._get_token_verifier = original

        asyncio.run(run_test())

    def test_get_current_principal_rejects_missing_subject_claim(self):
        async def run_test() -> None:
            class MissingSubjectVerifier:
                def decode(self, token: str):
                    return {"scope": "fraud:transactions:read"}

            original = security._get_token_verifier
            security._get_token_verifier = lambda: MissingSubjectVerifier()
            try:
                credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials="valid-but-missing-sub")
                with self.assertRaises(HTTPException) as context:
                    await security.get_current_principal(credentials)

                self.assertEqual(context.exception.status_code, 401)
                self.assertEqual(context.exception.detail, "Access token is missing the subject claim.")
            finally:
                security._get_token_verifier = original

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