import os
import unittest
import uuid

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

from datetime import datetime, timezone
from unittest.mock import patch

from app.database import TransactionRecord, _build_history_values, _row_to_result, save_transaction


class TransactionHistoryTests(unittest.TestCase):
    def test_transaction_record_tracks_audit_fields(self):
        self.assertIn("correlation_id", TransactionRecord.__table__.columns)
        self.assertIn("updated_at", TransactionRecord.__table__.columns)
        self.assertTrue(TransactionRecord.__table__.c["created_at"].type.timezone)
        self.assertTrue(TransactionRecord.__table__.c["updated_at"].type.timezone)

    def test_row_to_result_serializes_utc_datetimes(self):
        row = TransactionRecord(
            transaction_id="tx-1",
            user_id="user-1",
            amount=125.5,
            category="GAMBLING",
            timestamp=1722435000,
            is_fraud=True,
            triggered_rules=["HIGH_VALUE_THRESHOLD"],
            correlation_id="corr-123",
            created_at=datetime(2026, 8, 14, 20, 30, tzinfo=timezone.utc),
            updated_at=datetime(2026, 8, 14, 21, 0, tzinfo=timezone.utc),
        )

        payload = _row_to_result(row)

        self.assertEqual(payload["created_at"], "2026-08-14T20:30:00+00:00")
        self.assertEqual(payload["updated_at"], "2026-08-14T21:00:00+00:00")

    def test_build_history_values_includes_result_and_source_metadata(self):
        result = {
            "transaction_id": "tx-1",
            "user_id": "user-1",
            "amount": 125.5,
            "category": "GAMBLING",
            "timestamp": 1722435000,
            "is_fraud": True,
            "triggered_rules": ["HIGH_VALUE_THRESHOLD"],
        }
        source_metadata = {
            "source_topic": "transactions",
            "source_partition": 2,
            "source_offset": 42,
            "source_timestamp": 1722435001,
        }

        values = _build_history_values(result, source_metadata)

        self.assertEqual(values["transaction_id"], "tx-1")
        self.assertEqual(values["source_topic"], "transactions")
        self.assertEqual(values["source_partition"], 2)
        self.assertEqual(values["source_offset"], 42)
        self.assertEqual(values["source_timestamp"], 1722435001)
        self.assertEqual(values["triggered_rules"], ["HIGH_VALUE_THRESHOLD"])
        self.assertTrue(values["correlation_id"])
        uuid.UUID(values["correlation_id"])

    def test_build_history_values_preserves_explicit_correlation_id(self):
        result = {
            "transaction_id": "tx-2",
            "user_id": "user-2",
            "amount": 77.0,
            "category": "RETAIL",
            "timestamp": 1722436000,
            "is_fraud": False,
            "triggered_rules": [],
        }

        values = _build_history_values(result, correlation_id="corr-456")

        self.assertEqual(values["correlation_id"], "corr-456")

    def test_save_transaction_uses_single_generated_correlation_id_for_transaction_and_history(self):
        result = {
            "transaction_id": "tx-3",
            "user_id": "user-3",
            "amount": 42.5,
            "category": "FOOD",
            "timestamp": 1722437000,
            "is_fraud": False,
            "triggered_rules": [],
        }
        executed = []

        class FakeSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def execute(self, stmt):
                executed.append(stmt)

            async def commit(self):
                return None

        class FakeSessionFactory:
            def __call__(self):
                return FakeSession()

        async def run_test() -> None:
            with patch("app.database.uuid4", side_effect=["generated-correlation-id", "fallback-correlation-id"]):
                with patch("app.database._retry_async", new=lambda operation, **kwargs: operation()):
                    await save_transaction(FakeSessionFactory(), result)

            upsert_stmt = executed[0]
            history_stmt = executed[1]
            upsert_values = upsert_stmt.compile().params
            history_values = history_stmt.compile().params

            self.assertEqual(upsert_values["correlation_id"], "generated-correlation-id")
            self.assertEqual(history_values["correlation_id"], "generated-correlation-id")
            self.assertNotEqual(history_values["correlation_id"], "fallback-correlation-id")

        import asyncio

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
