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

from app.database import TransactionRecord, _build_history_values, _row_to_result


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


if __name__ == "__main__":
    unittest.main()
