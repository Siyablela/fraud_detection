import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/fraud_detection")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("KAFKA_TOPIC_NAME", "transactions_topic")
os.environ.setdefault("KAFKA_CONSUMER_GROUP_ID", "fraud-worker-group")
os.environ.setdefault("FRAUD_RULES_CONFIG_PATH", "tests/rules.json")
os.environ.setdefault("JWT_ISSUER", "https://auth.example.com/")
os.environ.setdefault("JWT_AUDIENCE", "fraud-api")
os.environ.setdefault("DB_POOL_MIN_SIZE", "1")
os.environ.setdefault("DB_POOL_MAX_SIZE", "10")
os.environ.setdefault("VELOCITY_WINDOW_SECONDS", "60")
os.environ.setdefault("DEFAULT_HIGH_VALUE_THRESHOLD", "10000")
os.environ.setdefault("DEFAULT_VELOCITY_THRESHOLD", "5")
os.environ.setdefault("DEFAULT_RESTRICTED_CATEGORIES", '{"GAMBLING": 5000, "CRYPTO": 5000}')

from app.config import load_rules_config
from app.rule import evaluate_transaction, Transaction


class RulesConfigTests(unittest.TestCase):
    def test_load_rules_config_from_file(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(
                {
                    "high_value_threshold": 1000,
                    "velocity_threshold": 2,
                    "restricted_categories": {"GAMBLING": 1000, "CRYPTO": 500},
                },
                handle,
            )
            temp_path = handle.name

        try:
            config = load_rules_config(temp_path)
            self.assertEqual(config.high_value_threshold, 1000)
            self.assertEqual(config.velocity_threshold, 2)
            self.assertEqual(config.restricted_categories["GAMBLING"], 1000)
        finally:
            os.remove(temp_path)

    def test_evaluate_transaction_uses_configured_thresholds(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(
                {
                    "high_value_threshold": 1000,
                    "velocity_threshold": 2,
                    "restricted_categories": {"GAMBLING": 1000, "CRYPTO": 500},
                },
                handle,
            )
            temp_path = handle.name

        try:
            os.environ["FRAUD_RULES_CONFIG_PATH"] = temp_path
            transaction = Transaction(
                transaction_id="tx-1",
                user_id="user-1",
                amount=1500,
                category="GAMBLING",
            )
            result = evaluate_transaction(transaction, history_count=2)
            self.assertTrue(result["is_fraud"])
            self.assertIn("HIGH_VALUE_TRANSACTION", result["triggered_rules"])
            self.assertIn("RISKY_CATEGORY_LIMIT", result["triggered_rules"])
        finally:
            os.environ.pop("FRAUD_RULES_CONFIG_PATH", None)
            os.remove(temp_path)


if __name__ == "__main__":
    unittest.main()
