from decimal import Decimal
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault(
    "DATABASE_URL", "postgresql://user:pass@localhost:5432/fraud_detection"
)
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("KAFKA_TOPIC_NAME", "transactions_topic")
os.environ.setdefault("KAFKA_CONSUMER_GROUP_ID", "fraud-worker-group")
os.environ.setdefault("FRAUD_RULES_CONFIG_PATH", "tests/rules.json")
os.environ.setdefault("JWT_ISSUER", "https://auth.example.com/")
os.environ.setdefault("JWT_AUDIENCE", "fraud-api")
os.environ.setdefault("DB_POOL_MIN_SIZE", "1")
os.environ.setdefault("DB_POOL_MAX_SIZE", "10")
os.environ.setdefault("DEFAULT_HIGH_VALUE_THRESHOLD", "10000")
os.environ.setdefault(
    "DEFAULT_RESTRICTED_CATEGORIES", '{"GAMBLING": 5000, "CRYPTO": 5000}'
)

import app.rule as rule_module
from app.config import load_rules_config
from app.rule import evaluate_transaction, Transaction


class RulesConfigTests(unittest.TestCase):
    def test_load_rules_config_from_file(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(
                {
                    "rules_version": "2026.10.24-test",
                    "high_value_threshold": 1000,
                    "restricted_categories": {"GAMBLING": 1000, "CRYPTO": 500},
                },
                handle,
            )
            temp_path = handle.name

        try:
            config = load_rules_config(temp_path)
            self.assertEqual(config.rules_version, "2026.10.24-test")
            self.assertTrue(config.rules_hash)
            self.assertEqual(config.high_value_threshold, 1000)
            self.assertEqual(config.restricted_categories["GAMBLING"], 1000)
        finally:
            os.remove(temp_path)

    def test_evaluate_transaction_uses_configured_thresholds(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(
                {
                    "rules_version": "2026.10.24-eval",
                    "high_value_threshold": 1000,
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
            result = evaluate_transaction(transaction)
            self.assertTrue(result["is_fraud"])
            self.assertIn("HIGH_VALUE_THRESHOLD", result["triggered_rules"])
            self.assertIn("RISKY_CATEGORY_LIMIT", result["triggered_rules"])
            self.assertEqual(result["rules_version"], "2026.10.24-eval")
            self.assertTrue(result["rules_hash"])
        finally:
            os.environ.pop("FRAUD_RULES_CONFIG_PATH", None)
            os.remove(temp_path)

    def test_evaluate_transaction_flags_high_value_threshold(self):
        transaction = Transaction(
            transaction_id="tx-high-value",
            user_id="user-2",
            amount=20000,
            category="GROCERIES",
        )

        result = evaluate_transaction(transaction)

        self.assertTrue(result["is_fraud"])
        self.assertEqual(result["triggered_rules"], ["HIGH_VALUE_THRESHOLD"])


# Assuming these exist in your application code:
# from app.models import Transaction
# from app.rules import evaluate_transaction
# import app.rules as rule_module


class FraudRulesTests(unittest.TestCase):

    def test_evaluate_transaction_without_velocity_context_is_stateless(self):
        # With no recent-activity context the velocity rules must never fire,
        # preserving the original two-rule behaviour for stateless callers.
        transaction = Transaction(
            transaction_id="tx-stateless",
            user_id="user-g",
            amount=10,
            category="GROCERIES",
        )
        result = evaluate_transaction(transaction)
        self.assertFalse(result["is_fraud"])
        self.assertEqual(result["triggered_rules"], [])

    def _with_rules(self, **overrides):
        """Install a stub rules provider returning a fixed RulesConfig for the test."""
        from app.config import RulesConfig

        config = RulesConfig(
            high_value_threshold=overrides.get("high_value_threshold", 10000),
            restricted_categories=overrides.get("restricted_categories", {}),
            velocity_window_seconds=overrides.get("velocity_window_seconds", 60),
            velocity_max_count=overrides.get("velocity_max_count", 0),
            velocity_max_amount=overrides.get("velocity_max_amount", 0),
        )

        class _StubProvider:
            def get(self_inner):
                return config

        original = rule_module._rules
        rule_module._rules = _StubProvider()
        # Restores the original provider after the test runs
        self.addCleanup(lambda: setattr(rule_module, "_rules", original))

    def test_evaluate_transaction_flags_velocity_count(self):
        self._with_rules(velocity_max_count=3, velocity_max_amount=0)

        transaction = Transaction(
            transaction_id="tx-velocity",
            user_id="user-3",
            amount=10,
            category="GROCERIES",
        )

        # 3 prior + this one = 4, which exceeds the max of 3.
        result = evaluate_transaction(
            transaction, recent_count=3, recent_amount_sum=Decimal("0.00")
        )

        self.assertTrue(result["is_fraud"])
        self.assertIn("VELOCITY_COUNT_EXCEEDED", result["triggered_rules"])

    def test_evaluate_transaction_flags_velocity_amount(self):
        from decimal import Decimal

        self._with_rules(velocity_max_count=0, velocity_max_amount=1000)
        transaction = Transaction(
            transaction_id="tx-velocity-amount",
            user_id="user-4",
            amount=200,
            category="GROCERIES",
        )
        # 900 prior + 200 this = 1100, exceeding the 1000 window cap.
        result = evaluate_transaction(
            transaction, recent_count=1, recent_amount_sum=Decimal("900")
        )
        self.assertTrue(result["is_fraud"])
        self.assertIn("VELOCITY_AMOUNT", result["triggered_rules"])

    def test_velocity_not_triggered_below_thresholds(self):
        from decimal import Decimal

        self._with_rules(velocity_max_count=5, velocity_max_amount=1000)
        transaction = Transaction(
            transaction_id="tx-ok", user_id="user-5", amount=10, category="GROCERIES"
        )
        result = evaluate_transaction(
            transaction, recent_count=1, recent_amount_sum=Decimal("50")
        )
        self.assertFalse(result["is_fraud"])
        self.assertEqual(result["triggered_rules"], [])


if __name__ == "__main__":
    unittest.main()
