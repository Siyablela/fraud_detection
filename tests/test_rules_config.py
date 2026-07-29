import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import load_rules_config
from app.rule import evaluate_transaction, Transaction


class RulesConfigTests(unittest.TestCase):
    def test_load_rules_config_from_file(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(
                {
                    "high_value_threshold": 1000,
                    "restricted_categories": {"GAMBLING": 1000, "CRYPTO": 500},
                },
                handle,
            )
            temp_path = handle.name

        try:
            config = load_rules_config(temp_path)
            self.assertEqual(config.high_value_threshold, 1000)
            self.assertEqual(config.restricted_categories["GAMBLING"], 1000)
        finally:
            os.remove(temp_path)

    def test_evaluate_transaction_uses_configured_thresholds(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(
                {
                    "high_value_threshold": 1000,
                    "restricted_categories": {"GAMBLING": 1000, "CRYPTO": 500},
                },
                handle,
            )
            temp_path = handle.name

        try:
            os.environ["FRAUD_RULES_CONFIG_PATH"] = temp_path
            transaction = Transaction(
                audit_id="audit-1",
                correlation_id="corr-1",
                user_id="user-1",
                amount=1500,
                category="GAMBLING",
            )
            result = evaluate_transaction(transaction)
            self.assertTrue(result["is_fraud"])
            self.assertIn("HIGH_VALUE_TRANSACTION", result["triggered_rules"])
            self.assertIn("RISKY_CATEGORY_LIMIT", result["triggered_rules"])
        finally:
            os.environ.pop("FRAUD_RULES_CONFIG_PATH", None)
            os.remove(temp_path)


if __name__ == "__main__":
    unittest.main()
