import importlib
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class SettingsTests(unittest.TestCase):
    def test_settings_exposes_kafka_producer_configuration(self):
        os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/fraud_detection")
        os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        os.environ.setdefault("KAFKA_TOPIC_NAME", "transactions_topic")
        os.environ.setdefault("KAFKA_CONSUMER_GROUP_ID", "fraud-worker-group")
        os.environ.setdefault("FRAUD_RULES_CONFIG_PATH", "tests/rules.json")
        os.environ.setdefault("JWT_ISSUER", "https://auth.example.com/")
        os.environ.setdefault("JWT_AUDIENCE", "fraud-api")
        os.environ.setdefault("DB_POOL_MIN_SIZE", "1")
        os.environ.setdefault("DB_POOL_MAX_SIZE", "10")
        os.environ.setdefault("DEFAULT_RESTRICTED_CATEGORIES", '{"GAMBLING": 5000, "CRYPTO": 5000}')
        os.environ.setdefault("KAFKA_PRODUCER_ACKS", "all")
        os.environ.setdefault("KAFKA_PRODUCER_ENABLE_IDEMPOTENCE", "true")

        import app.settings as settings_module

        settings_module = importlib.reload(settings_module)
        settings_module.clear_settings_cache()

        self.assertEqual(settings_module.get_settings().kafka_producer_acks, "all")
        self.assertTrue(settings_module.get_settings().kafka_producer_enable_idempotence)


if __name__ == "__main__":
    unittest.main()
