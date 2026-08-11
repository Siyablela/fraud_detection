import importlib
import os
import sys
import tempfile
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
        original_jwt_issuer = os.environ.get("JWT_ISSUER")
        original_jwt_audience = os.environ.get("JWT_AUDIENCE")
        try:
            os.environ["JWT_ISSUER"] = "https://auth.example.com/"
            os.environ["JWT_AUDIENCE"] = "fraud-api"
            os.environ.setdefault("DB_POOL_MIN_SIZE", "1")
            os.environ.setdefault("DB_POOL_MAX_SIZE", "10")
            os.environ.setdefault("DEFAULT_HIGH_VALUE_THRESHOLD", "10000")
            os.environ.setdefault("DEFAULT_RESTRICTED_CATEGORIES", '{"GAMBLING": 5000, "CRYPTO": 5000}')
            os.environ.setdefault("KAFKA_PRODUCER_ACKS", "all")
            os.environ.setdefault("KAFKA_PRODUCER_ENABLE_IDEMPOTENCE", "true")

            import app.settings as settings_module

            settings_module = importlib.reload(settings_module)
            settings_module.clear_settings_cache()

            self.assertEqual(settings_module.get_settings().kafka_producer_acks, "all")
            self.assertTrue(settings_module.get_settings().kafka_producer_enable_idempotence)
        finally:
            if original_jwt_issuer is None:
                os.environ.pop("JWT_ISSUER", None)
            else:
                os.environ["JWT_ISSUER"] = original_jwt_issuer
            if original_jwt_audience is None:
                os.environ.pop("JWT_AUDIENCE", None)
            else:
                os.environ["JWT_AUDIENCE"] = original_jwt_audience

    def test_settings_reads_secret_values_from_file_envs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            database_url_path = Path(tmpdir) / "database-url"
            database_url_path.write_text("postgresql://vault:pass@localhost:5432/fraud_detection", encoding="utf-8")
            token_secret_path = Path(tmpdir) / "token-secret"
            token_secret_path.write_text("vault-token-secret", encoding="utf-8")
            service_secret_path = Path(tmpdir) / "service-secret"
            service_secret_path.write_text("vault-service-secret", encoding="utf-8")

            original_database_url = os.environ.get("DATABASE_URL")
            original_database_url_file = os.environ.get("DATABASE_URL_FILE")
            original_token_secret = os.environ.get("KEYCLOAK_TOKEN_CLIENT_SECRET")
            original_token_secret_file = os.environ.get("KEYCLOAK_TOKEN_CLIENT_SECRET_FILE")
            original_service_secret = os.environ.get("KEYCLOAK_SERVICE_CLIENT_SECRET")
            original_service_secret_file = os.environ.get("KEYCLOAK_SERVICE_CLIENT_SECRET_FILE")
            original_jwt_issuer = os.environ.get("JWT_ISSUER")
            original_jwt_audience = os.environ.get("JWT_AUDIENCE")
            try:
                os.environ.pop("DATABASE_URL", None)
                os.environ["DATABASE_URL_FILE"] = str(database_url_path)
                os.environ.pop("KEYCLOAK_TOKEN_CLIENT_SECRET", None)
                os.environ["KEYCLOAK_TOKEN_CLIENT_SECRET_FILE"] = str(token_secret_path)
                os.environ.pop("KEYCLOAK_SERVICE_CLIENT_SECRET", None)
                os.environ["KEYCLOAK_SERVICE_CLIENT_SECRET_FILE"] = str(service_secret_path)
                os.environ["JWT_ISSUER"] = "https://auth.example.com/"
                os.environ["JWT_AUDIENCE"] = "fraud-api"
                os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
                os.environ["KAFKA_TOPIC_NAME"] = "transactions_topic"
                os.environ["KAFKA_CONSUMER_GROUP_ID"] = "fraud-worker-group"
                os.environ["FRAUD_RULES_CONFIG_PATH"] = "tests/rules.json"
                os.environ["DB_POOL_MIN_SIZE"] = "1"
                os.environ["DB_POOL_MAX_SIZE"] = "10"
                os.environ["DEFAULT_HIGH_VALUE_THRESHOLD"] = "10000"
                os.environ["DEFAULT_RESTRICTED_CATEGORIES"] = '{"GAMBLING": 5000, "CRYPTO": 5000}'

                import app.settings as settings_module

                settings_module = importlib.reload(settings_module)
                settings_module.clear_settings_cache()

                resolved = settings_module.get_settings()
                self.assertEqual(resolved.database_url, "postgresql://vault:pass@localhost:5432/fraud_detection")
                self.assertEqual(resolved.keycloak_token_client_secret, "vault-token-secret")
                self.assertEqual(resolved.keycloak_service_client_secret, "vault-service-secret")
            finally:
                if original_database_url is None:
                    os.environ.pop("DATABASE_URL", None)
                else:
                    os.environ["DATABASE_URL"] = original_database_url
                if original_database_url_file is None:
                    os.environ.pop("DATABASE_URL_FILE", None)
                else:
                    os.environ["DATABASE_URL_FILE"] = original_database_url_file
                if original_token_secret is None:
                    os.environ.pop("KEYCLOAK_TOKEN_CLIENT_SECRET", None)
                else:
                    os.environ["KEYCLOAK_TOKEN_CLIENT_SECRET"] = original_token_secret
                if original_token_secret_file is None:
                    os.environ.pop("KEYCLOAK_TOKEN_CLIENT_SECRET_FILE", None)
                else:
                    os.environ["KEYCLOAK_TOKEN_CLIENT_SECRET_FILE"] = original_token_secret_file
                if original_service_secret is None:
                    os.environ.pop("KEYCLOAK_SERVICE_CLIENT_SECRET", None)
                else:
                    os.environ["KEYCLOAK_SERVICE_CLIENT_SECRET"] = original_service_secret
                if original_service_secret_file is None:
                    os.environ.pop("KEYCLOAK_SERVICE_CLIENT_SECRET_FILE", None)
                else:
                    os.environ["KEYCLOAK_SERVICE_CLIENT_SECRET_FILE"] = original_service_secret_file
                if original_jwt_issuer is None:
                    os.environ.pop("JWT_ISSUER", None)
                else:
                    os.environ["JWT_ISSUER"] = original_jwt_issuer
                if original_jwt_audience is None:
                    os.environ.pop("JWT_AUDIENCE", None)
                else:
                    os.environ["JWT_AUDIENCE"] = original_jwt_audience


if __name__ == "__main__":
    unittest.main()
