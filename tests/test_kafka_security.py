import os
import unittest

from app.kafka_security import build_kafka_client_config
from app.settings import Settings, clear_settings_cache


class KafkaSecurityTests(unittest.TestCase):
    def tearDown(self) -> None:
        for key in [
            "KAFKA_SECURITY_PROTOCOL",
            "KEYCLOAK_SERVICE_CLIENT_ID",
            "KEYCLOAK_SERVICE_CLIENT_SECRET",
            "JWT_ISSUER",
        ]:
            os.environ.pop(key, None)
        clear_settings_cache()

    def test_settings_do_not_expose_mtls_fields(self):
        self.assertNotIn("kafka_ssl_truststore_path", Settings.model_fields)
        self.assertNotIn("kafka_ssl_keystore_cert_path", Settings.model_fields)
        self.assertNotIn("kafka_ssl_keystore_key_path", Settings.model_fields)
        self.assertNotIn("kafka_ssl_keystore_password", Settings.model_fields)

    def test_build_kafka_client_config_uses_keycloak_oauthbearer(self):
        os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_PLAINTEXT"
        os.environ["JWT_ISSUER"] = "http://localhost:8081/realms/fraud"
        os.environ["KEYCLOAK_SERVICE_CLIENT_ID"] = "fraud-service-cli"
        os.environ["KEYCLOAK_SERVICE_CLIENT_SECRET"] = "service-secret"
        clear_settings_cache()

        config = build_kafka_client_config()

        self.assertEqual(config["security.protocol"], "SASL_PLAINTEXT")
        self.assertEqual(config["sasl.mechanisms"], "OAUTHBEARER")
        self.assertEqual(
            config["sasl.oauthbearer.token.endpoint.url"],
            "http://localhost:8081/realms/fraud/protocol/openid-connect/token",
        )
        self.assertEqual(config["sasl.oauthbearer.client.id"], "fraud-service-cli")
        self.assertEqual(config["sasl.oauthbearer.client.secret"], "service-secret")
        self.assertTrue(callable(config["oauth_cb"]))
