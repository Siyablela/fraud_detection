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
            "KAFKA_SSL_CA_LOCATION",
            "KAFKA_SSL_CERTIFICATE_LOCATION",
            "KAFKA_SSL_KEY_LOCATION",
            "KAFKA_SSL_KEY_PASSWORD",
        ]:
            os.environ.pop(key, None)
        clear_settings_cache()

    def test_settings_do_not_expose_mtls_fields(self):
        self.assertIn("kafka_ssl_ca_location", Settings.model_fields)
        self.assertIn("kafka_ssl_certificate_location", Settings.model_fields)
        self.assertIn("kafka_ssl_key_location", Settings.model_fields)
        self.assertIn("kafka_ssl_key_password", Settings.model_fields)

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
        self.assertNotIn("ssl.ca.location", config)

    def test_build_kafka_client_config_preserves_sasl_ssl_protocol(self):
        os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_SSL"
        os.environ["JWT_ISSUER"] = "http://localhost:8081/realms/fraud"
        os.environ["KEYCLOAK_SERVICE_CLIENT_ID"] = "fraud-service-cli"
        os.environ["KEYCLOAK_SERVICE_CLIENT_SECRET"] = "service-secret"
        clear_settings_cache()

        config = build_kafka_client_config()

        self.assertEqual(config["security.protocol"], "SASL_SSL")
        self.assertEqual(config["sasl.mechanisms"], "OAUTHBEARER")

    def test_build_kafka_client_config_layers_tls_material_on_sasl_ssl(self):
        os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_SSL"
        os.environ["JWT_ISSUER"] = "http://localhost:8081/realms/fraud"
        os.environ["KEYCLOAK_SERVICE_CLIENT_ID"] = "fraud-service-cli"
        os.environ["KEYCLOAK_SERVICE_CLIENT_SECRET"] = "service-secret"
        os.environ["KAFKA_SSL_CA_LOCATION"] = "/certs/ca.pem"
        os.environ["KAFKA_SSL_CERTIFICATE_LOCATION"] = "/certs/client.pem"
        os.environ["KAFKA_SSL_KEY_LOCATION"] = "/certs/client.key"
        clear_settings_cache()

        config = build_kafka_client_config()

        self.assertEqual(config["security.protocol"], "SASL_SSL")
        self.assertEqual(config["sasl.mechanisms"], "OAUTHBEARER")
        self.assertEqual(config["ssl.ca.location"], "/certs/ca.pem")
        self.assertEqual(config["ssl.certificate.location"], "/certs/client.pem")
        self.assertEqual(config["ssl.key.location"], "/certs/client.key")

    def test_build_kafka_client_config_supports_mutual_tls_without_sasl(self):
        os.environ["KAFKA_SECURITY_PROTOCOL"] = "SSL"
        os.environ["JWT_ISSUER"] = "http://localhost:8081/realms/fraud"
        os.environ["KEYCLOAK_SERVICE_CLIENT_ID"] = "fraud-service-cli"
        os.environ["KEYCLOAK_SERVICE_CLIENT_SECRET"] = "service-secret"
        os.environ["KAFKA_SSL_CA_LOCATION"] = "/certs/ca.pem"
        os.environ["KAFKA_SSL_CERTIFICATE_LOCATION"] = "/certs/client.pem"
        os.environ["KAFKA_SSL_KEY_LOCATION"] = "/certs/client.key"
        clear_settings_cache()

        config = build_kafka_client_config()

        self.assertEqual(config["security.protocol"], "SSL")
        self.assertNotIn("sasl.mechanisms", config)
        self.assertEqual(config["ssl.ca.location"], "/certs/ca.pem")
        self.assertEqual(config["ssl.certificate.location"], "/certs/client.pem")
        self.assertEqual(config["ssl.key.location"], "/certs/client.key")


if __name__ == "_main_":
    unittest.main()
