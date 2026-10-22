import os
import unittest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.observability import install_fastapi_observability

# Set environment defaults
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


def _app() -> FastAPI:
    app = FastAPI()
    install_fastapi_observability(app, "test-metrics-svc")

    @app.get("/items/{item_id}")
    def get_item(item_id: str):
        return {"id": item_id}

    return app


class MetricsRouteLabelTests(unittest.TestCase):

    def test_route_label_uses_template_not_raw_id(self):
        with TestClient(_app()) as client:
            # A parameterised request carrying a sensitive-looking identifier.
            self.assertEqual(client.get("/items/secret-txn-123").status_code, 200)

            metrics_body = client.get("/metrics").text

            # The templated route must appear.
            self.assertIn('route="/items/{item_id}"', metrics_body)

            # ...and the raw identifier must NOT leak into any metric label.
            self.assertNotIn("secret-txn-123", metrics_body)

    def test_unmatched_path_collapses_to_bounded_bucket(self):
        with TestClient(_app()) as client:
            self.assertEqual(client.get("/no/such/route-xyz").status_code, 404)

            metrics_body = client.get("/metrics").text

            self.assertIn('route="<unmatched>"', metrics_body)
            self.assertNotIn("route-xyz", metrics_body)


if __name__ == "_main_":
    unittest.main()