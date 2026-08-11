import unittest
import uuid

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from app.observability import (
    attach_correlation_id,
    ensure_correlation_id,
    get_correlation_id,
    get_request_id,
    install_fastapi_observability,
)


class ObservabilityTests(unittest.TestCase):
    def test_ensure_correlation_id_generates_a_uuid_when_none_is_bound(self):
        correlation_id = ensure_correlation_id()
        self.assertTrue(correlation_id)
        uuid.UUID(correlation_id)
        self.assertEqual(correlation_id, get_correlation_id())

    def test_attach_correlation_id_preserves_existing_payload_value(self):
        payload = {"event": "ready", "correlation_id": "incoming-correlation"}
        result = attach_correlation_id(payload)
        self.assertEqual(result["correlation_id"], "incoming-correlation")

    def test_request_and_correlation_ids_are_returned_and_bound(self):
        app = FastAPI()
        install_fastapi_observability(app, "test-service")

        @app.get("/trace")
        async def trace(request: Request):
            return {
                "request_id": get_request_id(),
                "correlation_id": get_correlation_id(),
            }

        with TestClient(app) as client:
            response = client.get("/trace", headers={"x-request-id": "caller-request"})

        self.assertEqual(response.headers["x-request-id"], "caller-request")
        self.assertTrue(response.headers["x-correlation-id"])
        self.assertNotEqual(response.headers["x-correlation-id"], "caller-request")
        uuid.UUID(response.headers["x-correlation-id"])
        self.assertEqual(response.json()["request_id"], "caller-request")
        self.assertEqual(response.json()["correlation_id"], response.headers["x-correlation-id"])

    def test_incoming_correlation_id_header_is_ignored_and_new_uuid_is_generated(self):
        app = FastAPI()
        install_fastapi_observability(app, "test-service")

        @app.get("/trace")
        async def trace(request: Request):
            return {
                "request_id": get_request_id(),
                "correlation_id": get_correlation_id(),
            }

        with TestClient(app) as client:
            response = client.get(
                "/trace",
                headers={
                    "x-request-id": "caller-request",
                    "x-correlation-id": "123e4567-e89b-12d3-a456-426614174000",
                },
            )

        self.assertEqual(response.headers["x-request-id"], "caller-request")
        self.assertTrue(response.headers["x-correlation-id"])
        uuid.UUID(response.headers["x-correlation-id"])
        self.assertNotEqual(response.headers["x-correlation-id"], "123e4567-e89b-12d3-a456-426614174000")
        self.assertEqual(response.json()["correlation_id"], response.headers["x-correlation-id"])


if __name__ == "__main__":
    unittest.main()
