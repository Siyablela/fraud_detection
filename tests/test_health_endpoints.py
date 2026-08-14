import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import health, ready


class HealthEndpointTests(unittest.TestCase):
    def test_liveness_endpoint_returns_ok(self):
        app = FastAPI()
        app.get("/live")(lambda: {"status": "ok"})

        with TestClient(app) as client:
            response = client.get("/live")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_readiness_uses_database_ping(self):
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(database=object())))

        with patch("app.api.ping_database", new=AsyncMock(return_value=None)):
            response = asyncio.run(ready(request))

        self.assertEqual(response, {"status": "ready"})

    def test_health_returns_503_when_database_is_unavailable(self):
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(database=object())))

        with patch("app.api.ping_database", new=AsyncMock(side_effect=RuntimeError("down"))):
            with self.assertRaises(Exception) as context:
                asyncio.run(health(request))

        self.assertEqual(context.exception.status_code, 503)
        self.assertEqual(context.exception.detail, "Database is unavailable")


if __name__ == "__main__":
    unittest.main()
