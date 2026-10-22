import os
import unittest
import jwt
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.testclient import TestClient
from app.rate_limit import RateLimitMiddleware

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


def _bearer(subject: str) -> dict[str, str]:
    """Build an Authorization header with a token for 'subject'."""
    token = jwt.encode({"sub": subject}, "test-key", algorithm="HS256")
    return {"Authorization": f"Bearer {token}"}


def _rate_limited_app(max_requests: int = 2) -> FastAPI:
    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, max_requests=max_requests, window_seconds=60)
    
    @app.get("/x")
    def protected():
        return {"ok": True}
        
    @app.get("/health")
    def health():
        return {"status": "ok"}
        
    return app


def _cors_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["https://allowed.example"],
        allow_methods=["GET"],
        allow_headers=["Authorization"],
    )

    @app.get("/x")
    def protected():
        return {"ok": True}
    
    return app

import unittest
from fastapi.testclient import TestClient

# Assuming the previous setup functions and classes are imported or in scope:
# from your_module import _rate_limited_app, _cors_app, _bearer


class RateLimitMiddlewareTests(unittest.TestCase):

    def test_requests_blocked_after_limit(self):
        with TestClient(_rate_limited_app(max_requests=2)) as client:
            self.assertEqual(client.get("/x").status_code, 200)
            self.assertEqual(client.get("/x").status_code, 200)
            blocked = client.get("/x")
            self.assertEqual(blocked.status_code, 429)
            # Corrected syntax from blocked, headers to blocked.headers
            self.assertIn("Retry-After", blocked.headers)

    def test_health_endpoints_are_exempt(self):
        with TestClient(_rate_limited_app(max_requests=1)) as client:
            for _ in range(5):
                self.assertEqual(client.get("/health").status_code, 200)

    def test_distinct_principals_get_independent_budgets_on_same_ip(self):
        # Same client IP (TestClient), different JWT subjects → separate buckets,
        # so one busy caller does not throttle another on an internal network.
        with TestClient(_rate_limited_app(max_requests=1)) as client:
            self.assertEqual(
                client.get("/x", headers=_bearer("user-A")).status_code, 200
            )
            # user-A's bucket is now empty.
            self.assertEqual(
                client.get("/x", headers=_bearer("user-A")).status_code, 429
            )
            # user-B is unaffected.
            self.assertEqual(
                client.get("/x", headers=_bearer("user-B")).status_code, 200
            )


class CorsMiddlewareTests(unittest.TestCase):

    def test_allowed_origin_is_echoed(self):
        with TestClient(_cors_app()) as client:
            response = client.get(
                "/x", headers={"Origin": "https://allowed.example"}
            )
            self.assertEqual(
                response.headers.get("access-control-allow-origin"),
                "https://allowed.example",
            )

    def test_disallowed_origin_is_not_echoed(self):
        with TestClient(_cors_app()) as client:
            response = client.get("/x", headers={"Origin": "https://evil.example"})
            # Corrected from assertINone(response, headers...) to assertIsNone
            self.assertIsNone(response.headers.get("access-control-allow-origin"))


class CreateAppWiringTests(unittest.TestCase):

    def test_create_app_registers_cors_and_rate_limit(self):
        # Corrected OCR typo 'appapi' to 'app' or your main package module
        from app import create_app

        app = create_app()

        # Fixed syntax error for list comprehension and accessing middleware names safely
        middleware_names = [entry.cls.__name__ for entry in app.user_middleware]

        self.assertIn("CORSMiddleware", middleware_names)
        self.assertIn("RateLimitMiddleware", middleware_names)


if __name__ == "_main_":
    unittest.main()