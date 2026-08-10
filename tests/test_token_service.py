import asyncio
import os
import unittest

import httpx

from app.settings import clear_settings_cache
from app.token_service import (
    IdentityProviderRejectedError,
    IdentityProviderUnavailableError,
    InvalidClientCredentialsError,
    InvalidCredentialsError,
    KeycloakTokenService,
)


os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost/db")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("KAFKA_TOPIC_NAME", "transactions")
os.environ.setdefault("KAFKA_CONSUMER_GROUP_ID", "fraud-worker")
os.environ.setdefault("FRAUD_RULES_CONFIG_PATH", "rules.json")
os.environ["JWT_ISSUER"] = "http://localhost:8081/realms/fraud"
os.environ["JWT_AUDIENCE"] = "fraud-api"
os.environ.setdefault("DB_POOL_MIN_SIZE", "1")
os.environ.setdefault("DB_POOL_MAX_SIZE", "5")
os.environ.setdefault("DEFAULT_HIGH_VALUE_THRESHOLD", "100.0")
os.environ.setdefault("DEFAULT_RESTRICTED_CATEGORIES", '{"GAMBLING": 100.0}')
os.environ.setdefault("KEYCLOAK_TOKEN_CLIENT_ID", "fraud-cli")


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


class _FakeAsyncClient:
    def __init__(self, response: _FakeResponse):
        self._response = response
        self.captured_data = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def post(self, url, data, headers):
        self.captured_data = {"url": url, "data": data, "headers": headers}
        return self._response


class TokenServiceTests(unittest.TestCase):
    def tearDown(self) -> None:
        os.environ.pop("KEYCLOAK_TOKEN_CLIENT_SECRET", None)
        os.environ.pop("KEYCLOAK_SERVICE_CLIENT_SECRET", None)
        clear_settings_cache()

    def test_exchange_token_calls_keycloak_token_endpoint(self):
        async def run_test() -> None:
            fake_response = _FakeResponse(
                200,
                {
                    "access_token": "abc",
                    "token_type": "Bearer",
                    "expires_in": 300,
                    "scope": "fraud:transactions:read",
                },
            )
            fake_client = _FakeAsyncClient(fake_response)

            class _ClientFactory:
                def __call__(self, timeout: float):
                    return fake_client

            original_client = httpx.AsyncClient
            httpx.AsyncClient = _ClientFactory()
            try:
                service = KeycloakTokenService()
                result = await service.exchange_token(
                    username="tester",
                    password="secret",
                    client_id=None,
                    scope="fraud:transactions:read",
                )
            finally:
                httpx.AsyncClient = original_client

            self.assertEqual(result.access_token, "abc")
            self.assertEqual(
                fake_client.captured_data["url"],
                "http://localhost:8081/realms/fraud/protocol/openid-connect/token",
            )
            self.assertEqual(fake_client.captured_data["data"]["client_id"], "fraud-cli")
            self.assertEqual(fake_client.captured_data["data"]["scope"], "fraud:transactions:read")

        asyncio.run(run_test())

    def test_exchange_token_uses_client_secret_when_set(self):
        async def run_test() -> None:
            os.environ["KEYCLOAK_TOKEN_CLIENT_SECRET"] = "secret-value"
            clear_settings_cache()

            fake_response = _FakeResponse(200, {"access_token": "abc"})
            fake_client = _FakeAsyncClient(fake_response)

            class _ClientFactory:
                def __call__(self, timeout: float):
                    return fake_client

            original_client = httpx.AsyncClient
            httpx.AsyncClient = _ClientFactory()
            try:
                service = KeycloakTokenService()
                await service.exchange_token(
                    username="tester",
                    password="secret",
                    client_id="custom-client",
                    scope=None,
                )
            finally:
                httpx.AsyncClient = original_client

            self.assertEqual(fake_client.captured_data["data"]["client_id"], "custom-client")
            self.assertEqual(fake_client.captured_data["data"]["client_secret"], "secret-value")

        asyncio.run(run_test())

    def test_exchange_service_token_uses_client_credentials_grant(self):
        async def run_test() -> None:
            os.environ["KEYCLOAK_SERVICE_CLIENT_SECRET"] = "service-secret"
            clear_settings_cache()

            fake_response = _FakeResponse(200, {"access_token": "svc-abc"})
            fake_client = _FakeAsyncClient(fake_response)

            class _ClientFactory:
                def __call__(self, timeout: float):
                    return fake_client

            original_client = httpx.AsyncClient
            httpx.AsyncClient = _ClientFactory()
            try:
                service = KeycloakTokenService()
                result = await service.exchange_service_token(
                    client_id=None,
                    client_secret=None,
                    scope="fraud:transactions:read",
                )
            finally:
                httpx.AsyncClient = original_client

            self.assertEqual(result.access_token, "svc-abc")
            self.assertEqual(fake_client.captured_data["data"]["grant_type"], "client_credentials")
            self.assertEqual(
                fake_client.captured_data["data"]["client_id"],
                "fraud-service-cli",
            )
            self.assertEqual(
                fake_client.captured_data["data"]["client_secret"],
                "service-secret",
            )

        asyncio.run(run_test())

    def test_exchange_token_maps_upstream_errors(self):
        async def run_test() -> None:
            class _FailingClient:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return None

                async def post(self, url, data, headers):
                    raise httpx.RequestError("boom")

            class _ClientFactoryUnavailable:
                def __call__(self, timeout: float):
                    return _FailingClient()

            original_client = httpx.AsyncClient
            httpx.AsyncClient = _ClientFactoryUnavailable()
            try:
                with self.assertRaises(IdentityProviderUnavailableError):
                    service = KeycloakTokenService()
                    await service.exchange_token(
                        username="tester",
                        password="secret",
                        client_id=None,
                        scope=None,
                    )
            finally:
                httpx.AsyncClient = original_client

            fake_invalid_grant = _FakeResponse(400, {"error": "invalid_grant"})
            fake_client = _FakeAsyncClient(fake_invalid_grant)

            class _ClientFactoryInvalidGrant:
                def __call__(self, timeout: float):
                    return fake_client

            httpx.AsyncClient = _ClientFactoryInvalidGrant()
            try:
                with self.assertRaises(InvalidCredentialsError):
                    service = KeycloakTokenService()
                    await service.exchange_token(
                        username="tester",
                        password="bad",
                        client_id=None,
                        scope=None,
                    )
            finally:
                httpx.AsyncClient = original_client

            fake_rejected = _FakeResponse(400, {"error": "invalid_request"})
            fake_client = _FakeAsyncClient(fake_rejected)

            class _ClientFactoryRejected:
                def __call__(self, timeout: float):
                    return fake_client

            httpx.AsyncClient = _ClientFactoryRejected()
            try:
                with self.assertRaises(IdentityProviderRejectedError):
                    service = KeycloakTokenService()
                    await service.exchange_token(
                        username="tester",
                        password="secret",
                        client_id=None,
                        scope=None,
                    )
            finally:
                httpx.AsyncClient = original_client

            fake_invalid_client = _FakeResponse(401, {"error": "invalid_client"})
            fake_client = _FakeAsyncClient(fake_invalid_client)

            class _ClientFactoryInvalidClient:
                def __call__(self, timeout: float):
                    return fake_client

            httpx.AsyncClient = _ClientFactoryInvalidClient()
            try:
                with self.assertRaises(InvalidClientCredentialsError):
                    service = KeycloakTokenService()
                    await service.exchange_service_token(
                        client_id="svc",
                        client_secret="bad",
                        scope=None,
                    )
            finally:
                httpx.AsyncClient = original_client

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
