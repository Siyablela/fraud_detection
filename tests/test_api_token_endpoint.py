import asyncio
import os
import unittest

from fastapi import HTTPException

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost/db")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("KAFKA_TOPIC_NAME", "transactions")
os.environ.setdefault("KAFKA_CONSUMER_GROUP_ID", "fraud-worker")
os.environ.setdefault("FRAUD_RULES_CONFIG_PATH", "rules.json")
os.environ.setdefault("JWT_ISSUER", "http://localhost:8081/realms/fraud")
os.environ.setdefault("JWT_AUDIENCE", "fraud-api")
os.environ.setdefault("DB_POOL_MIN_SIZE", "1")
os.environ.setdefault("DB_POOL_MAX_SIZE", "5")
os.environ.setdefault("DEFAULT_HIGH_VALUE_THRESHOLD", "100.0")
os.environ.setdefault("DEFAULT_RESTRICTED_CATEGORIES", '{"GAMBLING": 100.0}')

from app import api
from app.settings import clear_settings_cache
from app.token_service import (
    IdentityProviderRejectedError,
    IdentityProviderUnavailableError,
    InvalidClientCredentialsError,
    InvalidCredentialsError,
    TokenExchangeResult,
)


class ApiTokenEndpointTests(unittest.TestCase):
    def tearDown(self) -> None:
        for key in [
            "AUTH_TOKEN_ENDPOINT_ENABLED",
            "KEYCLOAK_TOKEN_CLIENT_ID",
            "KEYCLOAK_TOKEN_CLIENT_SECRET",
        ]:
            os.environ.pop(key, None)
        clear_settings_cache()

    def test_exchange_token_returns_access_token_payload(self):
        async def run_test() -> None:
            os.environ["AUTH_TOKEN_ENDPOINT_ENABLED"] = "true"
            os.environ["KEYCLOAK_TOKEN_CLIENT_ID"] = "fraud-cli"
            clear_settings_cache()

            class _FakeService:
                async def exchange_token(self, *, username, password, client_id, scope):
                    self.captured = {
                        "username": username,
                        "password": password,
                        "client_id": client_id,
                        "scope": scope,
                    }
                    return TokenExchangeResult(
                        access_token="token-123",
                        token_type="Bearer",
                        expires_in=300,
                        scope="fraud:transactions:read",
                    )

            fake_service = _FakeService()

            class _ServiceFactory:
                def __call__(self):
                    return fake_service

            original_factory = api.KeycloakTokenService
            api.KeycloakTokenService = _ServiceFactory()
            try:
                result = await api.exchange_token_for_testing(
                    api.TokenExchangeRequest(username="tester", password="secret")
                )
            finally:
                api.KeycloakTokenService = original_factory

            self.assertEqual(result.access_token, "token-123")
            self.assertEqual(result.scope, "fraud:transactions:read")
            self.assertEqual(fake_service.captured["username"], "tester")
            self.assertEqual(fake_service.captured["password"], "secret")
            self.assertIsNone(fake_service.captured["client_id"])

        asyncio.run(run_test())

    def test_exchange_token_rejects_when_endpoint_disabled(self):
        async def run_test() -> None:
            os.environ["AUTH_TOKEN_ENDPOINT_ENABLED"] = "false"
            clear_settings_cache()

            with self.assertRaises(HTTPException) as context:
                await api.exchange_token_for_testing(
                    api.TokenExchangeRequest(username="tester", password="secret")
                )

            self.assertEqual(context.exception.status_code, 404)

        asyncio.run(run_test())

    def test_exchange_token_maps_invalid_grant_to_unauthorized(self):
        async def run_test() -> None:
            os.environ["AUTH_TOKEN_ENDPOINT_ENABLED"] = "true"
            clear_settings_cache()

            class _FakeService:
                async def exchange_token(self, *, username, password, client_id, scope):
                    raise InvalidCredentialsError()

            class _ServiceFactory:
                def __call__(self):
                    return _FakeService()

            original_factory = api.KeycloakTokenService
            api.KeycloakTokenService = _ServiceFactory()
            try:
                with self.assertRaises(HTTPException) as context:
                    await api.exchange_token_for_testing(
                        api.TokenExchangeRequest(username="tester", password="wrong")
                    )
            finally:
                api.KeycloakTokenService = original_factory

            self.assertEqual(context.exception.status_code, 401)
            self.assertEqual(context.exception.detail, "Invalid username or password.")

        asyncio.run(run_test())

    def test_exchange_token_maps_upstream_errors_to_bad_gateway(self):
        async def run_test() -> None:
            os.environ["AUTH_TOKEN_ENDPOINT_ENABLED"] = "true"
            clear_settings_cache()

            class _UnavailableService:
                async def exchange_token(self, *, username, password, client_id, scope):
                    raise IdentityProviderUnavailableError()

            class _RejectedService:
                async def exchange_token(self, *, username, password, client_id, scope):
                    raise IdentityProviderRejectedError()

            original_factory = api.KeycloakTokenService
            try:
                class _UnavailableFactory:
                    def __call__(self):
                        return _UnavailableService()

                api.KeycloakTokenService = _UnavailableFactory()
                with self.assertRaises(HTTPException) as unavailable:
                    await api.exchange_token_for_testing(
                        api.TokenExchangeRequest(username="tester", password="secret")
                    )
                self.assertEqual(unavailable.exception.status_code, 502)
                self.assertEqual(
                    unavailable.exception.detail,
                    "Unable to reach identity provider token endpoint.",
                )

                class _RejectedFactory:
                    def __call__(self):
                        return _RejectedService()

                api.KeycloakTokenService = _RejectedFactory()
                with self.assertRaises(HTTPException) as rejected:
                    await api.exchange_token_for_testing(
                        api.TokenExchangeRequest(username="tester", password="secret")
                    )
                self.assertEqual(rejected.exception.status_code, 502)
                self.assertEqual(
                    rejected.exception.detail,
                    "Identity provider rejected token request.",
                )
            finally:
                api.KeycloakTokenService = original_factory

        asyncio.run(run_test())

    def test_exchange_service_token_returns_access_token_payload(self):
        async def run_test() -> None:
            os.environ["AUTH_TOKEN_ENDPOINT_ENABLED"] = "true"
            clear_settings_cache()

            class _FakeService:
                async def exchange_service_token(self, *, client_id, client_secret, scope):
                    self.captured = {
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "scope": scope,
                    }
                    return TokenExchangeResult(
                        access_token="service-token-123",
                        token_type="Bearer",
                        expires_in=300,
                    )

            fake_service = _FakeService()

            class _ServiceFactory:
                def __call__(self):
                    return fake_service

            original_factory = api.KeycloakTokenService
            api.KeycloakTokenService = _ServiceFactory()
            try:
                result = await api.exchange_service_token_for_testing(
                    api.ServiceTokenRequest(client_id="svc-client", client_secret="svc-secret")
                )
            finally:
                api.KeycloakTokenService = original_factory

            self.assertEqual(result.access_token, "service-token-123")
            self.assertEqual(fake_service.captured["client_id"], "svc-client")
            self.assertEqual(fake_service.captured["client_secret"], "svc-secret")

        asyncio.run(run_test())

    def test_exchange_service_token_maps_invalid_client_to_unauthorized(self):
        async def run_test() -> None:
            os.environ["AUTH_TOKEN_ENDPOINT_ENABLED"] = "true"
            clear_settings_cache()

            class _FakeService:
                async def exchange_service_token(self, *, client_id, client_secret, scope):
                    raise InvalidClientCredentialsError()

            class _ServiceFactory:
                def __call__(self):
                    return _FakeService()

            original_factory = api.KeycloakTokenService
            api.KeycloakTokenService = _ServiceFactory()
            try:
                with self.assertRaises(HTTPException) as context:
                    await api.exchange_service_token_for_testing(
                        api.ServiceTokenRequest(client_id="svc-client", client_secret="bad")
                    )
            finally:
                api.KeycloakTokenService = original_factory

            self.assertEqual(context.exception.status_code, 401)
            self.assertEqual(context.exception.detail, "Invalid client credentials.")

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
