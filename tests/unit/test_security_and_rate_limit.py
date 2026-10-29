import asyncio
import json
import os
import time

import fakeredis
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from app import security
from app.rate_limit import RateLimiter
from app.settings import Settings
from tests.conftest import _TEST_PUBLIC_PEM


def authenticate(token: str):
    return asyncio.run(security.get_current_principal(HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)))


def test_valid_token_yields_principal_with_scopes(make_token):
    principal = authenticate(make_token(scopes="fraud:transactions:read fraud:rules:read"))
    assert principal.subject == "analyst-1"
    assert principal.scopes == {"fraud:transactions:read", "fraud:rules:read"}


@pytest.mark.parametrize(
    ("claims", "detail"),
    [
        ({"exp": int(time.time()) - 3600}, "expired"),
        ({"aud": "someone-else"}, "invalid"),
        ({"iss": "https://evil.example"}, "invalid"),
        ({"sub": None}, "invalid"),  # required claim missing
        ({"iat": None}, "invalid"),
    ],
)
def test_tokens_failing_verification_are_401(make_token, claims, detail):
    with pytest.raises(HTTPException) as error:
        authenticate(make_token(**claims))
    assert error.value.status_code == 401
    assert detail in error.value.detail


def test_token_signed_by_another_key_is_rejected(make_token):
    other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    with pytest.raises(HTTPException) as error:
        authenticate(make_token(key=other_key))
    assert error.value.status_code == 401


def test_algorithm_confusion_hs256_with_public_key_is_rejected():
    """A classic attack: sign HS256 using the RSA public key as the HMAC secret."""
    payload = {
        "iss": "http://keycloak.test/realms/fraud",
        "aud": "fraud-api",
        "sub": "attacker",
        "iat": int(time.time()),
        "exp": int(time.time()) + 300,
        "scope": "fraud:rules:write",
    }
    import base64
    import hashlib
    import hmac

    def b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    signing_input = f"{b64(json.dumps({'alg': 'HS256', 'typ': 'JWT'}).encode())}.{b64(json.dumps(payload).encode())}"
    signature = hmac.new(_TEST_PUBLIC_PEM, signing_input.encode(), hashlib.sha256).digest()
    with pytest.raises(HTTPException) as error:
        authenticate(f"{signing_input}.{b64(signature)}")
    assert error.value.status_code == 401


@pytest.mark.parametrize("credentials", [None, HTTPAuthorizationCredentials(scheme="Bearer", credentials="  ")])
def test_missing_token_is_401(credentials):
    with pytest.raises(HTTPException) as error:
        asyncio.run(security.get_current_principal(credentials))
    assert error.value.status_code == 401
    assert error.value.headers["WWW-Authenticate"].startswith("Bearer")


class TestJwksVerifier:
    def _verifier(self, rsa_private_key, fetch_log, min_interval="60"):
        os.environ["JWT_JWKS_MIN_REFRESH_INTERVAL_SECONDS"] = min_interval
        jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(rsa_private_key.public_key()))
        jwk.update({"kid": "key-1", "use": "sig", "alg": "RS256"})

        def fetch(url):
            fetch_log.append(url)
            return {"keys": [jwk]}

        return security._JwksTokenVerifier(Settings.from_env(), fetch=fetch)

    def test_known_key_verifies_and_is_cached(self, make_token, rsa_private_key):
        fetches: list[str] = []
        verifier = self._verifier(rsa_private_key, fetches)
        for _ in range(3):
            assert verifier.decode(make_token(headers={"kid": "key-1"}))["sub"] == "analyst-1"
        assert len(fetches) == 1

    def test_unknown_kid_refresh_is_throttled(self, make_token, rsa_private_key):
        fetches: list[str] = []
        verifier = self._verifier(rsa_private_key, fetches)
        verifier.decode(make_token(headers={"kid": "key-1"}))
        for attempt in range(5):
            with pytest.raises(jwt.InvalidTokenError, match="unknown key"):
                verifier.decode(make_token(headers={"kid": f"forged-{attempt}"}))
        assert len(fetches) == 1  # no refetch storm

    def test_token_without_kid_is_rejected(self, make_token, rsa_private_key):
        verifier = self._verifier(rsa_private_key, [])
        with pytest.raises(jwt.InvalidTokenError, match="kid"):
            verifier.decode(make_token())


class TestRateLimiter:
    def _limiter(self, now, max_requests=3, redis_client=None):
        client = redis_client or fakeredis.FakeAsyncRedis()
        return RateLimiter(client, max_requests=max_requests, window_seconds=60, clock=lambda: now[0])

    def test_allows_budget_then_rejects_with_retry_after(self):
        now = [1_000_040.0]  # 20s into the window starting at 1_000_020
        limiter = self._limiter(now)

        async def run():
            for _ in range(3):
                await limiter.enforce("analyst-1")
            with pytest.raises(HTTPException) as error:
                await limiter.enforce("analyst-1")
            return error.value

        error = asyncio.run(run())
        assert error.status_code == 429
        assert error.headers["Retry-After"] == "40"

    def test_budget_is_per_subject_and_resets_each_window(self):
        now = [1_000_020.0]
        limiter = self._limiter(now, max_requests=1)

        async def run():
            await limiter.enforce("a")
            await limiter.enforce("b")
            now[0] += 60
            await limiter.enforce("a")

        asyncio.run(run())

    def test_fails_open_when_redis_is_down(self):
        class BrokenRedis:
            def pipeline(self, transaction=True):
                raise ConnectionError("redis down")

        limiter = RateLimiter(BrokenRedis(), max_requests=1, window_seconds=60)
        asyncio.run(limiter.enforce("a"))
        asyncio.run(limiter.enforce("a"))
