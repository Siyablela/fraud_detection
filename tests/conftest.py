"""Shared fixtures: a complete test environment, signing keys and token factory."""

import json
import os
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

_TEST_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_TEST_PUBLIC_PEM = _TEST_KEY.public_key().public_bytes(
    serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
)
_PUBLIC_KEY_PATH = Path(__file__).parent / ".test-jwt-public.pem"
_PUBLIC_KEY_PATH.write_bytes(_TEST_PUBLIC_PEM)

ISSUER = "http://keycloak.test/realms/fraud"
AUDIENCE = "fraud-api"

BASE_ENV = {
    "DATABASE_URL": "postgresql://test:test@localhost:5432/test",
    "REDIS_URL": "redis://localhost:6379/0",
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "KAFKA_TOPIC_NAME": "transactions",
    "KAFKA_CONSUMER_GROUP_ID": "fraud-worker-test",
    "JWT_ISSUER": ISSUER,
    "JWT_AUDIENCE": AUDIENCE,
    "JWT_PUBLIC_KEY_PATH": str(_PUBLIC_KEY_PATH),
    "RATE_LIMIT_ENABLED": "false",
}
for _name, _value in BASE_ENV.items():
    os.environ.setdefault(_name, _value)


@pytest.fixture(autouse=True)
def _fresh_settings():
    """Every test starts from BASE_ENV with no cached settings or verifier."""
    from app.security import get_token_verifier
    from app.settings import clear_settings_cache

    saved = dict(os.environ)
    os.environ.update(BASE_ENV)
    clear_settings_cache()
    get_token_verifier.cache_clear()
    yield
    os.environ.clear()
    os.environ.update(saved)
    clear_settings_cache()
    get_token_verifier.cache_clear()


@pytest.fixture
def rsa_private_key():
    return _TEST_KEY


@pytest.fixture
def make_token():
    """Build a signed access token; override any claim with keyword arguments."""

    def factory(scopes: str = "fraud:transactions:read", key=None, algorithm: str = "RS256", headers=None, **claims):
        now = int(time.time())
        payload = {
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": "analyst-1",
            "iat": now,
            "exp": now + 300,
            "scope": scopes,
            **claims,
        }
        payload = {name: value for name, value in payload.items() if value is not None}
        return jwt.encode(payload, key or _TEST_KEY, algorithm=algorithm, headers=headers)

    return factory


def make_event(**overrides) -> dict:
    event = {
        "schema_version": 1,
        "event_type": "TransactionCreated",
        "transaction_id": str(uuid4()),
        "user_id": str(uuid4()),
        "amount": "125.50",
        "currency": "ZAR",
        "category": "GROCERIES",
        "occurred_at": "2026-10-29T12:00:00Z",
    }
    event.update(overrides)
    return {name: value for name, value in event.items() if value is not None}


def encode_event(event: dict) -> bytes:
    return json.dumps(event).encode("utf-8")


def default_rule_set(version: int = 1):
    from app.rules import DEFAULT_RULE_SET_DEFINITION, RuleSet, content_hash, parse_rule_set_definition

    definition = parse_rule_set_definition(DEFAULT_RULE_SET_DEFINITION)
    return RuleSet(
        key="fraud-default",
        version=version,
        definition=definition,
        content_hash=content_hash(definition),
        description="default",
        created_by="test",
        created_at=datetime(2026, 10, 29, tzinfo=UTC),
    )


def make_decision(**overrides):
    from app.database import TransactionDecision

    values = {
        "transaction_id": uuid4(),
        "user_id": uuid4(),
        "amount": Decimal("125.50"),
        "currency": "ZAR",
        "category": "GROCERIES",
        "occurred_at": datetime(2026, 10, 29, 12, tzinfo=UTC),
        "risk_score": 0,
        "verdict": "APPROVED",
        "is_fraud": False,
        "triggered_rules": [],
        "rules_version": "fraud-default@v1",
        "rules_hash": "a" * 64,
        "correlation_id": None,
    }
    values.update(overrides)
    return TransactionDecision(**values)


def uuid(value: int) -> UUID:
    return UUID(int=value)
