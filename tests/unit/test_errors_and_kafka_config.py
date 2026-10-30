import os

import asyncpg
import pytest
import redis.exceptions
import sqlalchemy.exc
from pydantic import BaseModel, ValidationError

from app.errors import (
    ConflictingDuplicateError,
    FailureClass,
    InvalidEventError,
    RuleEvaluationError,
    RuleSetNotFoundError,
    TransientError,
    classify_failure,
)
from app.kafka_config import build_consumer_config, build_producer_config
from app.settings import Settings


def _validation_error() -> ValidationError:
    class Model(BaseModel):
        value: int

    try:
        Model(value="x")
    except ValidationError as exc:
        return exc
    raise AssertionError("unreachable")


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (TransientError("x"), FailureClass.TRANSIENT),
        (RuleSetNotFoundError("x"), FailureClass.TRANSIENT),
        (TimeoutError(), FailureClass.TRANSIENT),
        (ConnectionRefusedError(), FailureClass.TRANSIENT),
        (redis.exceptions.ConnectionError(), FailureClass.TRANSIENT),
        (sqlalchemy.exc.OperationalError("SELECT 1", {}, asyncpg.PostgresConnectionError()), FailureClass.TRANSIENT),
        (sqlalchemy.exc.DBAPIError("SELECT 1", {}, Exception(), connection_invalidated=True), FailureClass.TRANSIENT),
        (sqlalchemy.exc.TimeoutError(), FailureClass.TRANSIENT),
        (InvalidEventError("x"), FailureClass.PERMANENT),
        (ConflictingDuplicateError("x"), FailureClass.PERMANENT),
        (_validation_error(), FailureClass.PERMANENT),
        (RuleEvaluationError("x"), FailureClass.UNEXPECTED),
        (KeyError("x"), FailureClass.UNEXPECTED),
        (sqlalchemy.exc.IntegrityError("INSERT", {}, Exception()), FailureClass.UNEXPECTED),
    ],
)
def test_classify_failure(error, expected):
    assert classify_failure(error) is expected


def test_consumer_stores_offsets_manually_for_at_least_once():
    config = build_consumer_config(Settings.from_env())
    assert config["enable.auto.offset.store"] is False
    assert config["enable.auto.commit"] is True
    assert config["group.id"] == "fraud-worker-test"
    assert config["security.protocol"] == "PLAINTEXT"


def test_producer_is_idempotent_with_full_acknowledgement():
    config = build_producer_config(Settings.from_env())
    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True


def test_sasl_ssl_uses_oidc_client_credentials_and_tls():
    os.environ.update(
        {
            "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_OAUTH_TOKEN_URL": "https://idp/token",
            "KAFKA_OAUTH_CLIENT_ID": "fraud-worker",
            "KAFKA_OAUTH_CLIENT_SECRET": "from-secret-store",
            "KAFKA_SSL_CA_LOCATION": "/certs/ca.pem",
        }
    )
    config = build_consumer_config(Settings.from_env())
    assert config["sasl.mechanisms"] == "OAUTHBEARER"
    assert config["sasl.oauthbearer.method"] == "oidc"
    assert config["ssl.ca.location"] == "/certs/ca.pem"
    assert "ssl.key.location" not in config


def test_sasl_without_credentials_fails_fast():
    os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_PLAINTEXT"
    with pytest.raises(RuntimeError, match="KAFKA_OAUTH_CLIENT_SECRET"):
        build_consumer_config(Settings.from_env())


def test_unknown_protocol_rejected():
    os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_GSSAPI"
    with pytest.raises(RuntimeError, match="KAFKA_SECURITY_PROTOCOL"):
        build_producer_config(Settings.from_env())
