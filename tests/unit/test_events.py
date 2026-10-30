import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.errors import InvalidEventError, UnsupportedSchemaVersionError
from app.events import MAX_EVENT_BYTES, parse_transaction_event
from tests.conftest import encode_event, make_event


def test_valid_event_is_parsed_and_normalised():
    event = parse_transaction_event(
        encode_event(make_event(category=" crypto ", occurred_at="2026-10-29T14:00:00+02:00", amount="10.5"))
    )
    assert event.category == "CRYPTO"
    assert event.occurred_at == datetime(2026, 10, 29, 12, tzinfo=UTC)
    assert event.amount == Decimal("10.5")


def test_json_number_amount_keeps_exact_decimal_value():
    raw = encode_event(make_event(amount=None)).replace(b"}", b', "amount": 125.10}')
    assert parse_transaction_event(raw).amount == Decimal("125.10")


def test_unknown_fields_are_ignored_for_forward_compatibility():
    assert parse_transaction_event(encode_event(make_event(merchant="ACME"))).category == "GROCERIES"


@pytest.mark.parametrize(
    ("overrides", "field"),
    [
        ({"amount": "-1.00"}, "amount"),
        ({"amount": "0"}, "amount"),
        ({"amount": "1.001"}, "amount"),
        ({"occurred_at": "2026-10-29T12:00:00"}, "occurred_at"),  # no timezone
        ({"transaction_id": "tx-1"}, "transaction_id"),
        ({"category": "DROP TABLE;"}, "category"),
        ({"category": "X" * 65}, "category"),
        ({"event_type": "CreateTransaction"}, "event_type"),
        ({"user_id": None}, "user_id"),
        ({"correlation_id": "has spaces"}, "correlation_id"),
    ],
)
def test_invalid_fields_reported_without_echoing_values(overrides, field):
    with pytest.raises(InvalidEventError) as error:
        parse_transaction_event(encode_event(make_event(**overrides)))
    assert any(detail["field"] == field for detail in error.value.details)
    assert "DROP TABLE" not in json.dumps(error.value.details)


@pytest.mark.parametrize("version", [2, "1", None])
def test_unknown_schema_version_is_rejected_explicitly(version):
    with pytest.raises(UnsupportedSchemaVersionError):
        parse_transaction_event(encode_event({**make_event(), "schema_version": version}))


@pytest.mark.parametrize(
    "raw",
    [None, b"", b"not json", b"[1, 2]", b"\xff\xfe", b"{" + b" " * MAX_EVENT_BYTES + b"}"],
    ids=["none", "empty", "not-json", "not-object", "not-utf8", "oversized"],
)
def test_malformed_payloads(raw):
    with pytest.raises(InvalidEventError):
        parse_transaction_event(raw)


def test_currency_is_required_and_normalised():
    with pytest.raises(InvalidEventError) as error:
        parse_transaction_event(encode_event(make_event(currency=None)))
    assert any(detail["field"] == "currency" for detail in error.value.details)
    assert parse_transaction_event(encode_event(make_event(currency="usd"))).currency == "USD"


def test_optional_channel_country_and_enrichment():
    event = parse_transaction_event(
        encode_event(
            make_event(
                channel="online",
                country="za",
                enrichment={"distance_from_last_tx_km": "5.2", "is_new_merchant_for_user": True, "future": 1},
            )
        )
    )
    assert (event.channel, event.country) == ("ONLINE", "ZA")
    assert event.enrichment.distance_from_last_tx_km == Decimal("5.2")
    assert event.enrichment.is_new_merchant_for_user is True
    assert event.enrichment.minutes_since_last_tx is None


@pytest.mark.parametrize(
    ("overrides", "field"),
    [
        ({"currency": "RANDS"}, "currency"),
        ({"country": "ZAF"}, "country"),
        ({"channel": "ATM 1"}, "channel"),
        ({"enrichment": {"minutes_since_last_tx": -1}}, "enrichment.minutes_since_last_tx"),
        ({"enrichment": "not-an-object"}, "enrichment"),
    ],
)
def test_invalid_optional_fields(overrides, field):
    with pytest.raises(InvalidEventError) as error:
        parse_transaction_event(encode_event(make_event(**overrides)))
    assert any(detail["field"] == field for detail in error.value.details)
