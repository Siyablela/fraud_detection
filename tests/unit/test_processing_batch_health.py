import asyncio
import os
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

from app import processing
from app.batch_worker import seconds_until_next_run
from app.database import MessageProvenance, RecordOutcome, RecordResult
from app.errors import InvalidEventError
from app.healthcheck import check_worker
from app.healthcheck import main as healthcheck_main
from app.observability import get_correlation_id
from app.processing import TransactionProcessor
from tests.conftest import default_rule_set, encode_event, make_event

PROVENANCE = MessageProvenance(topic="transactions", partition=0, offset=5, timestamp_ms=None)


class StaticRuleSets:
    async def get_active(self):
        return default_rule_set(version=3)


def process(value: bytes):
    """Run the processor and capture the correlation id bound in its context."""

    async def run():
        result = await TransactionProcessor(None, StaticRuleSets()).process(value, PROVENANCE)
        return result, get_correlation_id()

    return asyncio.run(run())


@pytest.fixture
def fake_database(monkeypatch):
    calls = {}

    async def velocity(_sf, **kwargs):
        calls["velocity"] = kwargs
        return 5, Decimal("49000.00")

    async def record(_sf, decision, provenance):
        calls["decision"], calls["provenance"] = decision, provenance
        return RecordResult(outcome=RecordOutcome.EVALUATED, decision=decision)

    monkeypatch.setattr(processing, "get_velocity_aggregates", velocity)
    monkeypatch.setattr(processing, "record_decision", record)
    return calls


def test_processor_scores_with_event_time_window_and_records_traceability(fake_database):
    event = make_event(amount="1500.00", category="crypto", occurred_at="2026-10-29T12:00:00Z", correlation_id="c-9")
    result, correlation_id = process(encode_event(event))

    window = fake_database["velocity"]
    assert window["window_end"] == datetime(2026, 10, 29, 12, tzinfo=UTC)
    assert window["window_end"] - window["window_start"] == timedelta(seconds=60)
    assert window["exclude_transaction_id"] == UUID(event["transaction_id"])

    decision = result.decision
    assert decision.triggered_rules == ["VELOCITY_COUNT_EXCEEDED", "VELOCITY_AMOUNT"]
    assert decision.is_fraud is True
    assert decision.rules_version == "fraud-default@v3"
    assert decision.category == "CRYPTO"
    assert decision.correlation_id == "c-9"
    assert correlation_id == "c-9"


def test_processor_uses_transaction_id_as_correlation_fallback(fake_database):
    event = make_event()
    _, correlation_id = process(encode_event(event))
    assert correlation_id == event["transaction_id"]
    assert fake_database["decision"].correlation_id is None


def test_processor_rejects_invalid_event_before_touching_the_database(fake_database):
    with pytest.raises(InvalidEventError):
        process(b"{}")
    assert fake_database == {}


@pytest.mark.parametrize(
    ("now", "expected_hours"),
    [
        (datetime(2026, 10, 29, 1, 0, tzinfo=UTC), 1),
        (datetime(2026, 10, 29, 2, 0, tzinfo=UTC), 24),  # exactly at run time -> tomorrow
        (datetime(2026, 10, 29, 3, 0, tzinfo=UTC), 23),
    ],
)
def test_batch_schedule(now, expected_hours):
    assert seconds_until_next_run(now, run_hour=2, run_minute=0) == expected_hours * 3600


def test_worker_healthcheck_uses_heartbeat_age(tmp_path):
    heartbeat = tmp_path / "heartbeat"
    assert check_worker(str(heartbeat)) is False
    heartbeat.touch()
    assert check_worker(str(heartbeat)) is True
    stale = time.time() - 120
    os.utime(heartbeat, (stale, stale))
    assert check_worker(str(heartbeat)) is False


def test_healthcheck_cli_exit_codes(tmp_path):
    os.environ["WORKER_HEARTBEAT_PATH"] = str(tmp_path / "missing")
    assert healthcheck_main(["healthcheck", "worker"]) == 1
    assert healthcheck_main(["healthcheck", "unknown"]) == 2
