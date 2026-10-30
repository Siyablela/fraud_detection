import asyncio
import os
from types import SimpleNamespace

import pytest

from app.database import RecordOutcome
from app.errors import FailureClass, InvalidEventError, TransientError
from app.settings import Settings
from app.worker import FraudWorker, _PartitionState, retry_delay
from tests.conftest import make_decision


class FakeMessage:
    def __init__(self, offset: int, partition: int = 0, value: bytes = b"{}"):
        self._offset, self._partition, self._value = offset, partition, value

    def topic(self):
        return "transactions"

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return b"user"

    def value(self):
        return self._value

    def timestamp(self):
        return (1, 1_761_739_200_000)

    def error(self):
        return None


class FakeConsumer:
    def __init__(self, member_id: str | None = "member-1"):
        self.member_id = member_id
        self.stored: list[tuple[int, int]] = []
        self.paused: list[int] = []
        self.resumed: list[int] = []

    def memberid(self):
        return self.member_id

    def store_offsets(self, offsets):
        self.stored.extend((tp.partition, tp.offset) for tp in offsets)

    def pause(self, partitions):
        self.paused.extend(tp.partition for tp in partitions)

    def resume(self, partitions):
        self.resumed.extend(tp.partition for tp in partitions)


class ScriptedHandler:
    """Raises the scripted exceptions in order, then succeeds."""

    def __init__(self, *failures: BaseException):
        self.failures = list(failures)
        self.calls = 0

    async def process(self, value, provenance):
        self.calls += 1
        if self.failures:
            raise self.failures.pop(0)
        return SimpleNamespace(outcome=RecordOutcome.EVALUATED, decision=make_decision())


class RecordingDeadLetters:
    def __init__(self, failures: int = 0):
        self.failures = failures
        self.letters = []

    async def publish(self, letter):
        if self.failures:
            self.failures -= 1
            raise TransientError("dlq down")
        self.letters.append(letter)


@pytest.fixture
def settings():
    os.environ.update(
        {
            "WORKER_RETRY_INITIAL_BACKOFF_SECONDS": "0.001",
            "WORKER_RETRY_MAX_BACKOFF_SECONDS": "0.002",
            "WORKER_MAX_UNEXPECTED_ERROR_ATTEMPTS": "3",
            "WORKER_PARTITION_HIGH_WATERMARK": "4",
        }
    )
    return Settings.from_env()


def run_handle(settings, handler, dead_letters, stopping=False):
    worker = FraudWorker(settings, FakeConsumer(), handler, dead_letters)

    async def run():
        state = _PartitionState(topic="transactions", partition=0)
        if stopping:
            state.stopping.set()
        return await worker._handle(state, FakeMessage(offset=7))

    return asyncio.run(run())


def test_success_on_first_attempt(settings):
    handler, dead_letters = ScriptedHandler(), RecordingDeadLetters()
    assert run_handle(settings, handler, dead_letters) is True
    assert handler.calls == 1 and dead_letters.letters == []


def test_permanent_failure_is_dead_lettered_without_retry(settings):
    handler, dead_letters = ScriptedHandler(InvalidEventError("bad")), RecordingDeadLetters()
    assert run_handle(settings, handler, dead_letters) is True
    assert handler.calls == 1
    (letter,) = dead_letters.letters
    assert letter.failure_class is FailureClass.PERMANENT
    assert (letter.source_offset, letter.value, letter.key) == (7, b"{}", b"user")


def test_transient_failures_retry_until_success_and_never_dead_letter(settings):
    failures = [ConnectionError("db down")] * 6
    handler, dead_letters = ScriptedHandler(*failures), RecordingDeadLetters()
    assert run_handle(settings, handler, dead_letters) is True
    assert handler.calls == 7 and dead_letters.letters == []


def test_unexpected_failures_are_bounded_then_dead_lettered(settings):
    handler, dead_letters = ScriptedHandler(*[KeyError("bug")] * 10), RecordingDeadLetters()
    assert run_handle(settings, handler, dead_letters) is True
    assert handler.calls == 3
    assert dead_letters.letters[0].failure_class is FailureClass.UNEXPECTED
    assert dead_letters.letters[0].attempts == 3


def test_dead_letter_publish_is_retried_until_acknowledged(settings):
    handler, dead_letters = ScriptedHandler(InvalidEventError("bad")), RecordingDeadLetters(failures=2)
    assert run_handle(settings, handler, dead_letters) is True
    assert handler.calls == 1 and len(dead_letters.letters) == 1


def test_stopping_partition_abandons_message_for_redelivery(settings):
    handler, dead_letters = ScriptedHandler(ConnectionError("down")), RecordingDeadLetters()
    assert run_handle(settings, handler, dead_letters, stopping=True) is False
    assert handler.calls == 0


def test_partition_processes_in_order_and_stores_next_offsets(settings):
    consumer = FakeConsumer()
    worker = FraudWorker(settings, consumer, ScriptedHandler(), RecordingDeadLetters())

    async def run():
        state = _PartitionState(topic="transactions", partition=3)
        worker._partitions[("transactions", 3)] = state
        for offset in (10, 11, 12):
            state.queue.put_nowait(FakeMessage(offset, partition=3))
        task = state.task = asyncio.create_task(worker._consume_partition(state))
        while state.queue.qsize():
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.01)
        await worker._stop_partitions([state])
        assert task.done()

    asyncio.run(run())
    assert consumer.stored == [(3, 11), (3, 12), (3, 13)]


def test_backlog_pauses_partition_and_resumes_when_drained(settings):
    consumer = FakeConsumer()
    worker = FraudWorker(settings, consumer, ScriptedHandler(), RecordingDeadLetters())

    async def run():
        state = _PartitionState(topic="transactions", partition=1)
        worker._partitions[("transactions", 1)] = state
        for offset in range(5):
            worker._dispatch(FakeMessage(offset, partition=1))
        assert consumer.paused == [1]
        task = state.task = asyncio.create_task(worker._consume_partition(state))
        while state.queue.qsize():
            await asyncio.sleep(0.01)
        await worker._stop_partitions([state])
        await task

    asyncio.run(run())
    assert consumer.resumed == [1]
    assert [offset for _, offset in consumer.stored] == [1, 2, 3, 4, 5]


def test_messages_for_revoked_partitions_are_dropped_for_redelivery(settings):
    worker = FraudWorker(settings, FakeConsumer(), ScriptedHandler(), RecordingDeadLetters())
    worker._dispatch(FakeMessage(0, partition=9))  # not assigned: must not raise


@pytest.mark.parametrize("attempt", [1, 2, 5, 50])
def test_retry_delay_is_capped_and_jittered(attempt):
    for _ in range(50):
        delay = retry_delay(attempt, initial=0.5, maximum=30)
        capped = min(30, 0.5 * 2 ** (attempt - 1))
        assert capped * 0.5 <= delay <= capped


def test_worker_exits_when_out_of_consumer_group_too_long(settings):
    now = [0.0]
    consumer = FakeConsumer(member_id=None)
    worker = FraudWorker(settings, consumer, ScriptedHandler(), RecordingDeadLetters(), clock=lambda: now[0])
    now[0] = settings.worker_max_group_absence_seconds - 1
    worker._check_group_membership()  # still within the grace period
    now[0] = settings.worker_max_group_absence_seconds + 1
    with pytest.raises(RuntimeError, match="consumer group"):
        worker._check_group_membership()


def test_group_membership_resets_the_absence_timer(settings):
    now = [0.0]
    consumer = FakeConsumer(member_id="member-1")
    worker = FraudWorker(settings, consumer, ScriptedHandler(), RecordingDeadLetters(), clock=lambda: now[0])
    now[0] = 1_000.0
    worker._check_group_membership()
    consumer.member_id = None
    now[0] = 1_000.0 + settings.worker_max_group_absence_seconds - 1
    worker._check_group_membership()
