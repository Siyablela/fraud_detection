import asyncio
import os
import signal
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException
from prometheus_client import REGISTRY

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

    # The rest of the KafkaConsumer protocol; these tests drive the worker without polling.
    def subscribe(self, topics, **callbacks):
        pass

    def poll(self, timeout):
        return None

    def commit(self, *, asynchronous):
        pass

    def close(self):
        pass


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


def blocked_seconds(partition: int) -> float | None:
    return REGISTRY.get_sample_value(
        "fraud_worker_partition_blocked_seconds", {"service": "fraud-worker", "partition": str(partition)}
    )


def test_blocked_seconds_grows_while_retrying_and_resets_when_handled(settings):
    clock = iter(range(0, 10_000, 100))  # each reading is 100 s after the last
    seen = []

    class ObservingHandler(ScriptedHandler):
        async def process(self, value, provenance):
            seen.append(blocked_seconds(5))
            return await super().process(value, provenance)

    handler = ObservingHandler(*[ConnectionError("db down")] * 3)
    worker = FraudWorker(settings, FakeConsumer(), handler, RecordingDeadLetters(), clock=lambda: next(clock))
    state = _PartitionState(topic="transactions", partition=5)

    assert asyncio.run(worker._handle(state, FakeMessage(offset=7, partition=5))) is True
    assert seen[1:] == [100, 200, 300]  # measured from the first failure, updated on every retry
    assert blocked_seconds(5) == 0

    asyncio.run(worker._stop_partitions([state]))
    assert blocked_seconds(5) is None  # released partitions cannot keep the alert firing


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


class PollingConsumer(FakeConsumer):
    """Replays scripted ``poll`` results, then asks the worker to stop."""

    def __init__(self, worker_ref: list, *polled):
        super().__init__()
        self.polled = list(polled)
        self.worker_ref = worker_ref
        self.closed = False
        self.health_at_poll: list[float | None] = []

    def subscribe(self, topics, on_assign, on_revoke, on_lost):
        self.topics = topics

    def poll(self, _timeout):
        self.health_at_poll.append(kafka_up())
        if self.polled:
            return self.polled.pop(0)
        self.worker_ref[0].stop()
        return None

    def close(self):
        self.closed = True


class ErrorMessage:
    def __init__(self, error: KafkaError):
        self._error = error

    def error(self):
        return self._error


def kafka_up() -> float | None:
    return REGISTRY.get_sample_value("fraud_kafka_up", {"service": "fraud-worker", "component": "consumer"})


@pytest.fixture
def run_settings(settings, tmp_path):
    os.environ["WORKER_HEARTBEAT_PATH"] = str(tmp_path / "heartbeat")
    return Settings.from_env()


def run_worker(settings, *polled):
    ref: list = [None]
    consumer = PollingConsumer(ref, *polled)
    ref[0] = FraudWorker(settings, consumer, ScriptedHandler(), RecordingDeadLetters())
    return consumer, ref[0]


def test_transient_poll_errors_mark_kafka_down_and_keep_polling(run_settings):
    transport = ErrorMessage(KafkaError(KafkaError._TRANSPORT, "broker gone"))
    consumer, worker = run_worker(run_settings, FakeMessage(0), transport)

    asyncio.run(worker.run())  # reported, not raised: the loop polls again and stops normally

    assert consumer.health_at_poll[1:] == [1, 0]  # healthy after a message, down after the error
    assert consumer.closed and consumer.topics == ["transactions"]


def test_end_of_partition_is_not_an_error(run_settings):
    eof = ErrorMessage(KafkaError(KafkaError._PARTITION_EOF))
    consumer, worker = run_worker(run_settings, FakeMessage(0), eof)

    asyncio.run(worker.run())

    assert consumer.health_at_poll[1:] == [1, 1]  # EOF left the consumer marked healthy


def test_fatal_poll_error_stops_the_worker_after_a_clean_shutdown(run_settings):
    fatal = ErrorMessage(KafkaError(KafkaError._FENCED, "fenced", fatal=True))
    consumer, worker = run_worker(run_settings, fatal)

    with pytest.raises(KafkaException):
        asyncio.run(worker.run())

    assert consumer.closed  # offsets committed and the group left even on the way out


class TestMain:
    """``main`` wires real components together; dependencies are replaced by recorders."""

    @pytest.fixture
    def wiring(self, monkeypatch, run_settings):
        import app.worker as worker_module

        calls: list[str] = []

        class Recorder:
            def __init__(self, name):
                self.name = name

            def __call__(self, *_args, **_kwargs):
                calls.append(self.name)

        class FakeEngine:
            async def dispose(self):
                calls.append("engine.dispose")

        class FakeRedis:
            async def aclose(self):
                calls.append("redis.aclose")

        class FakeProducer:
            def __init__(self, config):
                self.config = config

            def flush(self, _timeout):
                calls.append("producer.flush")
                return 0

        class FakeKafkaConsumer:
            def __init__(self, config):
                calls.append("consumer.created")
                self.config = config

        async def ping(_session_factory, _service):
            calls.append("ping_database")

        monkeypatch.setattr(worker_module, "get_settings", lambda: run_settings)
        monkeypatch.setattr(worker_module, "start_http_server", Recorder("metrics_server"))
        monkeypatch.setattr(worker_module, "create_engine", lambda _settings: FakeEngine())
        monkeypatch.setattr(worker_module, "create_redis_client", lambda _settings: FakeRedis())
        monkeypatch.setattr(worker_module, "Producer", FakeProducer)
        monkeypatch.setattr(worker_module, "Consumer", FakeKafkaConsumer)
        monkeypatch.setattr(worker_module, "ping_database", ping)
        return worker_module, calls

    def run_main(self, monkeypatch, worker_module, worker_run):
        handlers: dict = {}
        monkeypatch.setattr(worker_module.FraudWorker, "run", worker_run)

        async def go():
            # add_signal_handler is not available on Windows event loops; record instead.
            monkeypatch.setattr(asyncio.get_running_loop(), "add_signal_handler", handlers.__setitem__)
            await worker_module.main()

        asyncio.run(go())
        return handlers

    def test_wires_dependencies_installs_signal_handlers_and_cleans_up(self, monkeypatch, wiring):
        worker_module, calls = wiring

        async def run(worker):
            calls.append("worker.run")

        handlers = self.run_main(monkeypatch, worker_module, run)

        assert calls == [
            "metrics_server",
            "consumer.created",
            "ping_database",
            "worker.run",
            "producer.flush",
            "redis.aclose",
            "engine.dispose",
        ]
        assert set(handlers) == {signal.SIGTERM, signal.SIGINT}
        assert all(getattr(handler, "__name__", "") == "stop" for handler in handlers.values())

    def test_cleans_up_when_the_worker_fails(self, monkeypatch, wiring):
        worker_module, calls = wiring

        async def run(worker):
            raise RuntimeError("not a consumer group member")

        with pytest.raises(RuntimeError):
            self.run_main(monkeypatch, worker_module, run)

        assert calls[-3:] == ["producer.flush", "redis.aclose", "engine.dispose"]
