"""Kafka consumer that scores transaction events and records fraud decisions.

Concurrency and ordering: one asyncio task per assigned partition processes that
partition's messages strictly in order. Producers key events by ``user_id``, so a
user's events share a partition and the velocity look-back always sees that
user's earlier events. Partitions run in parallel with each other.

Offsets: a message's offset is stored only after it is fully handled, and
partitions commit independently, so an offset is never committed ahead of an
unfinished message (see :mod:`app.kafka_config`).

Backpressure: if a partition's backlog reaches ``WORKER_PARTITION_HIGH_WATERMARK``
it is paused and resumed at half that. Polling never blocks on a slow partition,
so the consumer keeps its group membership (``max.poll.interval.ms``).

Failure handling (see :mod:`app.errors`):

* transient   -> retry with capped exponential backoff and jitter, indefinitely,
                 holding the partition (a dependency outage must not empty good
                 traffic into the DLQ);
* permanent   -> dead-letter immediately;
* unexpected  -> retry up to ``WORKER_MAX_UNEXPECTED_ERROR_ATTEMPTS`` times, then
                 dead-letter, so one poison message cannot stall its partition.
"""

import asyncio
import random
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from prometheus_client import start_http_server

from app.cache import RuleSetProvider, create_redis_client
from app.database import (
    MessageProvenance,
    RecordOutcome,
    create_engine,
    create_session_factory,
    ping_database,
)
from app.dlq import DeadLetter, DeadLetterPublisher, describe_error
from app.errors import FailureClass, classify_failure
from app.kafka_config import build_consumer_config, build_producer_config
from app.observability import (
    clear_log_context,
    configure_logging,
    get_correlation_id,
    get_logger,
    remove_partition_metrics,
    set_kafka_health,
    set_partition_backlog,
    set_partition_blocked,
    worker_fraud_decision,
    worker_message_outcome,
    worker_message_timer,
    worker_retry,
)
from app.processing import TransactionProcessor
from app.settings import Settings, get_settings

SERVICE_NAME = "fraud-worker"
logger = get_logger(__name__)


class MessageHandler(Protocol):
    """Processes one message value (``TransactionProcessor`` in production, fakes in tests)."""

    async def process(self, value: bytes | None, provenance: MessageProvenance) -> Any: ...


class KafkaConsumer(Protocol):
    """The consumer calls the worker makes (``confluent_kafka.Consumer`` in production, fakes in tests)."""

    def subscribe(self, topics: list[str], **callbacks: Any) -> None: ...
    def poll(self, timeout: float) -> Message | None: ...
    def memberid(self) -> str | None: ...
    def pause(self, partitions: list[TopicPartition]) -> None: ...
    def resume(self, partitions: list[TopicPartition]) -> None: ...
    def store_offsets(self, *, offsets: list[TopicPartition]) -> None: ...
    def commit(self, *, asynchronous: bool) -> Any: ...
    def close(self) -> None: ...


class DeadLetterSink(Protocol):
    """Publishes a dead letter and returns only once it is safely stored."""

    async def publish(self, letter: DeadLetter) -> None: ...


def retry_delay(attempt: int, initial: float, maximum: float) -> float:
    """Capped exponential backoff with jitter in [50%, 100%] of the capped delay.

    Jitter spreads retries from many partitions so they do not hit a recovering
    database in lockstep.
    """
    capped = min(maximum, initial * (2 ** max(attempt - 1, 0)))
    return capped * random.uniform(0.5, 1.0)


@dataclass
class _PartitionState:
    """One assigned partition: its queue of polled messages and the task that drains it in order."""

    topic: str
    partition: int
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    stopping: asyncio.Event = field(default_factory=asyncio.Event)  # set on revoke or shutdown
    task: asyncio.Task | None = None
    paused: bool = False


class FraudWorker:
    """Consumes transaction events and hands each one to ``handler``, one in-order task per partition.

    Threads: ``consumer.poll`` runs in a worker thread so it never blocks the event
    loop, and librdkafka calls the rebalance callbacks (``_on_assign`` and friends)
    on that same thread. Those callbacks therefore hand their work to the event loop
    and wait for it, rather than touching partition state directly.
    """

    def __init__(
        self,
        settings: Settings,
        consumer: KafkaConsumer,
        handler: MessageHandler,
        dead_letters: DeadLetterSink,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._settings = settings
        self._consumer = consumer
        self._handler = handler
        self._dead_letters = dead_letters
        self._partitions: dict[tuple[str, int], _PartitionState] = {}
        self._running = True
        self._loop: asyncio.AbstractEventLoop | None = None
        self._heartbeat = Path(settings.worker_heartbeat_path)
        self._clock = clock
        self._last_group_membership = clock()

    # --- lifecycle ------------------------------------------------------------

    def stop(self) -> None:
        """Ask the poll loop to finish; :meth:`run` then drains partitions and closes the consumer.

        Called from a signal handler, so it only sets a flag.
        """
        if self._running:
            logger.info("worker_stop_requested")
        self._running = False

    async def run(self) -> None:
        """Poll until :meth:`stop` is called, then drain and close."""
        self._loop = asyncio.get_running_loop()
        self._consumer.subscribe(
            [self._settings.kafka_topic_name],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )
        logger.info("worker_started", topic=self._settings.kafka_topic_name)
        try:
            while self._running:
                # 1. Check that the consumer is still a member of its group. If not, exit so the
                #    orchestrator restarts it.
                # 2. Touch the heartbeat file to indicate the worker is alive.
                # 3. Poll for a message with a 1-second timeout. If no message is received, continue
                #    the loop. Use a thread to avoid blocking the event loop: poll() is C code that
                #    releases the GIL while it waits, so the event loop keeps running partition tasks
                #    meanwhile. Rebalance callbacks run on this poll thread, inside poll().
                # 4. If a message is received, check for errors. If there is an error, handle it and
                #    continue the loop. If there is no error, mark Kafka as healthy and dispatch the
                #    message to its partition's queue.
                self._check_group_membership()
                self._heartbeat.touch()  # liveness: the health check fails if this file goes stale
                message = await asyncio.to_thread(self._consumer.poll, 1.0)
                if message is None:
                    continue
                error = message.error()
                if error is not None:
                    self._handle_poll_error(error)
                    continue
                set_kafka_health(SERVICE_NAME, "consumer", True)
                self._dispatch(message)
        finally:
            await self._shutdown()

    def _check_group_membership(self) -> None:
        """Exit if the consumer has been outside its group for too long.

        librdkafka normally rejoins by itself after a broker outage, but a consumer
        can get stuck outside the group (seen after a coordinator failover) while
        its poll loop - and therefore its liveness heartbeat - keeps running.
        Exiting lets the orchestrator's restart policy start a fresh consumer.
        """
        if self._consumer.memberid():
            self._last_group_membership = self._clock()
            return
        absent_for = self._clock() - self._last_group_membership
        if absent_for > self._settings.worker_max_group_absence_seconds:
            logger.error("worker_not_in_consumer_group", absent_seconds=round(absent_for))
            raise RuntimeError(f"not a consumer group member for {absent_for:.0f}s; exiting for a restart")

    def _handle_poll_error(self, error: KafkaError) -> None:
        """Fatal errors stop the worker; anything else is logged and librdkafka recovers by itself."""
        if error.fatal():
            logger.error("kafka_fatal_error", error=error.str())
            raise KafkaException(error)
        if error.code() != KafkaError._PARTITION_EOF:  # EOF just means "caught up"
            set_kafka_health(SERVICE_NAME, "consumer", False)
            logger.warning("kafka_consumer_error", error=error.str(), retriable=error.retriable())

    async def _shutdown(self) -> None:
        """Finish or abandon in-flight messages, commit stored offsets and leave the group."""
        logger.info("worker_draining", partitions=len(self._partitions))
        await self._stop_partitions(list(self._partitions.values()))
        # close() commits stored offsets and leaves the group cleanly, so the partitions
        # are reassigned at once instead of after the session timeout.
        await asyncio.to_thread(self._consumer.close)
        set_kafka_health(SERVICE_NAME, "consumer", False)
        logger.info("worker_stopped")

    # --- rebalancing (callbacks run on the poll thread) -----------------------

    def _run_on_loop(self, coroutine: Any) -> None:
        """Run ``coroutine`` on the event loop and block the calling (poll) thread until it finishes.

        Blocking is the point: librdkafka must not finish a rebalance until partition
        tasks have started or stopped.
        """
        if self._loop is None:
            raise RuntimeError("rebalance callback fired before the worker started")
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        future.result(timeout=self._settings.worker_shutdown_timeout_seconds + 5)

    def _on_assign(self, _consumer: Any, partitions: list[TopicPartition]) -> None:
        """Partitions were given to this consumer: start a task for each."""
        self._run_on_loop(self._start_partitions(partitions))

    def _on_revoke(self, consumer: Any, partitions: list[TopicPartition]) -> None:
        """Partitions are moving to another consumer: stop their tasks, then commit what they finished.

        Committing here (synchronously) means the next owner resumes exactly after the
        last handled message instead of reprocessing up to a second's worth.
        """
        self._run_on_loop(self._stop_partitions(self._states_for(partitions)))
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError._NO_OFFSET:
                logger.warning("offset_commit_on_revoke_failed", error=str(exc))

    def _on_lost(self, _consumer: Any, partitions: list[TopicPartition]) -> None:
        """Partitions were taken away without a clean revoke (e.g. after a long pause)."""
        # Ownership is already gone, so nothing is committed: another consumer may be
        # processing these partitions now. Unfinished messages are simply redelivered.
        logger.warning("partitions_lost", partitions=[tp.partition for tp in partitions])
        self._run_on_loop(self._stop_partitions(self._states_for(partitions)))

    def _states_for(self, partitions: list[TopicPartition]) -> list[_PartitionState]:
        """The states for those partitions this worker currently holds."""
        return [state for tp in partitions if (state := self._partitions.get((tp.topic, tp.partition))) is not None]

    async def _start_partitions(self, partitions: list[TopicPartition]) -> None:
        """Create a queue and a draining task for each newly assigned partition."""
        for tp in partitions:
            key = (tp.topic, tp.partition)
            if key in self._partitions:
                continue
            state = _PartitionState(topic=tp.topic, partition=tp.partition)
            state.task = asyncio.create_task(self._consume_partition(state), name=f"partition-{tp.partition}")
            self._partitions[key] = state
        logger.info("partitions_assigned", partitions=sorted(tp.partition for tp in partitions))

    async def _stop_partitions(self, states: list[_PartitionState]) -> None:
        """Let each in-flight message finish (bounded), discard queued ones.

        Discarded messages were never stored as processed, so they are redelivered
        to whichever consumer owns the partition next.
        """
        for state in states:
            state.stopping.set()
            while not state.queue.empty():
                state.queue.get_nowait()
            state.queue.put_nowait(None)  # wake an idle task
        tasks = [state.task for state in states if state.task is not None]
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=self._settings.worker_shutdown_timeout_seconds)
            for task in pending:
                task.cancel()
            if pending:
                logger.warning("partition_drain_timeout", cancelled=len(pending))
                await asyncio.gather(*pending, return_exceptions=True)
        for state in states:
            self._partitions.pop((state.topic, state.partition), None)
            remove_partition_metrics(SERVICE_NAME, state.partition)
        if states:
            logger.info("partitions_released", partitions=sorted(state.partition for state in states))

    # --- dispatch and per-partition processing -------------------------------

    def _dispatch(self, message: Any) -> None:
        """Queue a polled message on its partition, pausing the partition if its backlog is full.

        Pausing (rather than blocking) keeps the poll loop running, so the consumer
        stays in its group even while a partition is slow.
        """

        # 1. Get the partition state for this message. If the partition is being revoked,
        #    ignore the message (it will be redelivered).
        # 2. Put the message on the partition's queue. If the queue size exceeds the high
        #    watermark, pause the partition and log a warning.
        state = self._partitions.get((message.topic(), message.partition()))
        if state is None or state.stopping.is_set():
            return  # partition is being revoked; the message will be redelivered
        state.queue.put_nowait(message)
        backlog = state.queue.qsize()
        set_partition_backlog(SERVICE_NAME, state.partition, backlog)
        if not state.paused and backlog >= self._settings.worker_partition_high_watermark:
            self._consumer.pause([TopicPartition(state.topic, state.partition)])
            state.paused = True
            logger.warning("partition_paused", partition=state.partition, backlog=backlog)

    async def _consume_partition(self, state: _PartitionState) -> None:
        """Process one partition's messages strictly in offset order, storing each offset when done."""
        while not state.stopping.is_set():
            message = await state.queue.get()
            if message is None or state.stopping.is_set():
                break
            if await self._handle(state, message):
                try:
                    self._consumer.store_offsets(
                        offsets=[TopicPartition(state.topic, state.partition, message.offset() + 1)]
                    )
                except KafkaException as exc:
                    # The partition was lost mid-message; its new owner reprocesses
                    # it, which the idempotent write path makes harmless.
                    logger.warning("offset_store_failed", partition=state.partition, error=str(exc))
            backlog = state.queue.qsize()
            set_partition_backlog(SERVICE_NAME, state.partition, backlog)
            if state.paused and backlog <= self._settings.worker_partition_high_watermark // 2:
                self._consumer.resume([TopicPartition(state.topic, state.partition)])
                state.paused = False
                logger.info("partition_resumed", partition=state.partition, backlog=backlog)

    async def _handle(self, state: _PartitionState, message: Any) -> bool:
        """Process one message to completion. Returns ``False`` only if stopped mid-retry.

        Each pass of the loop either processes the message or, once it has been judged
        undeliverable (``pending_letter``), retries publishing it to the DLQ. Failures
        are classified to decide between retrying and dead-lettering; see app/errors.py.
        """
        topic, partition, offset = message.topic(), message.partition(), message.offset()
        timestamp_type, timestamp_ms = message.timestamp()
        provenance = MessageProvenance(
            topic=topic, partition=partition, offset=offset, timestamp_ms=timestamp_ms if timestamp_type else None
        )
        pending_letter: DeadLetter | None = None
        unexpected_failures = 0
        attempt = 0
        failing_since: float | None = None

        while not state.stopping.is_set():
            attempt += 1
            clear_log_context()  # a fresh correlation id per attempt; processing binds it
            try:
                if pending_letter is None:
                    with worker_message_timer(SERVICE_NAME, topic):
                        result = await self._handler.process(message.value(), provenance)
                    worker_message_outcome(SERVICE_NAME, topic, result.outcome.value)
                    if result.outcome is RecordOutcome.EVALUATED:
                        worker_fraud_decision(SERVICE_NAME, result.decision.verdict, result.decision.triggered_rules)
                    self._clear_blocked(partition, failing_since)
                    return True
                await self._dead_letters.publish(pending_letter)
                self._clear_blocked(partition, failing_since)
                worker_message_outcome(SERVICE_NAME, topic, "dlq")
                logger.error(
                    "message_dead_lettered",
                    dlq_topic=self._settings.kafka_dlq_topic_name,
                    source_partition=partition,
                    source_offset=offset,
                    failure_class=pending_letter.failure_class.value,
                    error_type=type(pending_letter.error).__name__,
                    error=describe_error(pending_letter.error),
                    attempts=pending_letter.attempts,
                )
                return True
            except asyncio.CancelledError:  # shutdown timed out: let the cancellation through
                raise
            except Exception as exc:
                if failing_since is None:
                    failing_since = self._clock()
                if pending_letter is None:
                    failure = classify_failure(exc)
                    if failure is FailureClass.UNEXPECTED:
                        unexpected_failures += 1
                        logger.exception(
                            "message_processing_unexpected_error",
                            source_partition=partition,
                            source_offset=offset,
                            attempt=unexpected_failures,
                        )
                    if failure is FailureClass.PERMANENT or (
                        failure is FailureClass.UNEXPECTED
                        and unexpected_failures >= self._settings.worker_max_unexpected_error_attempts
                    ):
                        # From now on the loop retries the DLQ publish, never the processing.
                        pending_letter = DeadLetter(
                            source_topic=topic,
                            source_partition=partition,
                            source_offset=offset,
                            key=message.key(),
                            value=message.value(),
                            error=exc,
                            failure_class=failure,
                            attempts=attempt,
                            correlation_id=get_correlation_id(),
                        )
                        continue
                    failure_label = failure.value
                else:
                    failure_label = "dlq_publish"
                delay = retry_delay(
                    attempt,
                    self._settings.worker_retry_initial_backoff_seconds,
                    self._settings.worker_retry_max_backoff_seconds,
                )
                worker_retry(SERVICE_NAME, failure_label)
                # Transient retries never give up, so this is what reveals one message
                # (rather than an outage) holding its partition indefinitely.
                set_partition_blocked(SERVICE_NAME, partition, self._clock() - failing_since)
                logger.warning(
                    "message_processing_retry",
                    failure_class=failure_label,
                    error_type=type(exc).__name__,
                    source_partition=partition,
                    source_offset=offset,
                    attempt=attempt,
                    retry_in_seconds=round(delay, 2),
                )
                try:
                    await asyncio.wait_for(state.stopping.wait(), timeout=delay)
                except TimeoutError:
                    pass
        logger.info("message_abandoned_for_redelivery", source_partition=partition, source_offset=offset)
        return False

    @staticmethod
    def _clear_blocked(partition: int, failing_since: float | None) -> None:
        """Reset the blocked-seconds gauge once a message that had been failing is handled."""
        if failing_since is not None:
            set_partition_blocked(SERVICE_NAME, partition, 0)


async def main() -> None:
    """Wire up the worker's dependencies, run until SIGTERM/SIGINT, then release everything.

    Each client (database pool, Redis, Kafka producer and consumer) is created once
    here and shared for the life of the process.
    """
    settings = get_settings()
    configure_logging(SERVICE_NAME, settings.observability_log_level, settings.logs_redact_fields)
    # Serves /metrics for Prometheus from a background daemon thread.
    start_http_server(settings.worker_metrics_port)

    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    redis_client = create_redis_client(settings)
    producer = Producer(build_producer_config(settings))
    # error_cb reports client-level problems (all brokers down, transport errors),
    # which never reach poll() as message errors.
    consumer = Consumer({**build_consumer_config(settings), "error_cb": _log_kafka_client_error})
    try:
        await ping_database(session_factory, SERVICE_NAME)
        rule_sets = RuleSetProvider(
            session_factory,
            redis_client,
            key=settings.rule_set_key,
            local_ttl_seconds=settings.rule_local_cache_ttl_seconds,
            redis_ttl_seconds=settings.rule_redis_cache_ttl_seconds,
        )
        worker = FraudWorker(
            settings,
            consumer,
            TransactionProcessor(session_factory, rule_sets),
            DeadLetterPublisher(producer, settings.kafka_dlq_topic_name),
        )

        # SIGTERM (docker stop, Kubernetes) and SIGINT (Ctrl+C) call worker.stop() on the
        # event loop, which starts a graceful drain instead of killing the process mid-message.
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, worker.stop)
        await worker.run()
    finally:
        # Deliver any dead letter still queued (normally none: each publish already
        # waits for its acknowledgement). flush() blocks, so it runs off the event loop.
        await asyncio.to_thread(producer.flush, 10)
        await redis_client.aclose()
        await engine.dispose()


def _log_kafka_client_error(error: KafkaError) -> None:
    """librdkafka's error callback: mark Kafka unhealthy when the brokers are unreachable."""
    if error.code() in (KafkaError._ALL_BROKERS_DOWN, KafkaError._TRANSPORT):
        set_kafka_health(SERVICE_NAME, "consumer", False)
    logger.warning("kafka_client_error", error=error.str(), fatal=error.fatal())


if __name__ == "__main__":
    asyncio.run(main())
