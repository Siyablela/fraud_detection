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

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
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
    remove_partition_backlog,
    set_kafka_health,
    set_partition_backlog,
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
    async def process(self, value: bytes | None, provenance: MessageProvenance) -> Any: ...


class DeadLetterSink(Protocol):
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
    topic: str
    partition: int
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    stopping: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task | None = None
    paused: bool = False


class FraudWorker:
    def __init__(
        self,
        settings: Settings,
        consumer: Any,
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
                self._check_group_membership()
                self._heartbeat.touch()
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
        if error.fatal():
            logger.error("kafka_fatal_error", error=error.str())
            raise KafkaException(error)
        if error.code() != KafkaError._PARTITION_EOF:
            set_kafka_health(SERVICE_NAME, "consumer", False)
            logger.warning("kafka_consumer_error", error=error.str(), retriable=error.retriable())

    async def _shutdown(self) -> None:
        logger.info("worker_draining", partitions=len(self._partitions))
        await self._stop_partitions(list(self._partitions.values()))
        # close() commits stored offsets and leaves the group cleanly.
        await asyncio.to_thread(self._consumer.close)
        set_kafka_health(SERVICE_NAME, "consumer", False)
        logger.info("worker_stopped")

    # --- rebalancing (callbacks run on the poll thread) -----------------------

    def _run_on_loop(self, coroutine: Any) -> None:
        if self._loop is None:
            raise RuntimeError("rebalance callback fired before the worker started")
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        future.result(timeout=self._settings.worker_shutdown_timeout_seconds + 5)

    def _on_assign(self, _consumer: Any, partitions: list[TopicPartition]) -> None:
        self._run_on_loop(self._start_partitions(partitions))

    def _on_revoke(self, consumer: Any, partitions: list[TopicPartition]) -> None:
        self._run_on_loop(self._stop_partitions(self._states_for(partitions)))
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError._NO_OFFSET:
                logger.warning("offset_commit_on_revoke_failed", error=str(exc))

    def _on_lost(self, _consumer: Any, partitions: list[TopicPartition]) -> None:
        # Ownership is already gone; never commit for lost partitions.
        logger.warning("partitions_lost", partitions=[tp.partition for tp in partitions])
        self._run_on_loop(self._stop_partitions(self._states_for(partitions)))

    def _states_for(self, partitions: list[TopicPartition]) -> list[_PartitionState]:
        return [state for tp in partitions if (state := self._partitions.get((tp.topic, tp.partition))) is not None]

    async def _start_partitions(self, partitions: list[TopicPartition]) -> None:
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
            remove_partition_backlog(SERVICE_NAME, state.partition)
        if states:
            logger.info("partitions_released", partitions=sorted(state.partition for state in states))

    # --- dispatch and per-partition processing -------------------------------

    def _dispatch(self, message: Any) -> None:
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
        """Process one message to completion. Returns ``False`` only if stopped mid-retry."""
        topic, partition, offset = message.topic(), message.partition(), message.offset()
        timestamp_type, timestamp_ms = message.timestamp()
        provenance = MessageProvenance(
            topic=topic, partition=partition, offset=offset, timestamp_ms=timestamp_ms if timestamp_type else None
        )
        pending_letter: DeadLetter | None = None
        unexpected_failures = 0
        attempt = 0

        while not state.stopping.is_set():
            attempt += 1
            clear_log_context()
            try:
                if pending_letter is None:
                    with worker_message_timer(SERVICE_NAME, topic):
                        result = await self._handler.process(message.value(), provenance)
                    worker_message_outcome(SERVICE_NAME, topic, result.outcome.value)
                    if result.outcome is RecordOutcome.EVALUATED:
                        worker_fraud_decision(SERVICE_NAME, result.decision.verdict, result.decision.triggered_rules)
                    return True
                await self._dead_letters.publish(pending_letter)
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
            except asyncio.CancelledError:
                raise
            except Exception as exc:
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


async def main() -> None:
    settings = get_settings()
    configure_logging(SERVICE_NAME, settings.observability_log_level, settings.logs_redact_fields)
    start_http_server(settings.worker_metrics_port)

    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    redis_client = create_redis_client(settings)
    producer = Producer(build_producer_config(settings))
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
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, worker.stop)
        await worker.run()
    finally:
        await asyncio.to_thread(producer.flush, 10)
        await redis_client.aclose()
        await engine.dispose()


def _log_kafka_client_error(error: KafkaError) -> None:
    if error.code() in (KafkaError._ALL_BROKERS_DOWN, KafkaError._TRANSPORT):
        set_kafka_health(SERVICE_NAME, "consumer", False)
    logger.warning("kafka_client_error", error=error.str(), fatal=error.fatal())


if __name__ == "__main__":
    asyncio.run(main())
