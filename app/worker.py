import asyncio
from datetime import datetime, timedelta, timezone
import inspect
import json
import signal
from typing import Any

import json_logic
import redis.asyncio as aioredis
import structlog
from confluent_kafka import Consumer, KafkaError, Producer
from prometheus_client import start_http_server

from app.database import (
    create_pool,
    get_recent_user_activity,
    ping_database,
    save_transaction,
)
from app.dlq import build_dlq_payload, encode_dlq_payload
from app.kafka_security import build_kafka_client_config
from app.observability import (
    attach_correlation_id,
    configure_logging,
    ensure_correlation_id,
    extract_correlation_id,
    get_correlation_id,
    get_logger,
    set_kafka_health,
    setup_tracing,
    worker_fraud_decision,
    worker_message_outcome,
    worker_message_timer,
)
from app.rule import Transaction, evaluate_transaction, get_active_rules_config
from app.settings import get_settings

SERVICE_NAME = "fraud-worker"
logger = get_logger(__name__)


def _vlocity_window_bounds(
    transaction_timestamp: float, window_seconds: int
) -> tuple[float, float]:
    """Build an event-time velocity window for a transaction."""
    window_end_epoch = float(transaction_timestamp)
    window_start_epoch = window_end_epoch - float(window_seconds)
    return window_start_epoch, window_end_epoch


def _message_value(msg: Any, attribute: str) -> Any:
    """Safely resolve a Kafka message attribute, invoking callables when needed."""
    value = getattr(msg, attribute, None)
    if callable(value):
        return value()
    return value


async def _invoke_callable(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """Invoke a sync or async callable in a way that preserves asyncio semantics."""
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    return await asyncio.to_thread(fn, *args, **kwargs)


async def commit_processed_message(consumer: Consumer, msg: Any) -> None:
    """Commit a Kafka message using the consumer's supported commit API."""
    commit_method = getattr(consumer, "commit", None)
    if commit_method is None:
        return

    topic = _message_value(msg, "topic")
    offset = _message_value(msg, "offset") + 1
    partition = _message_value(msg, "partition")
    if isinstance(offset, int) and isinstance(partition, int):
        commit_offsets = {(topic, partition): offset}
    else:
        commit_offsets = {topic: offset}

    if inspect.iscoroutinefunction(commit_method):
        try:
            await commit_method(message=msg, asynchronous=False)
        except TypeError:
            await commit_method(commit_offsets)
        return

    try:
        await _invoke_callable(commit_method, message=msg, asynchronous=False)
    except TypeError:
        await _invoke_callable(commit_method, commit_offsets)


async def route_message_to_dlq(
    consumer: Consumer,
    producer: Producer | None = None,
    msg: Any = None,
    raw_event: str = "",
    error: Exception | None = None,
    topic_name: str = "",
    dlq_topic_name: str = "",
    dlq_producer: Producer | None = None,
) -> None:
    """Publish a failed message to the DLQ and commit the original Kafka offset."""
    timestamp_value = _message_value(msg, "timestamp")
    if isinstance(timestamp_value, tuple) and len(timestamp_value) >= 2:
        source_timestamp = timestamp_value[1]
    else:
        source_timestamp = timestamp_value

    dlq_payload = build_dlq_payload(
        source_topic=topic_name,
        source_partition=_message_value(msg, "partition"),
        source_offset=_message_value(msg, "offset"),
        source_timestamp=source_timestamp,
        raw_payload=raw_event,
        error=error,
    )
    if isinstance(dlq_payload, dict):
        try:
            parsed_event = json.loads(raw_event)
        except Exception:
            parsed_event = {}
        correlation_id = extract_correlation_id(parsed_event) or ensure_correlation_id()
        dlq_payload.setdefault("correlation_id", correlation_id)

    target_producer = dlq_producer or producer
    if target_producer is None:
        raise RuntimeError("No Kafka producer is configured for DLQ delivery.")
    if hasattr(target_producer, "produce"):
        await _invoke_callable(
            target_producer.produce,
            dlq_topic_name,
            value=encode_dlq_payload(dlq_payload),
            key=_message_value(msg, "key"),
        )
        await _invoke_callable(target_producer.flush, 30)
    else:
        await _invoke_callable(
            target_producer.send_and_wait,
            topic=dlq_topic_name,
            key=_message_value(msg, "key"),
            value=encode_dlq_payload(dlq_payload),
        )
    worker_message_outcome(SERVICE_NAME, topic_name, "dlq")
    logger.exception(
        "transaction_routed_to_dlq",
        dlq_topic=dlq_topic_name,
        source_offset=_message_value(msg, "offset"),
        source_partition=_message_value(msg, "partition"),
    )
    await commit_processed_message(consumer, msg)


class FraudWorker:
    def __init__(self, settings: Any, db_pool: Any, redis_client: aioredis.Redis):
        self.settings = settings
        self.db_pool = db_pool
        self.redis_client = redis_client

        # Pod-Local Memory Cache for sub-microsecond checks across all 50 pods
        self._local_rule_cache: dict[str, dict[str, Any]] = {}
        self._active_background_tasks: set[asyncio.Task] = set()
        self._should_run = True

        # Orchestration Clients
        kafka_config = build_kafka_client_config(settings)
        self.consumer = Consumer(kafka_config["consumer"])
        self.producer = Producer(kafka_config["producer"])
        self.pubsub_task: asyncio.Task | None = None

    def handle_termination(self, signum: int) -> None:
        """Fires when the OS coordinates a termination request (SIGTERM/SIGINT)."""
        logger.info(
            "received_os_termination_signal", signal=signal.Signals(signum).name
        )
        self._should_run = False

    async def start(self) -> None:
        """Starts background tasks and enters the core streaming event loop."""
        start_http_server(self.settings.metrics_port)
        self.consumer.subscribe([self.settings.kafka_source_topic])
        set_kafka_health(True)

        # 1. Spawn the Redis Pub/Sub cluster synchronizer task
        self.pubsub_task = asyncio.create_task(self._listen_for_cache_invalidations())

        logger.info(
            "worker_runtime_initialized", topic=self.settings.kafka_source_topic
        )

        # 2. Main Consumer loop
        while self._should_run:
            try:
                # Prevent blocking the loop thread so shutdown updates can intercept
                msg = await _invoke_callable(self.consumer.poll, timeout=0.5)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error("kafka_broker_connection_error", error=msg.error())
                        set_kafka_health(False)
                        continue

                    set_kafka_health(True)

                # 3. Create non-blocking task with metric wraps
                task = asyncio.create_task(self._run_task_with_telemetry(msg))
                self._active_background_tasks.add(task)
                task.add_done_callback(self._active_background_tasks.discard)

            except Exception:
                logger.exception("worker_ingestion_loop_fault")
                await asyncio.sleep(1)

        await self.shutdown()

    async def _run_task_with_telemetry(self, msg: Any) -> None:
        """Wrapper method managing processing window performance tags."""
        topic_name = _message_value(msg, "topic")
        with worker_message_timer(SERVICE_NAME, topic_name):
            await self.process_kafka_message(msg)

    async def _listen_for_cache_invalidations(self) -> None:
        """Listens on the Redis Pub/Sub invalidation channel to evict local data."""
        pubsub = self.redis_client.pubsub()
        await pubsub.subscribe("channel:rule_invalidations")
        logger.info(
            "subscribed_to_cache_invalidation_bus", channel="channel:rule_invalidations"
        )

        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        invalidated_key = data.get("rule_key")

                        if invalidated_key in self._local_rule_cache:
                            del self._local_rule_cache[invalidated_key]
                            logger.info(
                                "pod_local_cache_evicted_via_pubsub",
                                rule_key=invalidated_key,
                                event_timestamp=data.get("timestamp"),
                            )
                    except Exception:
                        logger.exception(
                            "failed_to_process_pubsub_invalidation_message"
                        )
        except asyncio.CancelledError:
            logger.info("pubsub_listener_cancellation_requested")
        finally:
            await pubsub.unsubscribe("channel:rule_invalidations")
            await pubsub.close()

    async def process_kafka_message(self, msg: Any) -> None:
        """Processes an inbound transaction utilizing layered in-memory cache resolutions."""
        raw_event = _message_value(msg, "value")
        topic_name = _message_value(msg, "topic")

        try:
            parsed_event = json.loads(raw_event)
            correlation_id = (
                extract_correlation_id(parsed_event) or ensure_correlation_id()
            )
            attach_correlation_id(correlation_id)

            transaction = Transaction(
                amount=parsed_event.get("amount"),
                currency=parsed_event.get("currency"),
                merchant_category=parsed_event.get("merchant_category"),
                user_risk_score=parsed_event.get("user_risk_score"),
                is_international=parsed_event.get("is_international"),
            )

            rule_key = "fraud_default_ruleset"
            rule_config = self._local_rule_cache.get(rule_key)

            # LAYER 1: Pod-Local Variable Lookup (Sub-microsecond latency)
            if rule_config:
                metrics_cache_hit_type = "pod_local_memory"
            else:
                # LAYER 2: Shared Redis Cache Fallback Hit
                redis_key = f"rules:{rule_key}:latest"
                cached_payload = await self.redis_client.get(redis_key)

                if cached_payload:
                    rule_config = json.loads(cached_payload)
                    metrics_cache_hit_type = "shared_redis_cluster"
                else:
                    # LAYER 3: Relational Database Core Fallback Hit
                    rule_config = await get_active_rules_config(self.db_pool, rule_key)
                    metrics_cache_hit_type = "postgres_database"
                    if rule_config:
                        await self.redis_client.set(
                            redis_key, json.dumps(rule_config), ex=86400
                        )

                if rule_config:
                    # Hydrate local variable so future checks avoid network overhead entirely
                    self._local_rule_cache[rule_key] = rule_config
                    logger.debug("hydrated_pod_local_memory_cache", rule_key=rule_key)

            if not rule_config:
                raise RuntimeError(
                    f"Unable to resolve valid execution rule profiles for target: {rule_key}"
                )

            # Execute AST JsonLogic engine evaluation
            is_fraudulent = json_logic.jsonLogic(
                rule_config["root_condition"], transaction.__dict__
            )

            logger.info(
                "fraud_evaluation_completed",
                rule_version_used=rule_config["version"],
                decision=is_fraudulent,
                cache_resolution_track=metrics_cache_hit_type,
            )
            worker_fraud_decision(SERVICE_NAME, is_fraudulent)

            await save_transaction(
                self.db_pool,
                transaction,
                is_fraudulent,
                rule_version=rule_config["version"],
            )
            await commit_processed_message(self.consumer, msg)
            worker_message_outcome(SERVICE_NAME, topic_name, "success")

        except Exception as error:
            await route_message_to_dlq(
                consumer=self.consumer,
                producer=self.producer,
                msg=msg,
                raw_event=raw_event,
                error=error,
                topic_name=topic_name,
                dlq_topic_name=self.settings.kafka_dlq_topic,
            )

    async def shutdown(self) -> None:
        """Performs a graceful drain of active tasks and closes downstream pools."""
        logger.info("worker_drain_sequence_initiated", active_tasks_count=len(self._active_background_tasks))
        set_kafka_health(False)
        
        # 1. Invalidation loop cancellation
        if self.pubsub_task:
            self.pubsub_task.cancel()
            try:
                await self.pubsub_task
            except asyncio.CancelledError:
                pass

        # 2. Wait for background processing tasks to exit cleanly before stopping
        if self._active_background_tasks:
            logger.info("waiting_for_active_background_tasks_to_complete")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._active_background_tasks, return_exceptions=True),
                    timeout=30.0
                )
                logger.info("all_active_tasks_successfully_drained")
            except asyncio.TimeoutError:
                logger.warning(
                    "drain_timeout_exceeded", 
                    dropped_tasks_count=len(self._active_background_tasks)
                )

        # 3. Connection pools breakdown
        await _invoke_callable(self.consumer.close)
        await _invoke_callable(self.producer.flush, 5)
        logger.info("worker_shutdown_completed")


async def main():
    """Application entry point handling orchestration boots."""
    settings = get_settings()
    configure_logging(SERVICE_NAME, settings.observability_log_level)
    
    if settings.observability_enable_tracing:
        setup_tracing(SERVICE_NAME)

    # 1. Database and Cache Pooling Setup
    db_pool = await create_pool(settings.DATABASE_URL)
    await ping_database(db_pool)

    redis_pool = aioredis.ConnectionPool(
        host=settings.REDIS_HOST, port=settings.REDIS_PORT,
        db=settings.REDIS_DB, password=settings.REDIS_PASSWORD,
        decode_responses=True, max_connections=settings.redis_max_connections
    )
    redis_client = aioredis.Redis(connection_pool=redis_pool)

    # 2. Initialize Engine worker class
    worker = FraudWorker(settings, db_pool, redis_client)

    # 3. Attach signal listeners directly inside the active event loop
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: worker.handle_termination(s))

    # 4. Trigger worker processing loop
    try:
        await worker.start()
    finally:
        await redis_pool.disconnect()
        await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
