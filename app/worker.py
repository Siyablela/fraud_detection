import asyncio
from datetime import datetime, timedelta, timezone
import inspect
import json
from typing import Any
import signal

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
    """Build an event-time velocity windo for a transaction

    Args:
        transaction_timestamp: The transactions's business timestamp as epoch seconds.
        window_seconds: Velocity loopback window size in seconds.

    Returns:
        tuple[float, float]: Inclusive window bounds as ``(start_epoch, end_epoch)``.
    """
    window_end_epoch = float(transaction_timestamp)
    window_start_epoch = window_end_epoch - float(window_seconds)
    return window_start_epoch, window_end_epoch


def _message_value(msg: Any, attribute: str) -> Any:
    """Safely resolve a Kafka message attribute, invoking callables when needed.

    Args:
        msg: Kafka message object to inspect.
        attribute: Attribute name to resolve from the message.

    Returns:
        Any: The message attribute value, or the result of a callable attribute.
    """
    value = getattr(msg, attribute, None)
    if callable(value):
        return value()
    return value


async def _invoke_callable(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """Invoke a sync or async callable in a way that preserves asyncio semantics.

    Args:
        fn: Callable to invoke.
        *args: Positional arguments to pass through to the callable.
        **kwargs: Keyword arguments to pass through to the callable.

    Returns:
        Any: The result returned by the callable.
    """
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    return await asyncio.to_thread(fn, *args, **kwargs)


async def commit_processed_message(consumer: Consumer, msg: Any) -> None:
    """Commit a Kafka message using the consumer's supported commit API.

    Args:
        consumer: Kafka consumer instance handling the message.
        msg: Kafka message to commit as processed.

    Returns:
        None: The message offset is committed in place when supported.
    """
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
    """Publish a failed message to the DLQ and commit the original Kafka offset.

    Args:
        consumer: Kafka consumer used to commit the original message.
        producer: Optional producer used to publish to the DLQ topic.
        msg: Original Kafka message raised by the worker.
        raw_event: Raw serialized message payload.
        error: Exception that caused the message to be routed to the DLQ.
        topic_name: Original source topic name.
        dlq_topic_name: Dead-letter topic name.
        dlq_producer: Optional explicit DLQ producer override.

    Returns:
        None: The message is written to the DLQ and the original offset is committed.
    """
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


async def process_kafka_message(
    consumer: Consumer,
    msg: Any,
    db_pool: Any,
    redis_client: aioredis.Redis,
    producer: Producer,
    dlq_topic: str,
) -> None:
    """Process an inbound Kafka transaction event using high-speed shared caching.

    Args:
        consumer: Active Kafka consumer instance handling the main queue loop.
        msg: The raw confluent_kafka message structure.
        db_pool: The shared database pool manager.
        redis_client: The async redis pool client injected at startup.
        producer: Dead-letter-queue fallback publisher client.
        dlq_topic: Destination topic identifier for failed validation schemas.
    """
    raw_event = _message_value(msg, "value")
    topic_name = _message_value(msg, "topic")
    
    try:
        # Extract metadata and setup context
        parsed_event = json.loads(raw_event)
        correlation_id = extract_correlation_id(parsed_event) or ensure_correlation_id()
        attach_correlation_id(correlation_id)
        
        # 1. Map properties onto your structural Transaction engine class object
        transaction = Transaction(
            amount=parsed_event.get("amount"),
            currency=parsed_event.get("currency"),
            merchant_category=parsed_event.get("merchant_category"),
            user_risk_score=parsed_event.get("user_risk_score"),
            is_international=parsed_event.get("is_international")
        )
        
        # 2. Fetch the target rule using the high-performance caching layer
        rule_key = "fraud_default_ruleset"
        redis_key = f"rules:{rule_key}:latest"
        
        rule_config = None
        try:
            cached_payload = await redis_client.get(redis_key)
            if cached_payload:
                rule_config = json.loads(cached_payload)
        except Exception:
            logger.exception("worker_redis_cache_read_failed", rule_key=rule_key)
        
        # 3. Cache Miss Fallback
        if not rule_config:
            logger.info("worker_cache_miss_falling_back_to_db", rule_key=rule_key)
            rule_config = await get_active_rules_config(db_pool, rule_key)
            if not rule_config:
                raise RuntimeError(f"Unable to resolve valid ruleset metadata configuration for: {rule_key}")
            
            # Rehydrate cache block so sibling pods/threads do not hit DB concurrently
            try:
                await redis_client.set(redis_key, json.dumps(rule_config), ex=86400)
            except Exception:
                logger.exception("worker_redis_cache_write_failed", rule_key=rule_key)
        
        # 4. Execute in-memory JSON-Logic via json-logic-py using object properties
        is_fraudulent = json_logic.jsonLogic(rule_config["root_condition"], transaction.__dict__)
        
        logger.info(
            "fraud_evaluation_completed",
            rule_version_used=rule_config["version"],
            decision=is_fraudulent
        )
        worker_fraud_decision(SERVICE_NAME, is_fraudulent)
        
        # 5. Persist to database tracking rule execution metadata context
        await save_transaction(
            db_pool,
            transaction,
            is_fraudulent,
            rule_version=rule_config["version"]
        )
        
        # 6. Commit offset cleanly after everything passes successfully
        await commit_processed_message(consumer, msg)
        worker_message_outcome(SERVICE_NAME, topic_name, "success")

    except Exception as error:
        # Gracefully handle corrupted entries by shipping out to the DLQ channel
        await route_message_to_dlq(
            consumer=consumer,
            producer=producer,
            msg=msg,
            raw_event=raw_event,
            error=error,
            topic_name=topic_name,
            dlq_topic_name=dlq_topic,
        )

# Global variable tracking flag for orchestration
_worker_should_run = True
_active_background_tasks: set[asyncio.Task] = set()

def _handle_termination_signal(signum, frame):
    """Intercepts OS signals to trigger a graceful shutdown sequence."""
    global _worker_should_run
    logger.info("received_os_termination_signal", signal=signal.Signals(signum).name)
    _worker_should_run = False


async def main_worker_loop():
    """Main worker initialization and orchestration engine event loop with graceful task drains."""
    global _worker_should_run
    
    settings = get_settings()
    configure_logging(SERVICE_NAME, settings.observability_log_level)
    
    if settings.observability_enable_tracing:
        setup_tracing(SERVICE_NAME)

    # 1. Register POSIX OS system event listeners inside the event loop
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: _handle_termination_signal(s, None))

    # 2. Bootstrapping connection pooling objects
    start_http_server(settings.metrics_port)
    db_pool = await create_pool(settings.DATABASE_URL)
    await ping_database(db_pool)

    redis_pool = aioredis.ConnectionPool(
        host=settings.REDIS_HOST, port=settings.REDIS_PORT,
        db=settings.REDIS_DB, password=settings.REDIS_PASSWORD,
        decode_responses=True, max_connections=settings.redis_max_connections
    )
    redis_client = aioredis.Redis(connection_pool=redis_pool)

    # 3. Setting up client handles
    kafka_config = build_kafka_client_config(settings)
    consumer = Consumer(kafka_config["consumer"])
    producer = Producer(kafka_config["producer"])
    consumer.subscribe([settings.kafka_source_topic])
    set_kafka_health(True)

    logger.info("worker_runtime_initialized", topic=settings.kafka_source_topic)

    # 4. Main Event Loop Ingestion Track
    while _worker_should_run:
        try:
            # Poll Kafka loop safely using a short timeout to allow loop checks to react to shutdown flags
            msg = await _invoke_callable(consumer.poll, timeout=0.5)
            
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
            
            # 5. Non-blocking Task Spawning with tracking attachment
            # Wrapping the execution timeline with the performance timer inside the task itself
            task = asyncio.create_task(
                _run_task_with_telemetry(consumer, msg, db_pool, redis_client, producer, settings)
            )
            
            # Store a strong reference to prevent garbage collection mid-flight
            _active_background_tasks.add(task)
            # Remove reference automatically upon final task completion
            task.add_done_callback(_active_background_tasks.discard)

        except Exception:
            logger.exception("worker_ingestion_loop_fault")
            await asyncio.sleep(1) # Prevent hot looping on broker connectivity drops

    # -------------------------------------------------------------------------
    # 6. GRACEFUL DRAIN SEQUENCE (Executes once _worker_should_run turns False)
    # -------------------------------------------------------------------------
    logger.info("worker_drain_sequence_initiated", active_tasks_count=len(_active_background_tasks))
    
    # Stop polling new events immediately
    set_kafka_health(False)
    
    if _active_background_tasks:
        logger.info("waiting_for_active_background_tasks_to_complete")
        # Give active concurrent processing tasks up to 30 seconds to safely exit
        try:
            await asyncio.wait_for(
                asyncio.gather(*_active_background_tasks, return_exceptions=True),
                timeout=30.0
            )
            logger.info("all_active_tasks_successfully_drained")
        except asyncio.TimeoutError:
            logger.warning(
                "drain_timeout_exceeded", 
                dropped_tasks_count=len(_active_background_tasks),
                detail="Forcing application exit before some active transactions could complete."
            )

    # 7. Final infrastructure teardown
    logger.info("closing_downstream_connection_pools")
    await _invoke_callable(consumer.close)
    await _invoke_callable(producer.flush, 5)
    await redis_pool.disconnect()
    await db_pool.close()
    logger.info("worker_graceful_shutdown_completed")


async def _run_task_with_telemetry(consumer, msg, db_pool, redis_client, producer, settings):
    """Wrapper task handling metric boundaries over the processing flow."""
    topic_name = _message_value(msg, "topic")
    with worker_message_timer(SERVICE_NAME, topic_name):
        await process_kafka_message(
            consumer=consumer,
            msg=msg,
            db_pool=db_pool,
            redis_client=redis_client,
            producer=producer,
            dlq_topic=settings.kafka_dlq_topic
        )