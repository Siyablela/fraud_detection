import asyncio
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import start_http_server
import redis.asyncio as aioredis
from app.database import create_pool, save_transaction
from app.dlq import build_dlq_payload, encode_dlq_payload
from app.kafka_security import kafka_client_security_kwargs
from app.observability import (
    configure_logging,
    get_logger,
    setup_tracing,
    worker_fraud_decision,
    worker_message_outcome,
    worker_message_timer,
)
from app.rule import Transaction, evaluate_transaction
from app.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP_ID,
    KAFKA_DLQ_TOPIC_NAME,
    KAFKA_PRODUCER_ACKS,
    KAFKA_PRODUCER_ENABLE_IDEMPOTENCE,
    KAFKA_PRODUCER_MAX_IN_FLIGHT,
    KAFKA_TOPIC_NAME,
    OBSERVABILITY_ENABLE_TRACING,
    OBSERVABILITY_LOG_LEVEL,
    REDIS_URL,
    VELOCITY_WINDOW_SECONDS,
    WORKER_METRICS_PORT,
)

TOPIC_NAME = KAFKA_TOPIC_NAME
DLQ_TOPIC_NAME = KAFKA_DLQ_TOPIC_NAME
CONSUMER_GROUP_ID = KAFKA_CONSUMER_GROUP_ID
SERVICE_NAME = "fraud-worker"
configure_logging(SERVICE_NAME, OBSERVABILITY_LOG_LEVEL)
if OBSERVABILITY_ENABLE_TRACING:
    setup_tracing(SERVICE_NAME)

logger = get_logger(__name__)

async def main():
    # Initialize infrastructure connections
    database_engine, database = create_pool()
    start_http_server(WORKER_METRICS_PORT)
    logger.info("worker_metrics_started", port=WORKER_METRICS_PORT)
    
    # Redis remains in place *only* as a high-speed state store for velocity checks
    redis_conn = aioredis.from_url(REDIS_URL, decode_responses=True)
    
    # Configure the asynchronous Kafka consumer
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",  # Read from start if no committed offsets exist
        enable_auto_commit=False,       # Manual commit for At-Least-Once delivery guarantees
        **kafka_client_security_kwargs(),
    )
    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks=KAFKA_PRODUCER_ACKS,
        enable_idempotence=KAFKA_PRODUCER_ENABLE_IDEMPOTENCE,
        max_in_flight_requests_per_connection=KAFKA_PRODUCER_MAX_IN_FLIGHT,
        **kafka_client_security_kwargs(),
    )
    
    await consumer.start()
    await dlq_producer.start()
    logger.info(
        "worker_started",
        topic=TOPIC_NAME,
        dlq_topic=DLQ_TOPIC_NAME,
        group_id=CONSUMER_GROUP_ID,
    )
    
    try:
        # Loop over the consumer stream. It automatically waits/polls internally.
        async for msg in consumer:
            raw_event = msg.value.decode("utf-8")
            
            try:
                with worker_message_timer(SERVICE_NAME, TOPIC_NAME):
                    event_data = json.loads(raw_event)

                    # Validate input structure using Pydantic
                    transaction = Transaction(**event_data)

                    # Velocity calculations stay in Redis for microsecond performance
                    velocity_key = f"velocity:{transaction.user_id}"
                    current_velocity = await redis_conn.incr(velocity_key)
                    if current_velocity == 1:
                        await redis_conn.expire(velocity_key, VELOCITY_WINDOW_SECONDS)

                    # Execute fraud logic
                    result = evaluate_transaction(transaction, current_velocity)

                    # Write to persistent database storage
                    await save_transaction(database, result)

                    logger.info(
                        "transaction_processed",
                        transaction_id=transaction.transaction_id,
                        user_id=transaction.user_id,
                        is_fraud=result["is_fraud"],
                        rules=result["triggered_rules"],
                    )

                    worker_fraud_decision(SERVICE_NAME, bool(result["is_fraud"]))
                    worker_message_outcome(SERVICE_NAME, TOPIC_NAME, "success")

                    # Commit offset only AFTER successful storage in PostgreSQL.
                    # This prevents message loss if the pod crashes mid-execution.
                    await consumer.commit()
                
            except Exception as exc:
                dlq_payload = build_dlq_payload(
                    source_topic=TOPIC_NAME,
                    source_partition=msg.partition,
                    source_offset=msg.offset,
                    source_timestamp=msg.timestamp,
                    raw_payload=raw_event,
                    error=exc,
                )

                try:
                    await dlq_producer.send_and_wait(
                        topic=DLQ_TOPIC_NAME,
                        key=msg.key,
                        value=encode_dlq_payload(dlq_payload),
                    )
                    worker_message_outcome(SERVICE_NAME, TOPIC_NAME, "dlq")
                    logger.exception(
                        "transaction_routed_to_dlq",
                        dlq_topic=DLQ_TOPIC_NAME,
                        source_offset=msg.offset,
                        source_partition=msg.partition,
                    )
                    # Commit after successful DLQ write to avoid poison message loops.
                    await consumer.commit()
                except Exception:
                    logger.exception(
                        "dlq_publish_failed",
                        dlq_topic=DLQ_TOPIC_NAME,
                        source_offset=msg.offset,
                        source_partition=msg.partition,
                    )
                    worker_message_outcome(SERVICE_NAME, TOPIC_NAME, "error")
                    await asyncio.sleep(1)
                
    finally:
        # Clean shutdown of engine dependencies
        logger.info("worker_shutdown")
        await consumer.stop()
        await dlq_producer.stop()
        await redis_conn.aclose()
        await database_engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
