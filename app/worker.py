import asyncio
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import start_http_server
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

                    result = evaluate_transaction(transaction)

                    worker_fraud_decision(SERVICE_NAME, bool(result["is_fraud"]))
                    worker_message_outcome(SERVICE_NAME, TOPIC_NAME, "success")

                    # Commit only after persistence succeeds so the broker can redeliver on crash.
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
                    # Failures are preserved in the DLQ instead of being dropped or retried forever.
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
                    # Only commit once the DLQ write completes successfully.
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
        await database_engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
