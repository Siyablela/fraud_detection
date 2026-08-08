import asyncio
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition
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
from app.settings import get_settings

SERVICE_NAME = "fraud-worker"

logger = get_logger(__name__)


async def commit_processed_message(consumer: AIOKafkaConsumer, msg) -> None:
    await consumer.commit({TopicPartition(msg.topic, msg.partition): msg.offset + 1})


async def route_message_to_dlq(
    consumer: AIOKafkaConsumer,
    dlq_producer: AIOKafkaProducer,
    msg,
    raw_event: str,
    error: Exception,
    topic_name: str,
    dlq_topic_name: str,
) -> None:
    dlq_payload = build_dlq_payload(
        source_topic=topic_name,
        source_partition=msg.partition,
        source_offset=msg.offset,
        source_timestamp=msg.timestamp,
        raw_payload=raw_event,
        error=error,
    )

    await dlq_producer.send_and_wait(
        topic=dlq_topic_name,
        key=msg.key,
        value=encode_dlq_payload(dlq_payload),
    )
    worker_message_outcome(SERVICE_NAME, topic_name, "dlq")
    logger.exception(
        "transaction_routed_to_dlq",
        dlq_topic=dlq_topic_name,
        source_offset=msg.offset,
        source_partition=msg.partition,
    )
    await commit_processed_message(consumer, msg)

async def main():
    settings = get_settings()
    topic_name = settings.kafka_topic_name
    dlq_topic_name = settings.kafka_dlq_topic_name
    consumer_group_id = settings.kafka_consumer_group_id
    configure_logging(SERVICE_NAME, settings.observability_log_level)
    if settings.observability_enable_tracing:
        setup_tracing(SERVICE_NAME)

    # Initialize infrastructure connections
    database_engine, database = create_pool()
    start_http_server(settings.worker_metrics_port)
    logger.info("worker_metrics_started", port=settings.worker_metrics_port)

    consumer = AIOKafkaConsumer(
        topic_name,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=consumer_group_id,
        auto_offset_reset="earliest",  # Read from start if no committed offsets exist
        enable_auto_commit=False,       # Manual commit for At-Least-Once delivery guarantees
        **kafka_client_security_kwargs(),
    )
    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks=settings.kafka_producer_acks,
        enable_idempotence=settings.kafka_producer_enable_idempotence,
        **kafka_client_security_kwargs(),
    )

    consumer_started = False
    dlq_producer_started = False

    try:
        await consumer.start()
        consumer_started = True
        await dlq_producer.start()
        dlq_producer_started = True
        logger.info(
            "worker_started",
            topic=topic_name,
            dlq_topic=dlq_topic_name,
            group_id=consumer_group_id,
        )

        # Loop over the consumer stream. It automatically waits/polls internally.
        async for msg in consumer:
            raw_event = msg.value.decode("utf-8")
            
            try:
                with worker_message_timer(SERVICE_NAME, topic_name):
                    event_data = json.loads(raw_event)

                    # Validate input structure using Pydantic
                    transaction = Transaction(**event_data)

                    result = evaluate_transaction(transaction)
                    await save_transaction(
                        database,
                        result,
                        source_metadata={
                            "source_topic": msg.topic,
                            "source_partition": msg.partition,
                            "source_offset": msg.offset,
                            "source_timestamp": msg.timestamp,
                        },
                    )

                    worker_fraud_decision(SERVICE_NAME, bool(result["is_fraud"]))
                    worker_message_outcome(SERVICE_NAME, topic_name, "success")

                    # Commit only after persistence succeeds so the broker can redeliver on crash.
                    await commit_processed_message(consumer, msg)
                
            except Exception as exc:
                try:
                    # Failures are preserved in the DLQ instead of being dropped or retried forever.
                    await route_message_to_dlq(
                        consumer=consumer,
                        dlq_producer=dlq_producer,
                        msg=msg,
                        raw_event=raw_event,
                        error=exc,
                        topic_name=topic_name,
                        dlq_topic_name=dlq_topic_name,
                    )
                except Exception:
                    logger.exception(
                        "dlq_publish_failed",
                        dlq_topic=dlq_topic_name,
                        source_offset=msg.offset,
                        source_partition=msg.partition,
                    )
                    worker_message_outcome(SERVICE_NAME, topic_name, "error")
                    await asyncio.sleep(1)
                
    finally:
        # Clean shutdown of engine dependencies
        logger.info("worker_shutdown")
        if consumer_started:
            await consumer.stop()
        if dlq_producer_started:
            await dlq_producer.stop()
        await database_engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
