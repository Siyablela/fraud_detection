import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from prometheus_client import start_http_server
from app.database import create_audit_event, create_pool, save_transaction
from app.observability import (
    configure_logging,
    setup_tracing,
    worker_fraud_decision,
    worker_message_outcome,
    worker_message_timer,
)
from app.rule import Transaction, evaluate_transaction
from app.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP_ID,
    KAFKA_TOPIC_NAME,
    OBSERVABILITY_ENABLE_TRACING,
    OBSERVABILITY_LOG_LEVEL,
    WORKER_METRICS_PORT,
)

TOPIC_NAME = KAFKA_TOPIC_NAME
CONSUMER_GROUP_ID = KAFKA_CONSUMER_GROUP_ID
SERVICE_NAME = "fraud-worker"
configure_logging(SERVICE_NAME, OBSERVABILITY_LOG_LEVEL)
if OBSERVABILITY_ENABLE_TRACING:
    setup_tracing(SERVICE_NAME)

logger = logging.getLogger(__name__)

async def main():
    # Initialize infrastructure connections
    database = await create_pool()
    start_http_server(WORKER_METRICS_PORT)
    logger.info("Worker metrics endpoint listening on port %s", WORKER_METRICS_PORT)
    
    # Configure the asynchronous Kafka consumer
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",  # Read from start if no committed offsets exist
        enable_auto_commit=False        # Manual commit for At-Least-Once delivery guarantees
    )
    
    await consumer.start()
    logger.info("Fraud processing worker active topic=%s group_id=%s", TOPIC_NAME, CONSUMER_GROUP_ID)
    
    try:
        # Loop over the consumer stream. It automatically waits/polls internally.
        async for msg in consumer:
            raw_event = msg.value.decode("utf-8")
            
            try:
                with worker_message_timer(SERVICE_NAME, TOPIC_NAME):
                    event_data = json.loads(raw_event)

                    # Validate input structure using Pydantic
                    transaction = Transaction(**event_data)

                    # Execute fraud logic
                    result = evaluate_transaction(transaction)

                    # Write to persistent database storage
                    await save_transaction(database, result)
                    await create_audit_event(
                        database,
                        audit_id=transaction.audit_id,
                        event_type="ASYNC_DECISION_PERSISTED",
                        service_name=SERVICE_NAME,
                        payload=result,
                        actor_id=transaction.actor_id,
                        actor_type=transaction.actor_type,
                        source_ip=transaction.source_ip,
                        user_agent=transaction.user_agent,
                        request_id=transaction.request_id,
                    )

                    logger.info(
                        "Processed audit_id=%s correlation_id=%s user_id=%s fraud=%s rules=%s",
                        transaction.audit_id,
                        transaction.correlation_id,
                        transaction.user_id,
                        result["is_fraud"],
                        ",".join(result["triggered_rules"]),
                    )

                    worker_fraud_decision(SERVICE_NAME, bool(result["is_fraud"]))
                    worker_message_outcome(SERVICE_NAME, TOPIC_NAME, "success")

                    # Commit offset only AFTER successful storage in PostgreSQL.
                    # This prevents message loss if the pod crashes mid-execution.
                    await consumer.commit()
                
            except Exception as e:
                logger.exception("Error processing transaction element: %s", e)
                worker_message_outcome(SERVICE_NAME, TOPIC_NAME, "error")
                # In production, route unparseable messages to a Kafka Dead-Letter Topic here.
                # We skip manual offset commit here to allow investigation or processing retries.
                await asyncio.sleep(1)
                
    finally:
        # Clean shutdown of engine dependencies
        logger.info("Shutting down worker clients")
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
