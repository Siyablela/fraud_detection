import json
import logging
from contextlib import asynccontextmanager
from uuid import uuid4

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.database import create_audit_event, database_pool
from app.observability import apply_tracing, configure_logging, install_fastapi_observability, setup_tracing
from app.rule import Transaction, TransactionRequest
from app.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_NAME,
    OBSERVABILITY_ENABLE_TRACING,
    OBSERVABILITY_LOG_LEVEL,
)

TOPIC_NAME = KAFKA_TOPIC_NAME
SERVICE_NAME = "fraud-producer-api"
configure_logging(SERVICE_NAME, OBSERVABILITY_LOG_LEVEL)
if OBSERVABILITY_ENABLE_TRACING:
    setup_tracing(SERVICE_NAME)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with database_pool() as pool:
        app.state.database = pool
        app.state.kafka_producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
        )
        await app.state.kafka_producer.start()
        yield
        await app.state.kafka_producer.stop()


app = FastAPI(title="Fraud Detection Transaction Producer", lifespan=lifespan)
install_fastapi_observability(app, SERVICE_NAME)
apply_tracing(app)


@app.post("/api/v1/transactions", status_code=status.HTTP_202_ACCEPTED)
async def emit_transaction(transaction_request: TransactionRequest, request: Request):
    request_id = request.headers.get("x-request-id")
    actor_id = request.headers.get("x-actor-id")
    actor_type = request.headers.get("x-actor-type")
    source_ip = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")

    transaction = Transaction(
        audit_id=str(uuid4()),
        correlation_id=transaction_request.correlation_id,
        user_id=transaction_request.user_id,
        amount=transaction_request.amount,
        category=transaction_request.category,
        timestamp=transaction_request.timestamp,
        actor_id=actor_id,
        actor_type=actor_type,
        source_ip=source_ip,
        user_agent=user_agent,
        request_id=request_id,
        ingest_path="kafka_ingress",
    )

    # Convert Pydantic model to JSON string, then encode to bytes for Kafka.
    payload = transaction.model_dump_json()
    payload_bytes = payload.encode("utf-8")

    # Use the generated audit_id as the Kafka message key.
    # This guarantees that messages for the same event always go to the same
    # Kafka partition, preserving exact chronological processing order.
    message_key = transaction.audit_id.encode("utf-8")

    # Send the message asynchronously.
    # send_and_wait() ensures the message is acknowledged by the broker.
    producer = request.app.state.kafka_producer
    await producer.send_and_wait(
        topic=TOPIC_NAME, 
        value=payload_bytes, 
        key=message_key
    )

    await create_audit_event(
        request.app.state.database,
        audit_id=transaction.audit_id,
        event_type="INGRESS_ACCEPTED",
        service_name=SERVICE_NAME,
        payload=transaction.model_dump(),
        actor_id=actor_id,
        actor_type=actor_type,
        source_ip=source_ip,
        user_agent=user_agent,
        request_id=request_id,
    )

    logger.info(
        "Queued audit_id=%s correlation_id=%s user_id=%s amount=%s category=%s",
        transaction.audit_id,
        transaction.correlation_id,
        transaction.user_id,
        transaction.amount,
        transaction.category,
    )

    return {
        "status": "queued",
        "audit_id": transaction.audit_id,
        "correlation_id": transaction.correlation_id,
        "topic": TOPIC_NAME,
    }


@app.get("/health")
async def health(request: Request):
    # Verifies Kafka's health by making a lightweight request for cluster metadata.
    try:
        producer = request.app.state.kafka_producer
        # If the client can fetch metadata, the connection to the broker is healthy.
        await producer.client.fetch_all_metadata()
        logger.info("Health check passed")
        return {"status": "ok"}
    except Exception:
        logger.exception("Health check failed: kafka is unavailable")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
            content={"status": "unavailable"}
        )
