import json
import os
import asyncio
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.rule import Transaction

# Kafka connection details supplied by environment variables.
# For local Windows Kubernetes port-forwarding, defaults to localhost:9092.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "transactions_topic"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize the async Kafka producer.
    app.state.kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
    )
    # Start the producer (connects to the cluster).
    await app.state.kafka_producer.start()
    yield
    # Cleanly flush remaining messages and close the connection on shutdown.
    await app.state.kafka_producer.stop()


app = FastAPI(title="Fraud Detection Transaction Producer", lifespan=lifespan)


@app.post("/api/v1/transactions", status_code=status.HTTP_202_ACCEPTED)
async def emit_transaction(transaction: Transaction, request: Request):
    # Convert Pydantic model to JSON string, then encode to bytes for Kafka.
    payload = transaction.model_dump_json()
    payload_bytes = payload.encode("utf-8")

    # Use the transaction_id as the Kafka message key.
    # This guarantees that transactions for the same ID always go to the same 
    # Kafka partition, preserving exact chronological processing order.
    message_key = str(transaction.transaction_id).encode("utf-8")

    # Send the message asynchronously.
    # send_and_wait() ensures the message is acknowledged by the broker.
    producer = request.app.state.kafka_producer
    await producer.send_and_wait(
        topic=TOPIC_NAME, 
        value=payload_bytes, 
        key=message_key
    )

    return {
        "status": "queued",
        "transaction_id": transaction.transaction_id,
        "topic": TOPIC_NAME,
    }


@app.get("/health")
async def health(request: Request):
    # Verifies Kafka's health by making a lightweight request for cluster metadata.
    try:
        producer = request.app.state.kafka_producer
        # If the client can fetch metadata, the connection to the broker is healthy.
        await producer.client.fetch_all_metadata()
        return {"status": "ok"}
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
            content={"status": "unavailable"}
        )
