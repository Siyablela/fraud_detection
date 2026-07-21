import json
import os
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.rule import Transaction

# Redis connection details are supplied by Compose or Kubernetes environment variables.
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create one async Redis client for the lifetime of the service.
    app.state.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    yield
    # Close the client cleanly when the application shuts down.
    await app.state.redis.aclose()


app = FastAPI(title="Fraud Detection Transaction Producer", lifespan=lifespan)


@app.post("/api/v1/transactions", status_code=status.HTTP_202_ACCEPTED)
async def emit_transaction(transaction: Transaction, request: Request):
    # FastAPI/Pydantic validates the incoming JSON against the Transaction model.
    payload = transaction.model_dump_json()

    # LPUSH adds the event to the queue consumed by the fraud-processing worker.
    await request.app.state.redis.lpush("transactions_queue", payload)

    return {
        "status": "queued",
        "transaction_id": transaction.transaction_id,
        "queue": "transactions_queue",
    }


@app.get("/health")
async def health(request: Request):
    # Check Redis as well as the HTTP process so orchestrators can detect dependency failures.
    try:
        await request.app.state.redis.ping()
        return {"status": "ok"}
    except Exception:
        return JSONResponse(status_code=503, content={"status": "unavailable"})
