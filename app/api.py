from contextlib import asynccontextmanager
import logging
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from app.database import create_audit_event, database_pool, get_audit_events
from app.database import get_transaction as find_transaction
from app.database import get_transactions_by_category as find_by_category
from app.database import get_transactions_by_user as find_by_user
from app.database import save_transaction
from app.observability import apply_tracing, configure_logging, install_fastapi_observability, setup_tracing
from app.rule import Transaction, TransactionRequest, evaluate_transaction
from app.settings import OBSERVABILITY_ENABLE_TRACING, OBSERVABILITY_LOG_LEVEL

SERVICE_NAME = "fraud-query-api"
configure_logging(SERVICE_NAME, OBSERVABILITY_LOG_LEVEL)
if OBSERVABILITY_ENABLE_TRACING:
    setup_tracing(SERVICE_NAME)

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with database_pool() as pool:
        app.state.database = pool
        yield


app = FastAPI(title="Fraud Detection Query Engine API", lifespan=lifespan)
install_fastapi_observability(app, SERVICE_NAME)
apply_tracing(app)


@app.get("/health")
async def health():
    try:
        await app.state.database.fetchval("SELECT 1")
        logger.info("Health check passed")
        return {"status": "ok"}
    except Exception:
        logger.exception("Health check failed: database is unavailable")
        raise HTTPException(status_code=503, detail="Database is unavailable")

@app.get("/api/v1/transactions/{audit_id}")
async def get_transaction(audit_id: str):
    logger.info("Fetching transaction audit_id=%s", audit_id)
    transaction = await find_transaction(app.state.database, audit_id)
    if not transaction:
        logger.warning("Transaction not found: audit_id=%s", audit_id)
        raise HTTPException(status_code=404, detail="Transaction records not located.")
    return transaction

@app.get("/api/v1/categories/{category_name}")
async def get_transactions_by_category(category_name: str, limit: int = 100):
    limit = max(1, min(limit, 1000))
    logger.info("Fetching category=%s limit=%s", category_name, limit)
    results = await find_by_category(app.state.database, category_name, limit)
    return {"category": category_name, "count": len(results), "data": results}


@app.get("/api/v1/users/{user_id}")
async def get_transactions_by_user(user_id: str, limit: int = 100):
    limit = max(1, min(limit, 1000))
    logger.info("Fetching user_id=%s limit=%s", user_id, limit)
    results = await find_by_user(app.state.database, user_id, limit)
    return {"user_id": user_id, "count": len(results), "data": results}


@app.get("/api/v1/transactions/{audit_id}/audit")
async def get_transaction_audit_events(audit_id: str, limit: int = 100):
    limit = max(1, min(limit, 1000))
    events = await get_audit_events(app.state.database, audit_id, limit)
    return {"audit_id": audit_id, "count": len(events), "events": events}


@app.post("/api/v1/fraud/check")
async def fraud_check(transaction_request: TransactionRequest, request: Request, persist: bool = True):
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
        ingest_path="realtime_api",
    )

    result = evaluate_transaction(transaction)

    await create_audit_event(
        app.state.database,
        audit_id=transaction.audit_id,
        event_type="REALTIME_DECISION_COMPUTED",
        service_name=SERVICE_NAME,
        payload=result,
        actor_id=actor_id,
        actor_type=actor_type,
        source_ip=source_ip,
        user_agent=user_agent,
        request_id=request_id,
    )

    if persist:
        await save_transaction(app.state.database, result)
        await create_audit_event(
            app.state.database,
            audit_id=transaction.audit_id,
            event_type="REALTIME_DECISION_PERSISTED",
            service_name=SERVICE_NAME,
            payload={"audit_id": transaction.audit_id, "persisted": True},
            actor_id=actor_id,
            actor_type=actor_type,
            source_ip=source_ip,
            user_agent=user_agent,
            request_id=request_id,
        )

    logger.info(
        "Realtime check audit_id=%s correlation_id=%s user_id=%s fraud=%s persist=%s",
        transaction.audit_id,
        transaction.correlation_id,
        transaction.user_id,
        result["is_fraud"],
        persist,
    )

    return {
        **result,
        "persisted": persist,
    }
