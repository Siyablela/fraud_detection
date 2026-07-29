from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, HTTPException
from app.database import database_pool, get_transaction as find_transaction
from app.database import get_transactions_by_category as find_by_category
from app.database import get_transactions_by_user as find_by_user
from app.database import save_transaction
from app.observability import apply_tracing, configure_logging, install_fastapi_observability, setup_tracing
from app.rule import Transaction, evaluate_transaction
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

@app.get("/api/v1/transactions/{correlation_id}")
async def get_transaction(correlation_id: str):
    logger.info("Fetching transaction correlation_id=%s", correlation_id)
    transaction = await find_transaction(app.state.database, correlation_id)
    if not transaction:
        logger.warning("Transaction not found: correlation_id=%s", correlation_id)
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


@app.post("/api/v1/fraud/check")
async def fraud_check(transaction: Transaction, persist: bool = True):
    result = evaluate_transaction(transaction)

    if persist:
        await save_transaction(app.state.database, result)

    logger.info(
        "Realtime check correlation_id=%s user_id=%s fraud=%s persist=%s",
        transaction.correlation_id,
        transaction.user_id,
        result["is_fraud"],
        persist,
    )

    return {
        **result,
        "persisted": persist,
    }
