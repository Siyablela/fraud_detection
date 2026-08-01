from contextlib import asynccontextmanager
import logging

from fastapi import Depends, FastAPI, HTTPException
from app.database import database_pool, get_transaction as find_transaction
from app.database import get_transactions_by_category as find_by_category
from app.database import ping_database
from app.observability import apply_tracing, configure_logging, install_fastapi_observability, setup_tracing
from app.security import require_scopes
from app.settings import (
    JWT_REQUIRED_SCOPE_FOR_TRANSACTION_READ,
    OBSERVABILITY_ENABLE_TRACING,
    OBSERVABILITY_LOG_LEVEL,
)

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
        await ping_database(app.state.database)
        logger.info("Health check passed")
        return {"status": "ok"}
    except Exception:
        logger.exception("Health check failed: database is unavailable")
        raise HTTPException(status_code=503, detail="Database is unavailable")

@app.get("/api/v1/transactions/{transaction_id}")
async def get_transaction(
    transaction_id: str,
    _principal=Depends(require_scopes(JWT_REQUIRED_SCOPE_FOR_TRANSACTION_READ)),
):
    logger.info("Fetching transaction %s", transaction_id)
    transaction = await find_transaction(app.state.database, transaction_id)
    if not transaction:
        logger.warning("Transaction not found: %s", transaction_id)
        raise HTTPException(status_code=404, detail="Transaction records not located.")
    return transaction

@app.get("/api/v1/categories/{category_name}")
async def get_transactions_by_category(
    category_name: str,
    limit: int = 100,
    _principal=Depends(require_scopes(JWT_REQUIRED_SCOPE_FOR_TRANSACTION_READ)),
):
    limit = max(1, min(limit, 1000))
    logger.info("Fetching category=%s limit=%s", category_name, limit)
    results = await find_by_category(app.state.database, category_name, limit)
    return {"category": category_name, "count": len(results), "data": results}
