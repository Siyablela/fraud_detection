from contextlib import asynccontextmanager
import logging
from math import ceil

from fastapi import Depends, FastAPI, HTTPException
from app.database import database_pool, get_transaction as find_transaction
from app.database import get_transactions_by_category as find_by_category
from app.database import ping_database
from app.observability import (
    apply_tracing,
    configure_logging,
    get_logger,
    install_fastapi_observability,
    setup_tracing,
)
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

logger = get_logger(__name__)

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
    # Health checks verify that the database pool is reachable before returning success.
    try:
        await ping_database(app.state.database)
        logger.info("health_check_passed")
        return {"status": "ok"}
    except Exception:
        logger.exception("health_check_failed", detail="database is unavailable")
        raise HTTPException(status_code=503, detail="Database is unavailable")

@app.get("/api/v1/transactions/{transaction_id}")
async def get_transaction(
    transaction_id: str,
    _principal=Depends(require_scopes(JWT_REQUIRED_SCOPE_FOR_TRANSACTION_READ)),
):
    # The scope dependency enforces read access before any lookup runs.
    logger.info("fetch_transaction", transaction_id=transaction_id)
    transaction = await find_transaction(app.state.database, transaction_id)
    if not transaction:
        logger.warning("transaction_not_found", transaction_id=transaction_id)
        raise HTTPException(status_code=404, detail="Transaction records not located.")
    return transaction

@app.get("/api/v1/categories/{category_name}")
async def get_transactions_by_category(
    category_name: str,
    page: int = 1,
    page_size: int = 100,
    _principal=Depends(require_scopes(JWT_REQUIRED_SCOPE_FOR_TRANSACTION_READ)),
):
    page = max(1, page)
    page_size = max(1, min(page_size, 1000))
    offset = (page - 1) * page_size
    logger.info(
        "fetch_category_transactions",
        category_name=category_name,
        page=page,
        page_size=page_size,
    )
    results, total_count = await find_by_category(
        app.state.database,
        category_name,
        offset,
        page_size,
    )
    total_pages = ceil(total_count / page_size) if total_count else 0
    has_more = offset + len(results) < total_count
    return {
        "category": category_name,
        "page": page,
        "page_size": page_size,
        "count": len(results),
        "total_count": total_count,
        "total_pages": total_pages,
        "has_previous": page > 1,
        "has_more": has_more,
        "previous_page": page - 1 if page > 1 else None,
        "next_page": page + 1 if has_more else None,
        "data": results,
    }
