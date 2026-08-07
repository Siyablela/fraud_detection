from contextlib import asynccontextmanager
from math import ceil
from types import SimpleNamespace

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
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
from app.security import AuthenticatedPrincipal, get_current_principal
from app.settings import get_settings

SERVICE_NAME = "fraud-query-api"
router = APIRouter()

logger = get_logger(__name__)


async def require_transaction_read_scope(
    principal: AuthenticatedPrincipal = Depends(get_current_principal),
) -> AuthenticatedPrincipal:
    required_scope = get_settings().jwt_required_scope_for_transaction_read
    if required_scope not in principal.scopes:
        raise HTTPException(
            status_code=403,
            detail="Access token does not include the required scope.",
            headers={
                "WWW-Authenticate": f'Bearer error="insufficient_scope", scope="{required_scope}"'
            },
        )
    return principal

@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(SERVICE_NAME, settings.observability_log_level)
    if settings.observability_enable_tracing:
        setup_tracing(SERVICE_NAME)
        apply_tracing(app)
    async with database_pool() as pool:
        app.state.database = pool
        yield


def create_app() -> FastAPI:
    app = FastAPI(title="Fraud Detection Query Engine API", lifespan=lifespan)
    install_fastapi_observability(app, SERVICE_NAME)
    app.include_router(router)
    return app


def _database_from_request(request: Request | SimpleNamespace):
    return request.app.state.database


@router.get("/health")
async def health(request: Request):
    # Health checks verify that the database pool is reachable before returning success.
    try:
        await ping_database(_database_from_request(request))
        logger.info("health_check_passed")
        return {"status": "ok"}
    except Exception:
        logger.exception("health_check_failed", detail="database is unavailable")
        raise HTTPException(status_code=503, detail="Database is unavailable")

@router.get("/api/v1/transactions/{transaction_id}")
async def get_transaction(
    transaction_id: str,
    request: Request,
    _principal=Depends(require_transaction_read_scope),
):
    # The scope dependency enforces read access before any lookup runs.
    logger.info("fetch_transaction", transaction_id=transaction_id)
    transaction = await find_transaction(_database_from_request(request), transaction_id)
    if not transaction:
        logger.warning("transaction_not_found", transaction_id=transaction_id)
        raise HTTPException(status_code=404, detail="Transaction records not located.")
    return transaction

@router.get("/api/v1/categories/{category_name}")
async def get_transactions_by_category(
    category_name: str,
    request: Request,
    page: int = 1,
    page_size: int = 100,
    _principal=Depends(require_transaction_read_scope),
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
        _database_from_request(request),
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
