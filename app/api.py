from contextlib import asynccontextmanager
from math import ceil
from types import SimpleNamespace
from datetime import datetime
from decimal import Decimal
from uuid import uuid7 as uuid

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.rate_limit import RateLimitMiddleware
from app.database import database_pool, get_transaction as find_transaction
from app.database import get_transactions_by_category as find_by_category, find_latest_rule_in_db
from app.database import ping_database
from app.observability import (
    apply_tracing,
    configure_logging,
    get_correlation_id,
    get_logger,
    install_fastapi_observability,
    setup_tracing,
)
from app.security import AuthenticatedPrincipal, get_current_principal
from app.settings import get_settings



import json
from contextlib import asynccontextmanager
from typing import Any, Dict
from fastapi import FastAPI, HTTPException, Depends, status
import redis.asyncio as aioredis
import json_logic
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database import engine, Base, get_db, RuleModel, RuleValidationSchema, TransactionIngestSchema

redis_client: aioredis.Redis | None = None





SERVICE_NAME = "fraud-query-api"
router = APIRouter()

logger = get_logger(__name__)


class TransactionResponse(BaseModel):
    transaction_id: uuid
    user_id: uuid
    amount: Decimal
    category: str
    timestamp: float
    is_fraud: bool
    triggered_rules: list[str]
    correlation_id: uuid | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class CategoryPageResponse(BaseModel):
    category: str
    page: int
    page_size: int
    count: int
    total_count: int
    total_pages: int
    has_previous: bool
    has_more: bool
    previous_page: int | None = None
    next_page: int | None = None
    data: list[TransactionResponse]
    correlation_id: uuid | None = None

async def require_transaction_scope(
    principal: AuthenticatedPrincipal, required_scope
) -> AuthenticatedPrincipal:
    """Ensure the caller has the transaction read scope required by the API.

    Args:
        principal: The authenticated caller principal extracted from the bearer token.

    Returns:
        AuthenticatedPrincipal: The validated principal after confirming the required
            scope is present.

    Raises:
        HTTPException: If the token is missing the required read scope.
    """
    if required_scope not in principal.scopes:
        raise HTTPException(
            status_code=403,
            detail="Access token does not include the required scope.",
            headers={
                "WWW-Authenticate": f'Bearer error="insufficient_scope", scope="{required_scope}"'
            },
        )
    return principal

async def require_transaction_read_scope(
    principal: AuthenticatedPrincipal = Depends(get_current_principal),
) -> AuthenticatedPrincipal:
    """Ensure the caller has the transaction read scope required by the API.

    Args:
        principal: The authenticated caller principal extracted from the bearer token.

    Returns:
        AuthenticatedPrincipal: The validated principal after confirming the required
            scope is present.

    Raises:
        HTTPException: If the token is missing the required read scope.
    """
    required_scope = get_settings().jwt_required_scope_for_transaction_read
    return require_transaction_scope(principal, required_scope)

async def require_transaction_write_scope(
    principal: AuthenticatedPrincipal = Depends(get_current_principal),
) -> AuthenticatedPrincipal:
    """Ensure the caller has the transaction read scope required by the API.

    Args:
        principal: The authenticated caller principal extracted from the bearer token.

    Returns:
        AuthenticatedPrincipal: The validated principal after confirming the required
            scope is present.

    Raises:
        HTTPException: If the token is missing the required read scope.
    """
    required_scope = get_settings().jwt_required_scope_for_transaction_write
    return require_transaction_scope(principal, required_scope)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Unified application lifespan for high-concurrency environments.
    Manages logging, tracing, database connectivity pools, and Redis caching blocks.
    """
    # 1. Bootstrapping Observability & Configuration Core
    settings = get_settings()
    configure_logging(
        SERVICE_NAME,
        settings.observability_log_level,
    )
    if settings.observability_enable_tracing:
        setup_tracing(SERVICE_NAME)
        apply_tracing(app)

    # 2. Build Independent High-Concurrency Connection Pools
    # Initialize SQLAlchemy connection pool
    db_pool = database_pool()
    app.state.database = db_pool

    # Initialize Async Redis Connection Pool
    redis_pool = aioredis.ConnectionPool(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        password=settings.REDIS_PASSWORD,
        decode_responses=True,
        max_connections=settings.db_pool_max_size  # Prevent a single pod from starving the Redis engine clusters
    )
    redis_client = aioredis.Redis(connection_pool=redis_pool)
    app.state.redis = redis_client

    # 3. Execute Readiness Probe Checks (Startup Gating)
    try:
        # Validate Database
        await _validate_database_ready(db_pool)
        
        # Validate Redis Cluster
        await redis_client.ping()
        
        logger.info("startup_dependency_check_passed", detail="Database and Redis structures online.")
    except Exception as e:
        logger.exception(
            "startup_dependency_check_failed",
            detail=f"Infrastructure downstream mismatch: {str(e)}",
        )
        # Ensure partial initialization states clean up if structural boots fail
        await redis_pool.disconnect()
        await db_pool.dispose()
        raise

    # ----------------------------------------------------
    # Application runs while sitting on this yield handle
    # ----------------------------------------------------
    yield

    # 4. Graceful Shutdown Track (Executes when pods spin down)
    logger.info("shutdown_sequence_initiated", detail="Safely disconnecting downstream pool targets.")
    
    # Clean up Redis Client connections
    await redis_pool.disconnect()
    
    # Clean up SQLAlchemy database resources
    await db_pool.dispose()
    
    logger.info("shutdown_sequence_completed")


def create_app() -> FastAPI:
    """Build and configure the FastAPI application instance.

    Returns:
        FastAPI: A configured application with middleware, tracing, and route registration.
    """
    settings = get_settings()
    app = FastAPI(title="Fraud Detection Query Engine API", lifespan=lifespan)
    install_fastapi_observability(app, SERVICE_NAME)
    # Rate limiting middleware (inner). Added before CORS to ensure that rate limits are applied before any CORS headers are added to the response.
    if settings.rate_limit_enabled:
        app.add_middleware(
            RateLimitMiddleware,
            max_requests=settings.rate_limit_requests,
            window_seconds=settings.rate_limit_window_seconds,
            burst=settings.rate_limit_burst,
        )
    # CORS middleware (outer). Added after rate limiting to ensure that CORS headers are applied to all responses, including those that may be rate-limited.
    # Credentials (cookies/basic-auth/client-certs) are intentionally not enabled.
    # This is a stateless Bearer-token API, amd a Bearer header set by js is a
    # normal header (allowed via allow_headers), not a CORS "credential".
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allowed_origins,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type"],
    )

    app.include_router(router)
    return app


def _database_from_request(request: Request | SimpleNamespace):
    """Return the shared database pool from a FastAPI request or test double.

    Args:
        request: A FastAPI request object or a lightweight test double exposing an app
            state.

    Returns:
        Any: The application's database session factory attached to request state.
    """
    return request.app.state.database

def _redis_from_request(request: Request | SimpleNamespace):
    """Return the shared Redis pool client from a FastAPI request or test double.

    Args:
        request: A FastAPI request object or a lightweight test double exposing an app state.

    Returns:
        redis.asyncio.Redis: The microservice's async Redis client instance.
    """
    return request.app.state.redis


async def _validate_database_ready(database) -> None:
    """Check that the database is reachable before the app is considered ready for traffic."""
    await ping_database(database)


@router.get("/live")
async def live() -> dict[str, str]:
    """Return a lightweight liveness signal indicating the process is running."""
    logger.info("liveness_check_passed")
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request):
    """Return the service readiness state only when the database is reachable."""
    try:
        await _validate_database_ready(_database_from_request(request))
        logger.info("readiness_check_passed")
        return {"status": "ready"}
    except Exception:
        logger.exception("readiness_check_failed", detail="database is unavailable")
        raise HTTPException(status_code=503, detail="Database is unavailable")


@router.get("/health")
async def health(request: Request):
    """Return the application health status after confirming the database is reachable.

    Args:
        request: The incoming HTTP request.

    Returns:
        dict[str, str]: A health payload with the service status.

    Raises:
        HTTPException: If the database cannot be reached and the service is unhealthy.
    """
    try:
        await _validate_database_ready(_database_from_request(request))
        logger.info("health_check_passed")
        return {"status": "ok"}
    except Exception:
        logger.exception("health_check_failed", detail="database is unavailable")
        raise HTTPException(status_code=503, detail="Database is unavailable")


@router.get("/api/v1/transactions/{transaction_id}", response_model=TransactionResponse)
async def get_transaction(
    transaction_id: uuid,
    request: Request,
    _principal=Depends(require_transaction_read_scope),
):
    """Return a single transaction by identifier for authorized callers.

    Args:
        transaction_id: The unique ID of the transaction to fetch.
        request: The incoming HTTP request.
        _principal: The caller principal validated by the read-scope dependency.

    Returns:
        TransactionResponse: The transaction payload with the active correlation ID attached.

    Raises:
        HTTPException: If no matching transaction is found.
    """
    # The scope dependency enforces read access before any lookup runs.
    logger.info("fetch_transaction", transaction_id=transaction_id)
    transaction = await find_transaction(
        _database_from_request(request), transaction_id
    )
    if not transaction:
        logger.warning("transaction_not_found", transaction_id=transaction_id)
        raise HTTPException(status_code=404, detail="Transaction records not located.")
    response_payload = dict(transaction)
    response_payload["correlation_id"] = get_correlation_id()
    return response_payload


@router.get("/api/v1/categories/{category_name}", response_model=CategoryPageResponse)
async def get_transactions_by_category(
    category_name: str,
    request: Request,
    page: int = Query(1, ge=1, description="1-based page number to fetch."),
    page_size: int = Query(
        100, ge=1, le=1000, description="Number of records to fetch per page."
    ),
    _principal=Depends(require_transaction_read_scope),
):
    """Return a paginated category view with metadata and correlation details.

    Args:
        category_name: The category to filter transactions by.
        request: The incoming HTTP request.
        page: The 1-based page number to fetch.
        page_size: Max number of records requested per page.
        _principal: The caller principal validated by the read-scope dependency.

    Returns:
        CategoryPageResponse: Paginated category result metadata and the matching transaction rows.
    """
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
        "correlation_id": get_correlation_id(),
    }

@router.post("/api/v1/rules/{rule_key}/evaluate", status_code=status.HTTP_200_OK)
async def evaluate_transaction_rules(
    rule_key: str,
    transaction_data: TransactionIngestSchema,
    request: Request,
    _principal=Depends(require_transaction_write_scope),
):
    """Evaluate a transaction against cached versioned logic rules."""
    logger.info("evaluate_transaction_rules", rule_key=rule_key)
    
    # 1. Fetch cache and DB handles from the active request context state
    redis_client = _redis_from_request(request)
    db_pool = _database_from_request(request)
    
    # 2. Fast Path Cache Read Namespace
    redis_key = f"rules:{rule_key}:latest"
    rule_config = None
    
    try:
        cached_payload = await redis_client.get(redis_key)
        if cached_payload:
            rule_config = json.loads(cached_payload)
    except Exception:
        logger.exception("redis_cache_read_failed", rule_key=rule_key)

    # 3. Cache Miss Fallback Track
    if not rule_config:
        logger.info("cache_miss_falling_back_to_db", rule_key=rule_key)
        rule_config = await find_latest_rule_in_db(db_pool, rule_key)
        if not rule_config:
            raise HTTPException(status_code=404, detail="Requested engine ruleset not found.")
            
        # Rehydrate cache block to shield neighboring pods from hitting the database
        try:
            await redis_client.set(redis_key, json.dumps(rule_config), ex=86400)
        except Exception:
            logger.exception("redis_cache_write_failed", rule_key=rule_key)

    # 4. In-Memory AST Engine Execution
    try:
        runtime_context = transaction_data.model_dump()
        is_triggered = json_logic.jsonLogic(rule_config["root_condition"], runtime_context)
    except Exception as e:
        logger.exception("engine_runtime_evaluation_error", rule_key=rule_key)
        raise HTTPException(status_code=500, detail="Internal rule evaluation engine failure.")

    return {
        "rule_key": rule_key,
        "version_used": rule_config["version"],
        "triggered": bool(is_triggered),
        "executed_actions": rule_config["actions"] if is_triggered else [],
        "correlation_id": get_correlation_id()
    }